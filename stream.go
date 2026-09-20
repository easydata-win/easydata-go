package easydata

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// StreamOptions tunes a held-connection read.
type StreamOptions struct {
	// Cursor resumes from a position a previous read returned. Empty starts at
	// the beginning of the batch.
	Cursor string
	// MaxReconnects bounds how many times a dropped connection is re-opened
	// before giving up. Zero means 10.
	MaxReconnects int
	// PageSize is entries per underlying read. Zero means 100.
	PageSize int
}

// Stream reads a batch's entries over a held connection instead of polling.
//
// Same cursor, same entries, same order as Results - the difference is that the
// server pushes rather than the client asking, so a long batch costs one
// request instead of hundreds against the rate limit.
//
// Use webhooks instead if you run a server with a public URL: they survive a
// restart and hold nothing open. This is for a client with nowhere to deliver
// to - an agent on a laptop, a CLI, an edge function.
//
// Reconnects are handled here and are exact: the event id IS the cursor, so a
// dropped connection resumes where it stopped with no duplicates and no gaps.
// The iterator ends when the batch reaches a terminal state; bound the wait
// with ctx.
//
// The error is yielded once and the walk then stops, like Results.
//
//	for entry, err := range ed.Stream(ctx, batchID, nil) {
//		if err != nil {
//			return err
//		}
//		save(entry)
//	}
func (c *Client) Stream(ctx context.Context, batchID string, opts *StreamOptions) iter.Seq2[ResultEntry, error] {
	if opts == nil {
		opts = &StreamOptions{}
	}
	maxReconnects := opts.MaxReconnects
	if maxReconnects <= 0 {
		maxReconnects = 10
	}
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}

	return func(yield func(ResultEntry, error) bool) {
		cursor := opts.Cursor
		reconnects := 0

		for {
			complete, stopped, err := c.streamOnce(ctx, batchID, &cursor, pageSize, yield)
			if stopped {
				return
			}
			if complete {
				return
			}
			if err != nil {
				// A response that was never produced is the case reconnecting
				// exists for. Anything the API actually said is final.
				var apiErr *Error
				if !AsErrorIs(err, &apiErr) || !apiErr.Transport() {
					yield(ResultEntry{}, err)
					return
				}
			}
			if ctx.Err() != nil {
				yield(ResultEntry{}, ctx.Err())
				return
			}

			reconnects++
			if reconnects > maxReconnects {
				yield(ResultEntry{}, &Error{Message: fmt.Sprintf(
					"stream dropped %d times without completing; last cursor %s",
					reconnects, cursor)})
				return
			}
			if !sleepCtx(ctx, backoff(reconnects, 0)) {
				return
			}
		}
	}
}

// streamOnce holds one connection. It reports whether the batch completed and
// whether the consumer stopped iterating.
func (c *Client) streamOnce(ctx context.Context, batchID string, cursor *string, pageSize int,
	yield func(ResultEntry, error) bool) (complete, stopped bool, err error) {

	q := url.Values{}
	q.Set("limit", strconv.Itoa(pageSize))
	if *cursor != "" {
		q.Set("cursor", *cursor)
	}
	endpoint := c.baseURL + "/batches/" + batchID + "/results?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return false, false, fmt.Errorf("easydata: building stream request: %w", err)
	}
	req.Header.Set("X-API-Key", c.apiKey)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("User-Agent", c.userAgent)
	if *cursor != "" {
		// Where to resume. The server prefers this over ?cursor= for the same
		// reason a browser sends it: it is the more recent of the two.
		req.Header.Set("Last-Event-ID", *cursor)
	}

	// A client whose Timeout would cut a held connection is the one thing that
	// makes streaming useless, so this request gets a transport with none. The
	// caller's ctx is the way out.
	streaming := &http.Client{Transport: c.http.Transport, CheckRedirect: c.http.CheckRedirect}

	resp, err := streaming.Do(req)
	if err != nil {
		return false, false, &Error{Message: fmt.Sprintf("could not open stream: %v", err), wrapped: err}
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return false, false, errorFrom(resp.StatusCode, raw, readRetryAfter(resp.Header))
	}

	for e := range parseSSE(resp.Body) {
		if e.ID != "" {
			*cursor = e.ID
		}

		switch e.Event {
		case "result":
			var entry ResultEntry
			if uerr := json.Unmarshal([]byte(e.Data), &entry); uerr != nil {
				yield(ResultEntry{}, fmt.Errorf("easydata: decoding stream entry: %w", uerr))
				return false, true, nil
			}
			if !yield(entry, nil) {
				return false, true, nil
			}

		case "complete":
			return true, false, nil

		case "error":
			yield(ResultEntry{}, &Error{
				Type:    ErrTypeInternal,
				Message: fmt.Sprintf("stream failed: %s. Resume from cursor %s.", e.Data, *cursor),
			})
			return false, true, nil

			// `timeout` is the server ending a long stream on purpose. Falling
			// out of the loop re-opens from the cursor, which is what it asked
			// for - a caller never sees it.
		}
	}

	// The body ended without a `complete`.
	return false, false, nil
}

// parseSSE is the wire format of Server-Sent Events, as (event, data, id).
//
// Hand-rolled rather than a dependency, because this client has none and the
// format is three rules: events are separated by a blank line, fields are
// `name: value`, and a line starting with `:` is a comment.
//
// Fields accumulate until a blank line, so a `data:` split across two reads is
// reassembled rather than parsed as half an entry - the bug every naive
// implementation has and which only shows up under load.
func parseSSE(r io.Reader) iter.Seq[struct{ Event, Data, ID string }] {
	type ev = struct{ Event, Data, ID string }

	return func(yield func(ev) bool) {
		sc := bufio.NewScanner(r)
		// An entry carrying a full profile is comfortably past the 64KB
		// default, and a scanner that hits its limit stops silently.
		sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

		cur := ev{Event: "message"}
		var data []string

		flush := func() bool {
			if len(data) == 0 && cur.Event == "message" {
				cur, data = ev{Event: "message"}, nil
				return true
			}
			cur.Data = strings.Join(data, "\n")
			out := cur
			cur, data = ev{Event: "message"}, nil
			return yield(out)
		}

		for sc.Scan() {
			line := strings.TrimRight(sc.Text(), "\r")

			if line == "" {
				if !flush() {
					return
				}
				continue
			}
			if strings.HasPrefix(line, ":") {
				continue // keepalive comment
			}

			field, value, _ := strings.Cut(line, ":")
			// One optional space after the colon is part of the framing.
			value = strings.TrimPrefix(value, " ")

			switch field {
			case "event":
				cur.Event = value
			case "data":
				data = append(data, value)
			case "id":
				cur.ID = value
			}
		}

		flush()
	}
}
