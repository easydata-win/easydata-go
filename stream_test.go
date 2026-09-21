package easydata

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// An SSE server that writes its body in tiny chunks, because a `data:` line
// split across two reads is the bug every naive parser has and it only shows
// up under load.
func sseServe(t *testing.T, bodies ...string) (*Client, *[]http.Header) {
	t.Helper()
	var seen []http.Header
	i := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Clone())

		body := "event: complete\ndata: {}\n\n"
		if i < len(bodies) {
			body = bodies[i]
		}
		i++

		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for c := 0; c < len(body); c += 5 {
			end := min(c+5, len(body))
			_, _ = w.Write([]byte(body[c:end]))
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	t.Cleanup(srv.Close)

	c, err := New("pk_x", WithBaseURL(srv.URL))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c, &seen
}

func TestStreamYieldsEntriesAndStopsOnComplete(t *testing.T) {
	c, seen := sseServe(t,
		`id: c1`+"\n"+`event: result`+"\n"+`data: {"item_index":0,"status":"succeeded","credits_used":1}`+"\n\n"+
			`: keepalive`+"\n\n"+
			`id: c2`+"\n"+`event: result`+"\n"+`data: {"item_index":1,"status":"succeeded","credits_used":1}`+"\n\n"+
			`id: c3`+"\n"+`event: complete`+"\n"+`data: {"status":"completed"}`+"\n\n",
	)

	var got []int
	for e, err := range c.Stream(context.Background(), "b-1", nil) {
		if err != nil {
			t.Fatalf("Stream: %v", err)
		}
		got = append(got, e.ItemIndex)
	}

	if len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Errorf("got %v, want [0 1] - a keepalive comment may have been parsed as an entry", got)
	}
	if (*seen)[0].Get("Accept") != "text/event-stream" {
		t.Errorf("Accept %q", (*seen)[0].Get("Accept"))
	}
}

// The whole reason the event id is the cursor: resuming has to be exact.
func TestStreamResumesFromTheLastEventID(t *testing.T) {
	c, seen := sseServe(t,
		`id: c1`+"\n"+`event: result`+"\n"+`data: {"item_index":0,"status":"succeeded","credits_used":1}`+"\n\n",
		`id: c2`+"\n"+`event: result`+"\n"+`data: {"item_index":1,"status":"succeeded","credits_used":1}`+"\n\n"+
			`event: complete`+"\n"+`data: {}`+"\n\n",
	)

	var got []int
	for e, err := range c.Stream(context.Background(), "b-1", nil) {
		if err != nil {
			t.Fatalf("Stream: %v", err)
		}
		got = append(got, e.ItemIndex)
	}

	if len(got) != 2 {
		t.Fatalf("got %v, want both entries across the reconnect", got)
	}
	if len(*seen) != 2 {
		t.Fatalf("the dropped connection was not re-opened (%d requests)", len(*seen))
	}
	if h := (*seen)[1].Get("Last-Event-ID"); h != "c1" {
		t.Errorf("resumed from %q, want c1", h)
	}
}

// The server ending a long stream on purpose. A caller should never see it.
func TestStreamReconnectsTransparentlyOnTimeout(t *testing.T) {
	c, seen := sseServe(t,
		`id: c1`+"\n"+`event: result`+"\n"+`data: {"item_index":0,"status":"succeeded","credits_used":1}`+"\n\n"+
			`id: c1`+"\n"+`event: timeout`+"\n"+`data: {"cursor":"c1"}`+"\n\n",
		`event: complete`+"\n"+`data: {}`+"\n\n",
	)

	n := 0
	for _, err := range c.Stream(context.Background(), "b-1", nil) {
		if err != nil {
			t.Fatalf("a timeout surfaced to the caller: %v", err)
		}
		n++
	}
	if n != 1 || len(*seen) != 2 {
		t.Errorf("%d entries over %d requests, want 1 over 2", n, len(*seen))
	}
}

func TestStreamSurfacesAnErrorEventWithItsCursor(t *testing.T) {
	c, _ := sseServe(t,
		`id: c9`+"\n"+`event: error`+"\n"+`data: {"type":"internal_error","cursor":"c9"}`+"\n\n",
	)

	var gotErr error
	for _, err := range c.Stream(context.Background(), "b-1", nil) {
		if err != nil {
			gotErr = err
		}
	}
	if gotErr == nil {
		t.Fatal("an error event did not surface")
	}
	if !strings.Contains(gotErr.Error(), "c9") {
		t.Errorf("the error does not name the cursor to resume from: %v", gotErr)
	}
}

// A non-200 is the normal typed error, not a stream that opens and dies.
func TestStreamOnAMissingBatchIsATypedError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":{"type":"not_found","message":"no such batch"}}`))
	}))
	t.Cleanup(srv.Close)

	c, _ := New("pk_x", WithBaseURL(srv.URL))

	var gotErr error
	for _, err := range c.Stream(context.Background(), "missing", nil) {
		if err != nil {
			gotErr = err
		}
	}
	if gotErr == nil {
		t.Fatal("a 404 did not surface")
	}
	if apiErr, ok := AsError(gotErr); !ok || apiErr.Type != ErrTypeNotFound {
		t.Errorf("got %v, want a typed not_found", gotErr)
	}
}

// Breaking out must stop the walk and close the connection.
func TestBreakingOutOfStreamStops(t *testing.T) {
	c, _ := sseServe(t,
		`id: c1`+"\n"+`event: result`+"\n"+`data: {"item_index":0,"status":"succeeded","credits_used":1}`+"\n\n"+
			`id: c2`+"\n"+`event: result`+"\n"+`data: {"item_index":1,"status":"succeeded","credits_used":1}`+"\n\n"+
			`event: complete`+"\n"+`data: {}`+"\n\n",
	)

	n := 0
	for range c.Stream(context.Background(), "b-1", nil) {
		n++
		break
	}
	if n != 1 {
		t.Errorf("yielded %d entries after a break", n)
	}
}

// A data line larger than bufio.Scanner's 64KB default: a profile record is
// comfortably past it, and a scanner that hits its limit stops SILENTLY.
func TestStreamHandlesAnEntryLargerThanTheScannerDefault(t *testing.T) {
	big := strings.Repeat("x", 200*1024)
	c, _ := sseServe(t,
		`id: c1`+"\n"+`event: result`+"\n"+
			`data: {"item_index":0,"status":"succeeded","credits_used":1,"data":{"bio":"`+big+`"}}`+"\n\n"+
			`event: complete`+"\n"+`data: {}`+"\n\n",
	)

	n := 0
	for e, err := range c.Stream(context.Background(), "b-1", nil) {
		if err != nil {
			t.Fatalf("Stream: %v", err)
		}
		var rec struct {
			Bio string `json:"bio"`
		}
		if err := e.Into(&rec); err != nil {
			t.Fatalf("Into: %v", err)
		}
		if len(rec.Bio) != len(big) {
			t.Errorf("record truncated: %d bytes, want %d", len(rec.Bio), len(big))
		}
		n++
	}
	if n != 1 {
		t.Errorf("got %d entries, want 1", n)
	}
}
