// Package easydata is the official Go client for the EasyData LinkedIn
// enrichment API.
//
// Everything is a batch, including a batch of one, and you are billed per
// record that resolves: a failure costs nothing.
//
//	ed, err := easydata.New("")  // reads EASYDATA_API_KEY
//
//	// One record, in this call, at twice the credits.
//	out, err := ed.Sync(ctx, easydata.OpProfilesEnrich, "https://linkedin.com/in/satyanadella", nil)
//
//	// A batch of any size, streamed as it drains.
//	b, err := ed.Submit(ctx, easydata.OpProfilesEnrich, targets, &easydata.SubmitOptions{
//		ExternalID: "crm-sync",
//	})
//	for entry, err := range ed.Results(ctx, b.BatchID, nil) {
//		if err != nil {
//			return err
//		}
//		save(entry)
//	}
package easydata

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"math"
	mathrand "math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// DefaultBaseURL is the published base URL. Override it with New's option or
// EASYDATA_BASE_URL when pointing at a local deployment.
const DefaultBaseURL = "https://api.easydata.win/v1"

// Operation is one of the published operations.
type Operation string

const (
	OpProfilesEnrich Operation = "profiles.enrich"
	// OpProfilesEnrichDetailed is profiles.enrich plus the optional sections.
	// See the Include* fields on SubmitOptions.
	OpProfilesEnrichDetailed Operation = "profiles.enrich.detailed"
	OpProfilesActivity       Operation = "profiles.activity"
	OpProfilesPosts          Operation = "profiles.posts"
	OpProfilesComments       Operation = "profiles.comments"
	OpProfilesReactions      Operation = "profiles.reactions"
	OpCompaniesEnrich        Operation = "companies.enrich"
	OpPostsEnrich            Operation = "posts.enrich"
	OpSalesSearchPeople      Operation = "sales.search.people"
	OpSalesSearchEmployees   Operation = "sales.search.employees"
	OpSalesSearchCompanies   Operation = "sales.search.companies"
)

// paths maps an operation to its route. A new operation is a row here.
var paths = map[Operation]string{
	OpProfilesEnrich:         "/profiles/enrich",
	OpProfilesEnrichDetailed: "/profiles/enrich/detailed",
	OpProfilesActivity:       "/profiles/activity",
	OpProfilesPosts:          "/profiles/posts",
	OpProfilesComments:       "/profiles/comments",
	OpProfilesReactions:      "/profiles/reactions",
	OpCompaniesEnrich:        "/companies/enrich",
	OpPostsEnrich:            "/posts/enrich",
	OpSalesSearchPeople:      "/sales/search/people",
	OpSalesSearchEmployees:   "/sales/search/employees",
	OpSalesSearchCompanies:   "/sales/search/companies",
}

// Batch statuses. Queued and Processing are the pair that mean the batch can
// still produce something.
const (
	StatusQueued     = "queued"
	StatusProcessing = "processing"
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
	StatusCancelled  = "cancelled"
)

// Entry statuses.
const (
	EntrySucceeded      = "succeeded"
	EntryFailed         = "failed"
	EntryQuotaExhausted = "quota_exhausted"
)

// Batch is a submission and its progress.
type Batch struct {
	BatchID   string    `json:"batch_id"`
	Operation Operation `json:"operation"`
	Status    string    `json:"status"`

	Total     int `json:"total"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
	Pending   int `json:"pending"`

	// ResultsAvailable counts delivered ENTRIES, which for a paged operation is
	// more than the number of targets.
	ResultsAvailable int     `json:"results_available"`
	CreditsUsed      float64 `json:"credits_used"`

	ExternalID string `json:"external_id,omitempty"`
	WebhookTag string `json:"webhook_tag,omitempty"`

	// FindEmails asks for a work email per person returned, at a flat +4 per
	// verdict.
	//
	// Addresses are looked up after the scraping, so results carrying one
	// arrive minutes later than results that do not. Each entry still appears
	// exactly once, complete, and the batch is not completed until every one
	// has.
	FindEmails bool `json:"find_emails,omitempty"`

	// Priority is true on a batch a /sync call created. It is what explains the
	// credits: the same operation on the same target costs twice as much there.
	Priority bool `json:"priority,omitempty"`

	CreatedAt   time.Time  `json:"created_at"`
	CompletedAt *time.Time `json:"completed_at"`

	// RecommendedPollMs is how hard to poll. Server-set: polling faster does not
	// make the scrape finish sooner, it only spends the request budget.
	RecommendedPollMs int `json:"recommended_poll_ms,omitempty"`
}

// Done reports whether the batch will produce anything further.
func (b Batch) Done() bool {
	return b.Status != StatusQueued && b.Status != StatusProcessing
}

// EntryError is a per-item failure.
type EntryError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

// ResultEntry is one row of a batch's results.
//
// Data is left as raw JSON because its shape is the operation's: unmarshal it
// into whichever record you asked for. A single struct covering every operation
// would be a struct where most fields are always empty.
type ResultEntry struct {
	// ItemIndex is your submission order. Order of DELIVERY is never guaranteed.
	ItemIndex int             `json:"item_index"`
	Input     json.RawMessage `json:"input"`
	Status    string          `json:"status"`
	// CreditsUsed is zero on a failure: a failed fetch produced nothing.
	CreditsUsed float64 `json:"credits_used"`
	// Page is set only for a paged operation, where one target yields one entry
	// per upstream page.
	Page      *int            `json:"page,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
	Error     *EntryError     `json:"error,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

// OK reports whether this entry resolved.
func (e ResultEntry) OK() bool { return e.Status == EntrySucceeded }

// HasData reports whether this entry carries a record.
//
// A literal JSON null counts as no data, not as data. The API omits the field
// on a failure, but a receiver reading a delivery, a stored payload or a proxy
// that normalises absent keys can see `"data": null` - and that decodes into a
// json.RawMessage as the four bytes "null", which is not empty. Unmarshalling
// those into a struct succeeds and leaves it zeroed, which is the failure mode
// this exists to prevent: a failed lookup read as a person with no name.
func (e ResultEntry) HasData() bool {
	return len(e.Data) > 0 && string(e.Data) != "null"
}

// Into unmarshals the record into dst. It is an error to call it on a failed
// entry, which carries no data at all.
func (e ResultEntry) Into(dst any) error {
	if !e.HasData() {
		if e.Error != nil {
			return fmt.Errorf("easydata: entry %d failed (%s): %s",
				e.ItemIndex, e.Error.Type, e.Error.Message)
		}
		return fmt.Errorf("easydata: entry %d carries no data", e.ItemIndex)
	}
	return json.Unmarshal(e.Data, dst)
}

// SyncResult is what a /sync call answers with.
//
// Complete is the one field to branch on. False means the deadline expired
// before the target finished: Result may be nil, the target is still being
// worked at priority, and BatchID is a real batch whose results are readable.
// It is never a 504, because the caller must keep the handle to results they
// may already have been charged for.
type SyncResult struct {
	BatchID   string    `json:"batch_id"`
	Operation Operation `json:"operation"`
	Status    string    `json:"status"`
	Complete  bool      `json:"complete"`

	Result      *ResultEntry `json:"result"`
	CreditsUsed float64      `json:"credits_used"`

	ExternalID  string     `json:"external_id,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	CompletedAt *time.Time `json:"completed_at"`
}

type resultPage struct {
	BatchID string        `json:"batch_id"`
	Status  string        `json:"status"`
	Entries []ResultEntry `json:"entries"`
}

// meta is the envelope's second half. camelCase where data is snake_case.
type meta struct {
	RequestID         string `json:"requestId"`
	NextCursor        string `json:"nextCursor"`
	HasMore           bool   `json:"hasMore"`
	Total             int    `json:"total"`
	RecommendedPollMs int    `json:"recommendedPollMs"`
}

// SubmitOptions are the fields a submission may carry beyond its targets.
//
// Pointers for the booleans, because absent is not the same as false: Enrich
// set to false is the caller saying so, and a plain bool cannot tell that from
// "not mentioned".
type SubmitOptions struct {
	// ExternalID is your own correlation handle. It comes back on the batch, on
	// the completion webhook and in the usage rollup.
	ExternalID string
	// CallbackURL receives the COMPLETION. Mutually exclusive with WebhookTag.
	CallbackURL string
	// WebhookTag routes the completion to the endpoints carrying that tag.
	WebhookTag string
	// MaxResults caps a paged operation, and IS its billing unit count.
	MaxResults int
	// Enrich runs the matching enrich on every returned row. Paged only.
	Enrich *bool
	// FindEmails buys a work email per person returned, flat +4 per verdict.
	FindEmails *bool

	// The five Include* fields below are the optional sections of
	// OpProfilesEnrichDetailed, and are refused with a 400 on anything else.
	//
	// Each is one more request to LinkedIn and costs +0.5 per target, charged
	// only for a section that actually answered. At least one of them is
	// required on that operation: with none set it is OpProfilesEnrich, which
	// returns the same record for less.
	//
	// IncludeCounts fills FollowerCount and ConnectionCount.
	IncludeCounts *bool
	// IncludeExperienceDetail fills each position's Description, Location,
	// WorkplaceType, EmploymentType, Skills and TenureMonths.
	IncludeExperienceDetail *bool
	// IncludeSkillDetails fills SkillDetails: every skill with its endorsement
	// count. A superset of Skills, which stops at 20, so both are returned.
	IncludeSkillDetails *bool
	// IncludeInterests fills Interests: the people, companies, groups,
	// newsletters and schools this member follows, in one request whatever
	// they follow.
	IncludeInterests *bool
	// IncludeRecommendations fills Recommendations, both directions in one
	// request.
	IncludeRecommendations *bool

	// IncludeResults folds the rows into the completion when the batch fits.
	IncludeResults *bool
	// IdempotencyKey overrides the one the client mints. Rarely wanted: the
	// client already generates one per submission so its OWN retry cannot
	// create a second batch. Set it when your caller's retry needs to be
	// idempotent too.
	IdempotencyKey string
}

// SyncOptions are what a /sync call may carry. The bounds are refusals rather
// than downgrades, so there is no Enrich and no webhook field here at all.
type SyncOptions struct {
	ExternalID string
	MaxResults int

	// FindEmails buys the one person's work email, flat +8 here like every
	// other number on this path. Allowed synchronously for a profile, where one
	// person is one bounded lookup, and refused on a search, where it would be
	// one lookup per row inside a single HTTP request.
	FindEmails *bool

	// The optional sections, as on SubmitOptions. Allowed here - a section is
	// one bounded extra request, which is exactly the shape this path is for -
	// and priced like everything else here, at double: +1 each.
	IncludeCounts           *bool
	IncludeExperienceDetail *bool
	IncludeSkillDetails     *bool
	IncludeInterests        *bool
	IncludeRecommendations  *bool

	IdempotencyKey string
}

// ResultsOptions tunes the cursor walk.
type ResultsOptions struct {
	// PageSize is entries per request. Zero means 100.
	PageSize int
	// NoWait stops at what is readable right now instead of holding the cursor
	// open until the batch reaches a terminal state.
	NoWait bool
}

// RateLimits are the ceilings, read off the last response's headers.
//
// A nil field means the deployment publishes no ceiling for it, which on the
// wire is an ABSENT header and never a 0 - reading a missing header as zero
// would make an account with no limit look completely blocked.
type RateLimits struct {
	Limit     *int
	Remaining *int
	Reset     *int

	ConcurrentBatchLimit     *int
	ConcurrentBatchRemaining *int

	SyncLimit     *int
	SyncRemaining *int
	SyncReset     *int

	SyncConcurrentLimit     *int
	SyncConcurrentRemaining *int
}

// Option configures a Client.
type Option func(*Client)

// WithBaseURL points the client at a different deployment.
func WithBaseURL(u string) Option {
	return func(c *Client) { c.baseURL = strings.TrimRight(u, "/") }
}

// WithHTTPClient supplies the transport, for a custom timeout or a test.
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) { c.http = h }
}

// WithMaxRetries bounds the retries after the first attempt. Zero switches
// retrying off entirely.
func WithMaxRetries(n int) Option {
	return func(c *Client) { c.maxRetries = n }
}

// Client talks to the API.
//
// Retries cover exactly the failures that are safe to repeat - 429, 5xx and a
// transport error - and every submission carries an Idempotency-Key, so that
// repeating one is free: a retried submit resolves to the batch the first
// attempt created rather than creating a second and charging for it.
//
// It is safe for concurrent use, with one caveat: RateLimits reflects whichever
// response landed last.
type Client struct {
	apiKey     string
	baseURL    string
	http       *http.Client
	maxRetries int
	userAgent  string

	// RateLimits is the ceilings from the most recent response, including a
	// failed one - a 429 is where they matter most.
	RateLimits RateLimits
}

// New builds a client. An empty apiKey reads EASYDATA_API_KEY.
func New(apiKey string, opts ...Option) (*Client, error) {
	if apiKey == "" {
		apiKey = os.Getenv("EASYDATA_API_KEY")
	}
	if apiKey == "" {
		return nil, fmt.Errorf(
			"easydata: no API key: pass one to New or set EASYDATA_API_KEY")
	}

	base := os.Getenv("EASYDATA_BASE_URL")
	if base == "" {
		base = DefaultBaseURL
	}

	c := &Client{
		apiKey:  apiKey,
		baseURL: strings.TrimRight(base, "/"),
		// Above the server's own /sync deadline, which is what holds a
		// connection longest. A timeout under it would abandon a request that
		// was about to answer - and that request has already been charged.
		http:       &http.Client{Timeout: 150 * time.Second},
		maxRetries: 3,
		userAgent:  "easydata-go/1.0",
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

// Submit creates a batch. It answers as soon as the batch exists; nothing has
// run yet.
//
// A batch of one is a batch. There is no separate single-target path and no
// ceiling to discover between one target and fifty thousand.
func (c *Client) Submit(ctx context.Context, op Operation, targets []any, opts *SubmitOptions) (*Batch, error) {
	path, ok := paths[op]
	if !ok {
		return nil, fmt.Errorf("easydata: unknown operation %q", op)
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("easydata: targets is empty: a batch needs at least one target")
	}
	if opts == nil {
		opts = &SubmitOptions{}
	}

	body := map[string]any{"targets": targets}
	putString(body, "external_id", opts.ExternalID)
	putString(body, "callback_url", opts.CallbackURL)
	putString(body, "webhook_tag", opts.WebhookTag)
	putInt(body, "max_results", opts.MaxResults)
	putBool(body, "enrich", opts.Enrich)
	putBool(body, "find_emails", opts.FindEmails)
	putBool(body, "include_counts", opts.IncludeCounts)
	putBool(body, "include_experience_detail", opts.IncludeExperienceDetail)
	putBool(body, "include_skill_details", opts.IncludeSkillDetails)
	putBool(body, "include_interests", opts.IncludeInterests)
	putBool(body, "include_recommendations", opts.IncludeRecommendations)
	putBool(body, "include_results", opts.IncludeResults)

	key := opts.IdempotencyKey
	if key == "" {
		// Minted here rather than left to the caller, because the retry is
		// ours: without a key, our own retry of a submission that actually
		// succeeded creates a second batch and bills for it.
		key = newIdempotencyKey()
	}

	var out Batch
	if _, err := c.do(ctx, http.MethodPost, path, body, nil, key, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Sync looks up ONE entity and answers with its record, at twice the credits.
//
// target is singular - not a slice of one, which the API refuses with a 400
// naming the other field. The bounds are refusals rather than downgrades: no
// enrich, no webhooks, and one upstream page for a paged operation.
func (c *Client) Sync(ctx context.Context, op Operation, target any, opts *SyncOptions) (*SyncResult, error) {
	path, ok := paths[op]
	if !ok {
		return nil, fmt.Errorf("easydata: unknown operation %q", op)
	}
	if opts == nil {
		opts = &SyncOptions{}
	}

	body := map[string]any{"target": target}
	putString(body, "external_id", opts.ExternalID)
	putInt(body, "max_results", opts.MaxResults)
	putBool(body, "find_emails", opts.FindEmails)
	putBool(body, "include_counts", opts.IncludeCounts)
	putBool(body, "include_experience_detail", opts.IncludeExperienceDetail)
	putBool(body, "include_skill_details", opts.IncludeSkillDetails)
	putBool(body, "include_interests", opts.IncludeInterests)
	putBool(body, "include_recommendations", opts.IncludeRecommendations)

	key := opts.IdempotencyKey
	if key == "" {
		key = newIdempotencyKey()
	}

	var out SyncResult
	if _, err := c.do(ctx, http.MethodPost, path+"/sync", body, nil, key, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Batch reads one batch's current state.
func (c *Client) Batch(ctx context.Context, batchID string) (*Batch, error) {
	var out Batch
	if _, err := c.do(ctx, http.MethodGet, "/batches/"+batchID, nil, nil, "", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// BatchFilter narrows a listing.
type BatchFilter struct {
	Status     string
	Operation  Operation
	ExternalID string
	Limit      int
	Offset     int
}

// Batches lists your batches, newest first.
func (c *Client) Batches(ctx context.Context, f BatchFilter) ([]Batch, error) {
	q := url.Values{}
	if f.Status != "" {
		q.Set("status", f.Status)
	}
	if f.Operation != "" {
		q.Set("operation", string(f.Operation))
	}
	if f.ExternalID != "" {
		q.Set("external_id", f.ExternalID)
	}
	if f.Limit > 0 {
		q.Set("limit", strconv.Itoa(f.Limit))
	}
	if f.Offset > 0 {
		q.Set("offset", strconv.Itoa(f.Offset))
	}

	// data is the array itself; the listing's pagination is in meta.
	var out []Batch
	if _, err := c.do(ctx, http.MethodGet, "/batches", nil, q, "", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Cancel stops a batch. Entries already delivered stay delivered and charged.
func (c *Client) Cancel(ctx context.Context, batchID string) (*Batch, error) {
	var out Batch
	if _, err := c.do(ctx, http.MethodPost, "/batches/"+batchID+"/cancel", nil, nil, "", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ResultsPage is one read of the cursor.
//
// RecommendedPollMs is the server's own number and is zero once the batch is
// terminal, which is the signal that there is nothing left to wait for.
type ResultsPage struct {
	Status            string
	Entries           []ResultEntry
	NextCursor        string
	HasMore           bool
	RecommendedPollMs int
}

// StillRunning reports whether the batch can still produce something.
func (p ResultsPage) StillRunning() bool {
	return p.Status == StatusQueued || p.Status == StatusProcessing
}

// ResultsPageAt reads one page of the cursor and returns the cursor to continue
// from.
//
// Results is the loop you usually want. This is the layer under it, for a
// caller that owns its own paging - a worker that stores the cursor between
// runs, or a handler that must return rather than block. The cursor is opaque:
// hand back exactly what you were given.
func (c *Client) ResultsPageAt(ctx context.Context, batchID, cursor string, pageSize int) (*ResultsPage, error) {
	if pageSize <= 0 {
		pageSize = 100
	}

	q := url.Values{}
	q.Set("limit", strconv.Itoa(pageSize))
	if cursor != "" {
		q.Set("cursor", cursor)
	}

	var page resultPage
	m, err := c.do(ctx, http.MethodGet, "/batches/"+batchID+"/results", nil, q, "", &page)
	if err != nil {
		return nil, err
	}
	return &ResultsPage{
		Status:            page.Status,
		Entries:           page.Entries,
		NextCursor:        m.NextCursor,
		HasMore:           m.HasMore,
		RecommendedPollMs: m.RecommendedPollMs,
	}, nil
}

// Results streams a batch's entries, yielding each as it becomes readable.
//
// This is the point of the cursor: results are readable WHILE the batch is
// still processing, so a long batch starts producing rows immediately rather
// than after it finishes. By default the iterator holds the cursor open until
// the batch reaches a terminal state, sleeping for the server's own
// recommendedPollMs between empty reads; ResultsOptions.NoWait stops at what is
// readable now.
//
// The cursor is monotonic and gapless, so an entry is yielded exactly once and
// a batch still filling in never re-delivers one already seen.
//
// The second value is the error. It is yielded once and the iteration then
// stops, so a caller that ignores it iterates nothing rather than silently
// treating a failure as an empty batch. Bound the wait with ctx.
func (c *Client) Results(ctx context.Context, batchID string, opts *ResultsOptions) iter.Seq2[ResultEntry, error] {
	if opts == nil {
		opts = &ResultsOptions{}
	}
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}

	return func(yield func(ResultEntry, error) bool) {
		cursor := ""
		for {
			page, err := c.ResultsPageAt(ctx, batchID, cursor, pageSize)
			if err != nil {
				yield(ResultEntry{}, err)
				return
			}

			for _, e := range page.Entries {
				if !yield(e, nil) {
					return
				}
			}

			if page.NextCursor != "" {
				cursor = page.NextCursor
			}
			if page.HasMore {
				continue
			}

			if opts.NoWait || !page.StillRunning() {
				return
			}

			wait := time.Duration(page.RecommendedPollMs) * time.Millisecond
			if wait <= 0 {
				wait = 2 * time.Second
			}
			select {
			case <-ctx.Done():
				// The batch is unaffected and its results stay readable: the
				// caller's deadline ended the walk, not the work.
				yield(ResultEntry{}, ctx.Err())
				return
			case <-time.After(wait):
			}
		}
	}
}

// Wait blocks until a batch reaches a terminal state.
//
// Use Results when you want the rows: this polls the batch row, which carries
// the counters and not the records.
func (c *Client) Wait(ctx context.Context, batchID string) (*Batch, error) {
	for {
		b, err := c.Batch(ctx, batchID)
		if err != nil {
			return nil, err
		}
		if b.Done() {
			return b, nil
		}

		wait := time.Duration(b.RecommendedPollMs) * time.Millisecond
		if wait <= 0 {
			wait = 2 * time.Second
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
}

// Account reads the allowance, the ceilings and the webhook signing secret.
func (c *Client) Account(ctx context.Context, dst any) error {
	_, err := c.do(ctx, http.MethodGet, "/account", nil, nil, "", dst)
	return err
}

// Usage reads spend by day and operation. from and to are YYYY-MM-DD.
func (c *Client) Usage(ctx context.Context, from, to, externalID string, dst any) error {
	q := url.Values{}
	if from != "" {
		q.Set("from", from)
	}
	if to != "" {
		q.Set("to", to)
	}
	if externalID != "" {
		q.Set("external_id", externalID)
	}
	_, err := c.do(ctx, http.MethodGet, "/usage", nil, q, "", dst)
	return err
}

// do performs one call, with retries, and unmarshals data into dst.
func (c *Client) do(ctx context.Context, method, path string, body any, q url.Values,
	idempotencyKey string, dst any) (meta, error) {

	endpoint := c.baseURL + path
	if len(q) > 0 {
		endpoint += "?" + q.Encode()
	}

	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return meta{}, fmt.Errorf("easydata: encoding request: %w", err)
		}
	}

	var lastErr error
	for attempt := 0; ; attempt++ {
		var reader io.Reader
		if payload != nil {
			// A fresh reader per attempt: a retry of a consumed body sends an
			// empty one, which the API reads as a request with no targets.
			reader = bytes.NewReader(payload)
		}

		req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
		if err != nil {
			return meta{}, fmt.Errorf("easydata: building request: %w", err)
		}
		req.Header.Set("X-API-Key", c.apiKey)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", c.userAgent)
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		resp, err := c.http.Do(req)
		if err != nil {
			// The request never produced a response. Safe to repeat: a
			// submission carries an idempotency key and everything else is a
			// read. A cancelled context is not retried - the caller is gone.
			if ctx.Err() != nil {
				return meta{}, ctx.Err()
			}
			lastErr = &Error{Message: fmt.Sprintf("could not reach %s: %v", endpoint, err), wrapped: err}
			if attempt >= c.maxRetries {
				return meta{}, lastErr
			}
			if !sleepCtx(ctx, backoff(attempt, 0)) {
				return meta{}, ctx.Err()
			}
			continue
		}

		raw, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()

		c.RateLimits = readLimits(resp.Header)

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if readErr != nil {
				return meta{}, fmt.Errorf("easydata: reading response: %w", readErr)
			}
			var env struct {
				Data json.RawMessage `json:"data"`
				Meta meta            `json:"meta"`
			}
			if err := json.Unmarshal(raw, &env); err != nil {
				return meta{}, fmt.Errorf("easydata: decoding response: %w", err)
			}
			if dst != nil && len(env.Data) > 0 {
				if err := json.Unmarshal(env.Data, dst); err != nil {
					return env.Meta, fmt.Errorf("easydata: decoding data: %w", err)
				}
			}
			return env.Meta, nil
		}

		retryAfter := readRetryAfter(resp.Header)
		apiErr := errorFrom(resp.StatusCode, raw, retryAfter)

		// 429 and 5xx are the two the server is telling us to try again on.
		// Everything else is a refusal repeating cannot fix, and retrying it
		// would only spend the rate budget on a certain no.
		if !apiErr.Retryable() || attempt >= c.maxRetries {
			return meta{}, apiErr
		}
		lastErr = apiErr
		if !sleepCtx(ctx, backoff(attempt, retryAfter)) {
			return meta{}, ctx.Err()
		}
	}
}

// backoff is the server's own number when it sent one, otherwise exponential.
//
// Jittered because the failure that produces a retry storm is the one every
// client sees at the same instant, and an unjittered backoff reconverges them
// on the same second.
func backoff(attempt int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		return min(retryAfter, 60*time.Second)
	}
	base := math.Min(math.Pow(2, float64(attempt)), 30) * float64(time.Second)
	return time.Duration(base * (0.5 + mathrand.Float64()/2))
}

// sleepCtx reports false if the context ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func readRetryAfter(h http.Header) time.Duration {
	raw := h.Get("Retry-After")
	if raw == "" {
		return 0
	}
	secs, err := strconv.ParseFloat(raw, 64)
	if err != nil || secs < 0 {
		return 0
	}
	return time.Duration(secs * float64(time.Second))
}

func readLimits(h http.Header) RateLimits {
	num := func(name string) *int {
		raw := h.Get(name)
		if raw == "" {
			return nil
		}
		n, err := strconv.Atoi(raw)
		if err != nil {
			return nil
		}
		return &n
	}
	return RateLimits{
		Limit:                    num("X-RateLimit-Limit"),
		Remaining:                num("X-RateLimit-Remaining"),
		Reset:                    num("X-RateLimit-Reset"),
		ConcurrentBatchLimit:     num("X-Concurrent-Batch-Limit"),
		ConcurrentBatchRemaining: num("X-Concurrent-Batch-Remaining"),
		SyncLimit:                num("X-Sync-Limit"),
		SyncRemaining:            num("X-Sync-Remaining"),
		SyncReset:                num("X-Sync-Reset"),
		SyncConcurrentLimit:      num("X-Sync-Concurrent-Limit"),
		SyncConcurrentRemaining:  num("X-Sync-Concurrent-Remaining"),
	}
}

func newIdempotencyKey() string {
	buf := make([]byte, 16)
	if _, err := cryptorand.Read(buf); err != nil {
		// crypto/rand does not fail in practice, and a key that collides is
		// worse than one derived from the clock - so fall back rather than
		// send a submission with no key at all.
		return "idem-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return "idem-" + hex.EncodeToString(buf)
}

// The three putters exist so that absent and false stay different: a plain
// assignment of a zero value would send `max_results: 0`, which is a caller
// asking for none rather than one who said nothing.
func putString(m map[string]any, k, v string) {
	if v != "" {
		m[k] = v
	}
}

func putInt(m map[string]any, k string, v int) {
	if v != 0 {
		m[k] = v
	}
}

func putBool(m map[string]any, k string, v *bool) {
	if v != nil {
		m[k] = *v
	}
}

// Bool is a helper for the pointer fields on SubmitOptions.
//
//	opts := &easydata.SubmitOptions{Enrich: easydata.Bool(true)}
func Bool(v bool) *bool { return &v }
