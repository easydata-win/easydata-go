package easydata

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// The client against a real server.
//
// httptest rather than a stubbed RoundTripper, because what is worth testing
// here IS the request: that a retried submit carries the same idempotency key,
// that the cursor stops when the batch does, that a 4xx is not retried.

type reply struct {
	status  int
	body    any
	headers map[string]string
}

type seen struct {
	method  string
	path    string
	query   string
	headers http.Header
	body    map[string]any
}

func serve(t *testing.T, replies ...reply) (*Client, *[]seen) {
	t.Helper()
	var got []seen
	i := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &body)
		}
		got = append(got, seen{
			method: r.Method, path: r.URL.Path, query: r.URL.RawQuery,
			headers: r.Header.Clone(), body: body,
		})

		rep := reply{status: 200, body: map[string]any{"data": map[string]any{}, "meta": map[string]any{}}}
		if i < len(replies) {
			rep = replies[i]
		}
		i++

		for k, v := range rep.headers {
			w.Header().Set(k, v)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rep.status)
		_ = json.NewEncoder(w).Encode(rep.body)
	}))
	t.Cleanup(srv.Close)

	c, err := New("pk_test_key", WithBaseURL(srv.URL))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c, &got
}

func batchBody(over map[string]any) map[string]any {
	d := map[string]any{
		"batch_id": "b-1", "operation": "profiles.enrich", "status": "queued",
		"total": 1, "succeeded": 0, "failed": 0, "pending": 1,
		"results_available": 0, "credits_used": 0, "recommended_poll_ms": 1,
		"created_at": "2026-09-01T10:00:00Z",
	}
	for k, v := range over {
		d[k] = v
	}
	return map[string]any{"data": d, "meta": map[string]any{"requestId": "req_1"}}
}

func entryBody(i int, over map[string]any) map[string]any {
	d := map[string]any{
		"item_index": i, "input": fmt.Sprintf("https://linkedin.com/in/p%d", i),
		"status": "succeeded", "credits_used": 1,
		"data":       map[string]any{"full_name": fmt.Sprintf("Person %d", i)},
		"created_at": "2026-09-01T10:00:00Z",
	}
	for k, v := range over {
		d[k] = v
	}
	return d
}

func resultsBody(entries []map[string]any, status, cursor string, hasMore bool) map[string]any {
	return map[string]any{
		"data": map[string]any{"batch_id": "b-1", "status": status, "entries": entries},
		"meta": map[string]any{
			"requestId": "req_1", "nextCursor": cursor,
			"hasMore": hasMore, "recommendedPollMs": 1,
		},
	}
}

func TestASubmissionCarriesAnIdempotencyKey(t *testing.T) {
	c, got := serve(t, reply{status: 202, body: batchBody(nil)})

	if _, err := c.Submit(context.Background(), OpProfilesEnrich, []any{"https://linkedin.com/in/x"}, nil); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if (*got)[0].headers.Get("Idempotency-Key") == "" {
		t.Error("a submission must carry an Idempotency-Key")
	}
}

// The whole reason the key is minted by the client and not the caller: without
// it, our own retry of a submission that actually succeeded creates a second
// batch and bills for it.
func TestARetriedSubmitReusesTheSameKey(t *testing.T) {
	c, got := serve(t,
		reply{status: 500, body: map[string]any{"error": map[string]any{"type": "internal_error"}}},
		reply{status: 202, body: batchBody(nil)},
	)

	if _, err := c.Submit(context.Background(), OpProfilesEnrich, []any{"x"}, nil); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(*got) != 2 {
		t.Fatalf("the 500 should have been retried, got %d attempts", len(*got))
	}
	first := (*got)[0].headers.Get("Idempotency-Key")
	second := (*got)[1].headers.Get("Idempotency-Key")
	if first != second {
		t.Errorf("a retry sent a different key (%s vs %s) and would double-bill", first, second)
	}
}

// A retry re-reads the body from a fresh reader. Consumed once, the second
// attempt would send an empty body, which the API reads as no targets.
func TestARetriedSubmitResendsTheBody(t *testing.T) {
	c, got := serve(t,
		reply{status: 503, body: map[string]any{"error": map[string]any{"type": "internal_error"}}},
		reply{status: 202, body: batchBody(nil)},
	)

	if _, err := c.Submit(context.Background(), OpProfilesEnrich, []any{"x", "y"}, nil); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	targets, ok := (*got)[1].body["targets"].([]any)
	if !ok || len(targets) != 2 {
		t.Errorf("the retry sent %#v, not the original two targets", (*got)[1].body)
	}
}

func TestSyncSendsTargetNotTargets(t *testing.T) {
	c, got := serve(t, reply{status: 200, body: map[string]any{
		"data": map[string]any{"batch_id": "b-1", "complete": true, "result": entryBody(0, nil)},
		"meta": map[string]any{},
	}})

	out, err := c.Sync(context.Background(), OpProfilesEnrich, "https://linkedin.com/in/x", nil)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if _, has := (*got)[0].body["target"]; !has {
		t.Error("sync must send `target`")
	}
	if _, has := (*got)[0].body["targets"]; has {
		t.Error("sync must not send `targets` - the API refuses it with a 400")
	}
	if !out.Complete {
		t.Error("complete was false")
	}

	var rec struct {
		FullName string `json:"full_name"`
	}
	if err := out.Result.Into(&rec); err != nil {
		t.Fatalf("Into: %v", err)
	}
	if rec.FullName != "Person 0" {
		t.Errorf("got %q", rec.FullName)
	}
}

func TestAnEmptyBatchNeverReachesTheWire(t *testing.T) {
	c, got := serve(t)
	if _, err := c.Submit(context.Background(), OpProfilesEnrich, nil, nil); err == nil {
		t.Fatal("an empty batch was accepted")
	}
	if len(*got) != 0 {
		t.Error("an empty batch reached the server")
	}
}

// Absent and false are different: Enrich set to false is the caller saying so,
// which is why the option is a *bool.
func TestFalseIsSentAndAbsentIsNot(t *testing.T) {
	c, got := serve(t, reply{status: 202, body: batchBody(nil)})

	_, err := c.Submit(context.Background(), OpProfilesEnrich, []any{"x"}, &SubmitOptions{
		Enrich: Bool(false), ExternalID: "run-7",
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	body := (*got)[0].body
	if v, has := body["enrich"]; !has || v != false {
		t.Errorf("enrich=false was dropped: %#v", body)
	}
	if body["external_id"] != "run-7" {
		t.Errorf("external_id: %#v", body["external_id"])
	}
	if _, has := body["find_emails"]; has {
		t.Error("an option that was never set was sent anyway")
	}
}

func TestA4xxIsNotRetriedAndCarriesItsField(t *testing.T) {
	c, got := serve(t, reply{status: 400, body: map[string]any{"error": map[string]any{
		"type": "invalid_request", "message": "bad", "field": "targets", "requestId": "req_9",
	}}})

	_, err := c.Submit(context.Background(), OpProfilesEnrich, []any{"x"}, nil)
	if err == nil {
		t.Fatal("a 400 was not reported")
	}
	if len(*got) != 1 {
		t.Errorf("a refusal repeating cannot fix was retried %d times", len(*got)-1)
	}
	if !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("errors.Is(ErrInvalidRequest) was false for %v", err)
	}
	apiErr, ok := AsError(err)
	if !ok {
		t.Fatalf("AsError failed for %T", err)
	}
	if apiErr.Field != "targets" || apiErr.RequestID != "req_9" || apiErr.Status != 400 {
		t.Errorf("error lost detail: %+v", apiErr)
	}
	if apiErr.Retryable() {
		t.Error("a 400 reported itself as retryable")
	}
}

func TestSentinelsDoNotMatchAcrossTypes(t *testing.T) {
	err := errorFrom(403, []byte(`{"error":{"type":"quota_exhausted","message":"spent"}}`), 0)

	if !errors.Is(err, ErrQuotaExhausted) {
		t.Error("quota_exhausted did not match its sentinel")
	}
	// The two 403s mean opposite things: one is fixed by clicking a link and
	// the other by waiting for the month.
	if errors.Is(err, ErrEmailUnverified) {
		t.Error("quota_exhausted matched the email_unverified sentinel")
	}
}

func TestRateLimitsAreReadAndAbsentMeansNil(t *testing.T) {
	c, _ := serve(t, reply{status: 202, body: batchBody(nil), headers: map[string]string{
		"X-RateLimit-Limit": "600", "X-RateLimit-Remaining": "599",
	}})

	if _, err := c.Submit(context.Background(), OpProfilesEnrich, []any{"x"}, nil); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if c.RateLimits.Limit == nil || *c.RateLimits.Limit != 600 {
		t.Errorf("limit: %v", c.RateLimits.Limit)
	}
	// "No limit" on the wire is an ABSENT header, never a 0: reading a missing
	// one as zero would make an unlimited account look completely blocked.
	if c.RateLimits.SyncLimit != nil {
		t.Errorf("an absent ceiling read as %v rather than nil", *c.RateLimits.SyncLimit)
	}
}

func TestResultsPagesUntilHasMoreIsFalse(t *testing.T) {
	c, got := serve(t,
		reply{status: 200, body: resultsBody([]map[string]any{entryBody(0, nil)}, "processing", "c1", true)},
		reply{status: 200, body: resultsBody([]map[string]any{entryBody(1, nil)}, "completed", "c2", false)},
	)

	var indexes []int
	for e, err := range c.Results(context.Background(), "b-1", nil) {
		if err != nil {
			t.Fatalf("Results: %v", err)
		}
		indexes = append(indexes, e.ItemIndex)
	}

	if len(indexes) != 2 || indexes[0] != 0 || indexes[1] != 1 {
		t.Errorf("got %v", indexes)
	}
	if (*got)[1].query != "cursor=c1&limit=100" {
		t.Errorf("the second page did not carry the cursor: %q", (*got)[1].query)
	}
}

// Results stream: an empty read of a processing batch is not the end.
func TestResultsKeepsWaitingWhileTheBatchIsLive(t *testing.T) {
	c, _ := serve(t,
		reply{status: 200, body: resultsBody(nil, "processing", "", false)},
		reply{status: 200, body: resultsBody([]map[string]any{entryBody(0, nil)}, "processing", "", false)},
		reply{status: 200, body: resultsBody([]map[string]any{entryBody(1, nil)}, "completed", "", false)},
	)

	var n int
	for _, err := range c.Results(context.Background(), "b-1", nil) {
		if err != nil {
			t.Fatalf("Results: %v", err)
		}
		n++
	}
	if n != 2 {
		t.Errorf("got %d entries, want 2", n)
	}
}

func TestNoWaitStopsAtWhatIsReadableNow(t *testing.T) {
	c, got := serve(t,
		reply{status: 200, body: resultsBody([]map[string]any{entryBody(0, nil)}, "processing", "", false)},
	)

	var n int
	for _, err := range c.Results(context.Background(), "b-1", &ResultsOptions{NoWait: true}) {
		if err != nil {
			t.Fatalf("Results: %v", err)
		}
		n++
	}
	if n != 1 || len(*got) != 1 {
		t.Errorf("NoWait polled again: %d entries over %d requests", n, len(*got))
	}
}

// Breaking out of the range must stop the walk, not leave it polling.
func TestBreakingOutOfResultsStops(t *testing.T) {
	c, got := serve(t,
		reply{status: 200, body: resultsBody(
			[]map[string]any{entryBody(0, nil), entryBody(1, nil)}, "processing", "c1", true)},
		reply{status: 200, body: resultsBody([]map[string]any{entryBody(2, nil)}, "completed", "", false)},
	)

	for range c.Results(context.Background(), "b-1", nil) {
		break
	}
	if len(*got) != 1 {
		t.Errorf("the walk continued after break: %d requests", len(*got))
	}
}

// The error is yielded once and the walk then stops, so a caller who ignores it
// iterates nothing rather than reading a failure as an empty batch.
func TestResultsYieldsItsErrorOnce(t *testing.T) {
	c, _ := serve(t, reply{status: 404, body: map[string]any{
		"error": map[string]any{"type": "not_found", "message": "no such batch"},
	}})

	var entries, errs int
	for _, err := range c.Results(context.Background(), "missing", nil) {
		if err != nil {
			errs++
			if !errors.Is(err, ErrNotFound) {
				t.Errorf("wrong error: %v", err)
			}
			continue
		}
		entries++
	}
	if entries != 0 || errs != 1 {
		t.Errorf("got %d entries and %d errors, want 0 and 1", entries, errs)
	}
}

func TestAFailedEntryCostsNothingAndCarriesItsError(t *testing.T) {
	c, _ := serve(t, reply{status: 200, body: resultsBody([]map[string]any{
		entryBody(0, map[string]any{
			"status": "failed", "credits_used": 0, "data": nil,
			"error": map[string]any{"type": "unprocessable_target", "message": "no such profile"},
		}),
	}, "completed", "", false)})

	for e, err := range c.Results(context.Background(), "b-1", nil) {
		if err != nil {
			t.Fatalf("Results: %v", err)
		}
		if e.OK() {
			t.Error("a failed entry reported OK")
		}
		if e.CreditsUsed != 0 {
			t.Errorf("a failure was charged %v", e.CreditsUsed)
		}
		if e.Error == nil || e.Error.Type != "unprocessable_target" {
			t.Errorf("error: %+v", e.Error)
		}
		// Into on a failed entry explains rather than unmarshalling nothing.
		var dst map[string]any
		if err := e.Into(&dst); err == nil {
			t.Error("Into on a failed entry did not report the failure")
		}
	}
}

func TestBatchesReadsTheArrayStraightOutOfData(t *testing.T) {
	c, _ := serve(t, reply{status: 200, body: map[string]any{
		"data": []any{batchBody(nil)["data"], batchBody(map[string]any{"batch_id": "b-2"})["data"]},
		"meta": map[string]any{"total": 2},
	}})

	rows, err := c.Batches(context.Background(), BatchFilter{Status: "completed"})
	if err != nil {
		t.Fatalf("Batches: %v", err)
	}
	if len(rows) != 2 || rows[1].BatchID != "b-2" {
		t.Errorf("got %+v", rows)
	}
}

func TestAnUnknownOperationIsRefusedLocally(t *testing.T) {
	c, got := serve(t)
	if _, err := c.Submit(context.Background(), Operation("profiles.invent"), []any{"x"}, nil); err == nil {
		t.Fatal("an unknown operation was accepted")
	}
	if len(*got) != 0 {
		t.Error("an unknown operation reached the server")
	}
}

// ------------------------------------------------------------------ webhooks

const testSecret = "whsec_test"

func signHMAC(t *testing.T, body []byte, ts int64) http.Header {
	t.Helper()
	mac := hmac.New(sha256.New, []byte(testSecret))
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write(body)

	h := http.Header{}
	h.Set("X-EasyData-Signature",
		fmt.Sprintf("t=%d,v1=%s", ts, hex.EncodeToString(mac.Sum(nil))))
	return h
}

func TestAGoodSignatureUnpacksTheDelivery(t *testing.T) {
	body := []byte(`{"id":"d-1","event":"batch.completed","createdAt":"2026-09-01T10:00:00Z","data":{"batch_id":"b-1","succeeded":494}}`)

	d, err := VerifyWebhook(testSecret, signHMAC(t, body, time.Now().Unix()), body, 0)
	if err != nil {
		t.Fatalf("VerifyWebhook: %v", err)
	}
	if d.ID != "d-1" || d.Event != EventBatchCompleted {
		t.Errorf("got %+v", d)
	}

	var p CompletionPayload
	if err := d.Into(&p); err != nil {
		t.Fatalf("Into: %v", err)
	}
	if p.Succeeded != 494 {
		t.Errorf("succeeded: %d", p.Succeeded)
	}
}

func TestATamperedBodyFails(t *testing.T) {
	body := []byte(`{"id":"d-1","event":"batch.completed","data":{}}`)
	headers := signHMAC(t, body, time.Now().Unix())

	if _, err := VerifyWebhook(testSecret, headers, append(body, ' '), 0); err == nil {
		t.Fatal("a tampered body verified")
	}
}

func TestAStaleTimestampFails(t *testing.T) {
	body := []byte(`{"id":"d-1","event":"batch.completed","data":{}}`)
	headers := signHMAC(t, body, time.Now().Add(-2*time.Hour).Unix())

	if _, err := VerifyWebhook(testSecret, headers, body, 0); err == nil {
		t.Fatal("a stale delivery verified")
	}
}

// A vector produced by backend/internal/webhooks.Sign itself.
//
// Every other test here signs with this file's own HMAC, which would keep
// passing if both sides were wrong in the same way. This one is the output of
// the server that will actually sign deliveries, pinned - so a change to either
// implementation that breaks the other fails here rather than in a customer's
// receiver. Regenerate with Sign("whsec_cross_check", time.Unix(1789000000, 0), body).
func TestTheServersOwnSignatureVerifies(t *testing.T) {
	body := []byte(`{"id":"d-1","event":"batch.result","createdAt":"2026-09-01T10:00:00Z","data":{"batch_id":"b-1"}}`)
	h := http.Header{}
	h.Set("X-EasyData-Signature",
		"t=1789000000,v1=f58f4775db8b4ddd9a99b8a8ed28fc2ea6a58cc888a433526f244c5140e8022b")

	// A negative tolerance switches the freshness check off: the vector is fixed
	// in time and the thing under test is the digest.
	d, err := VerifyWebhook("whsec_cross_check", h, body, -1)
	if err != nil {
		t.Fatalf("the server's own signature did not verify: %v", err)
	}
	if d.Event != EventBatchResult {
		t.Errorf("event: %s", d.Event)
	}

	var p ResultPayload
	if err := d.Into(&p); err != nil {
		t.Fatalf("Into: %v", err)
	}
	if p.BatchID != "b-1" {
		t.Errorf("batch_id: %s", p.BatchID)
	}
}

// One key signs for every customer, so wid is the whole check.
func TestEd25519RefusesADeliveryAddressedElsewhere(t *testing.T) {
	h := http.Header{}
	h.Set("X-EasyData-Signature-Ed25519",
		fmt.Sprintf("t=%d,kid=k1,wid=other,v1b=AAAA", time.Now().Unix()))

	_, err := VerifyWebhookEd25519("AAAA", h, []byte("{}"), "mine", 0)
	if err == nil {
		t.Fatal("a delivery addressed to another endpoint verified")
	}
}

func TestEd25519RequiresTheEndpointID(t *testing.T) {
	h := http.Header{}
	h.Set("X-EasyData-Signature-Ed25519", "t=1,kid=k1,wid=x,v1b=AAAA")

	if _, err := VerifyWebhookEd25519("AAAA", h, []byte("{}"), "", 0); err == nil {
		t.Fatal("verification without an endpoint id was allowed")
	}
}
