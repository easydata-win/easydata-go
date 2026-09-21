package easydata

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Verifying a webhook delivery.
//
// Two schemes ride on every delivery and you need only one of them.
//
// X-EasyData-Signature: t=<unix>,v1=<hex> is an HMAC-SHA256 over
// "<t>.<raw body>", keyed with your webhook secret.
//
// X-EasyData-Signature-Ed25519: t=<unix>,kid=<id>,wid=<endpoint>,v1b=<b64url>
// signs "<t>.<wid>.<raw body>" and is checked against the public key published
// at /.well-known/webhook-keys.json. It needs no secret at all, so a partner, a
// queue consumer or an edge function can check a delivery you forwarded them.
//
// Two rules that are easy to get wrong and silent when you do:
//
//   - Verify against the RAW body, exactly the bytes that arrived. Re-encoding
//     the decoded JSON changes key order and whitespace, and the signature will
//     not match a body you rebuilt. Read it with io.ReadAll before decoding.
//   - Never verify against X-EasyData-Timestamp. It carries the same unix
//     seconds but sits OUTSIDE both signed messages, so anyone replaying a
//     captured delivery can set it to whatever passes a freshness check. The t
//     inside the signature header is the only copy that cannot be edited
//     without breaking the signature, and it is the one these functions read.

// DefaultTolerance is the freshness window. Generous enough for a retry and a
// clock slightly out, short enough that a captured delivery goes stale.
const DefaultTolerance = 300 * time.Second

// The webhook events a customer can subscribe to.
//
// The four batch events are about work you submitted; the two credits ones are
// about the account paying for it, and they are the reason a pipeline stopped
// by a 402 can restart itself without anybody watching a dashboard.
const (
	EventBatchCompleted = "batch.completed"
	EventBatchFailed    = "batch.failed"
	EventBatchStarted   = "batch.started"
	EventBatchResult    = "batch.result"

	EventCreditsPurchased = "credits.purchased"
	EventCreditsLow       = "credits.low"
)

// Delivery is one webhook body.
//
// The event's own fields are in Data, ONE LEVEL DOWN - data.batch_id, never
// batch_id. Reading the top level instead is the quietest bug this API can hand
// you: the signature still verifies, the handler still returns 200, and every
// field is zero.
type Delivery struct {
	// ID is also the X-EasyData-Delivery header, and it is your idempotency
	// key: it is stable across every retry of the same delivery.
	ID        string          `json:"id"`
	Event     string          `json:"event"`
	CreatedAt time.Time       `json:"createdAt"`
	Data      json.RawMessage `json:"data"`
}

// Into unmarshals the event payload. Use CompletionPayload, StartedPayload,
// ResultPayload, PurchasePayload or LowPayload according to Event.
func (d Delivery) Into(dst any) error { return json.Unmarshal(d.Data, dst) }

// CompletionPayload is the Data of batch.completed and batch.failed.
type CompletionPayload struct {
	BatchID          string        `json:"batch_id"`
	Operation        Operation     `json:"operation"`
	Status           string        `json:"status"`
	Total            int           `json:"total"`
	Succeeded        int           `json:"succeeded"`
	Failed           int           `json:"failed"`
	CreditsUsed      float64       `json:"credits_used"`
	ResultsAvailable int           `json:"results_available"`
	ExternalID       string        `json:"external_id,omitempty"`
	WebhookTag       string        `json:"webhook_tag,omitempty"`
	CompletedAt      string        `json:"completed_at,omitempty"`
	Results          []ResultEntry `json:"results,omitempty"`
	ResultsOmitted   bool          `json:"results_omitted,omitempty"`
}

// StartedPayload is the Data of batch.started.
//
// It carries no counters, deliberately: a receiver that branched on Succeeded
// rather than on Event would read a batch that has just started as one that
// finished and scraped nothing.
type StartedPayload struct {
	BatchID    string    `json:"batch_id"`
	Operation  Operation `json:"operation"`
	Status     string    `json:"status"`
	Total      int       `json:"total"`
	ExternalID string    `json:"external_id,omitempty"`
	WebhookTag string    `json:"webhook_tag,omitempty"`
	StartedAt  string    `json:"started_at"`
}

// ResultPayload is the Data of batch.result: one entry, and enough of the batch
// around it to know what it belongs to.
type ResultPayload struct {
	BatchID    string      `json:"batch_id"`
	Operation  Operation   `json:"operation"`
	ExternalID string      `json:"external_id,omitempty"`
	WebhookTag string      `json:"webhook_tag,omitempty"`
	Result     ResultEntry `json:"result"`
}

// PurchasePayload is the Data of credits.purchased: credits landed, and this is
// the balance now.
//
// Subscribe to it when a pipeline of yours stops on insufficient_credits. It is
// what says the account can start again, and it arrives without anybody having
// to watch the balance.
type PurchasePayload struct {
	Pack    string  `json:"pack,omitempty"`
	Credits float64 `json:"credits"`
	Balance float64 `json:"balance"`
	Note    string  `json:"note,omitempty"`
}

// LowPayload is the Data of credits.low: the balance crossed the warning line.
//
// Once per emptying, not once per charge - a run that drains a balance delivers
// this once, at the crossing, and not again until a payment brings it back
// above the line.
type LowPayload struct {
	Balance   float64 `json:"balance"`
	Threshold float64 `json:"threshold"`
}

// VerifyWebhook checks the HMAC signature and returns the delivery.
//
//	func handler(w http.ResponseWriter, r *http.Request) {
//		body, _ := io.ReadAll(r.Body)
//		d, err := easydata.VerifyWebhook(secret, r.Header, body, 0)
//		if err != nil {
//			w.WriteHeader(http.StatusBadRequest)
//			return
//		}
//		if seen(d.ID) {   // stable across retries
//			return
//		}
//		handle(d)
//	}
//
// A zero tolerance means DefaultTolerance. Pass a negative one to switch the
// freshness check off, which is only ever right for a fixed test vector.
func VerifyWebhook(secret string, headers http.Header, body []byte, tolerance time.Duration) (*Delivery, error) {
	if secret == "" {
		return nil, fmt.Errorf("easydata: no signing secret: read it from GET /v1/account")
	}

	raw := headers.Get("X-EasyData-Signature")
	if raw == "" {
		return nil, fmt.Errorf("easydata: no X-EasyData-Signature header")
	}

	parts := parseSignature(raw)
	ts, sig := parts["t"], parts["v1"]
	if ts == "" || sig == "" {
		return nil, fmt.Errorf("easydata: malformed signature header %q", raw)
	}

	if err := checkAge(ts, tolerance); err != nil {
		return nil, err
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts))
	mac.Write([]byte("."))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))

	// Constant time: a byte-at-a-time comparison leaks where the first
	// difference is, which is enough to forge a digest given enough attempts.
	if subtle.ConstantTimeCompare([]byte(expected), []byte(sig)) != 1 {
		return nil, fmt.Errorf("easydata: signature does not match")
	}

	return decodeDelivery(body)
}

// VerifyWebhookEd25519 checks the asymmetric signature.
//
// webhookID is YOUR endpoint's id and passing it is not optional. One key signs
// for every customer on the deployment, so a delivery another customer
// legitimately received is a genuinely signed message; without checking wid,
// replaying theirs against your receiver verifies. The HMAC scheme needs no
// equivalent check, because your secret is only yours.
func VerifyWebhookEd25519(publicKeyBase64 string, headers http.Header, body []byte,
	webhookID string, tolerance time.Duration) (*Delivery, error) {

	if webhookID == "" {
		return nil, fmt.Errorf(
			"easydata: webhookID is required: one key signs for every customer, " +
				"so a delivery is only yours if wid matches your endpoint")
	}

	raw := headers.Get("X-EasyData-Signature-Ed25519")
	if raw == "" {
		return nil, fmt.Errorf("easydata: no X-EasyData-Signature-Ed25519 header")
	}

	parts := parseSignature(raw)
	ts, wid, sig := parts["t"], parts["wid"], parts["v1b"]
	if ts == "" || wid == "" || sig == "" {
		return nil, fmt.Errorf("easydata: malformed signature header %q", raw)
	}

	if subtle.ConstantTimeCompare([]byte(wid), []byte(webhookID)) != 1 {
		return nil, fmt.Errorf(
			"easydata: delivery was addressed to endpoint %s, not to %s", wid, webhookID)
	}

	if err := checkAge(ts, tolerance); err != nil {
		return nil, err
	}

	key, err := decodeAnyBase64(publicKeyBase64)
	if err != nil {
		return nil, fmt.Errorf("easydata: decoding public key: %w", err)
	}
	if len(key) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("easydata: public key is %d bytes, want %d",
			len(key), ed25519.PublicKeySize)
	}

	signature, err := decodeAnyBase64(sig)
	if err != nil {
		return nil, fmt.Errorf("easydata: decoding signature: %w", err)
	}

	message := append([]byte(ts+"."+wid+"."), body...)
	if !ed25519.Verify(ed25519.PublicKey(key), message, signature) {
		return nil, fmt.Errorf("easydata: signature does not match")
	}

	return decodeDelivery(body)
}

func decodeDelivery(body []byte) (*Delivery, error) {
	var d Delivery
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, fmt.Errorf("easydata: body is not a delivery: %w", err)
	}
	return &d, nil
}

func checkAge(ts string, tolerance time.Duration) error {
	if tolerance == 0 {
		tolerance = DefaultTolerance
	}
	if tolerance < 0 {
		return nil
	}
	secs, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return fmt.Errorf("easydata: signature timestamp is not a number: %q", ts)
	}
	age := time.Since(time.Unix(secs, 0))
	if age < 0 {
		age = -age
	}
	if age > tolerance {
		return fmt.Errorf("easydata: signature timestamp is outside %s", tolerance)
	}
	return nil
}

func parseSignature(header string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok {
			out[k] = v
		}
	}
	return out
}

// decodeAnyBase64 accepts the standard and URL alphabets, padded or not. A
// signature is base64url without padding and a public key is pasted into an env
// file by a human; which of the four forms arrives is not worth failing on.
func decodeAnyBase64(s string) ([]byte, error) {
	for _, enc := range []*base64.Encoding{
		base64.RawURLEncoding, base64.URLEncoding,
		base64.RawStdEncoding, base64.StdEncoding,
	} {
		if raw, err := enc.DecodeString(s); err == nil {
			return raw, nil
		}
	}
	return nil, fmt.Errorf("not base64 in any of the four spellings")
}
