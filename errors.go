package easydata

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// The error vocabulary. These are the strings the API sends as error.type, and
// they are the same set a per-item failure uses minus the ones that can only
// apply to a whole request.
const (
	ErrTypeInvalidRequest      = "invalid_request"
	ErrTypeInvalidAPIKey       = "invalid_api_key"
	ErrTypeQuotaExhausted      = "quota_exhausted"
	ErrTypeEmailUnverified     = "email_unverified"
	ErrTypeNotFound            = "not_found"
	ErrTypeConflict            = "conflict"
	ErrTypeUnprocessableTarget = "unprocessable_target"
	ErrTypeRateLimited         = "rate_limited"
	ErrTypeNotImplemented      = "not_implemented"
	ErrTypeInternal            = "internal_error"
	ErrTypeUpstreamTimeout     = "upstream_timeout"
	ErrTypeCapacityUnavailable = "capacity_unavailable"
)

// Sentinels for errors.Is. One per type worth branching on, matched on the
// Type string rather than on identity, so an Error built from a response the
// SDK has never seen still compares correctly.
var (
	ErrInvalidRequest      = &Error{Type: ErrTypeInvalidRequest}
	ErrInvalidAPIKey       = &Error{Type: ErrTypeInvalidAPIKey}
	ErrQuotaExhausted      = &Error{Type: ErrTypeQuotaExhausted}
	ErrEmailUnverified     = &Error{Type: ErrTypeEmailUnverified}
	ErrNotFound            = &Error{Type: ErrTypeNotFound}
	ErrConflict            = &Error{Type: ErrTypeConflict}
	ErrUnprocessableTarget = &Error{Type: ErrTypeUnprocessableTarget}
	ErrRateLimited         = &Error{Type: ErrTypeRateLimited}
	ErrNotImplemented      = &Error{Type: ErrTypeNotImplemented}
	ErrInternal            = &Error{Type: ErrTypeInternal}
	ErrUpstreamTimeout     = &Error{Type: ErrTypeUpstreamTimeout}
	ErrCapacityUnavailable = &Error{Type: ErrTypeCapacityUnavailable}
)

// Error is any failure the API reported, or any failure to reach it.
//
//	if errors.Is(err, easydata.ErrQuotaExhausted) { ... }
//
//	var apiErr *easydata.Error
//	if errors.As(err, &apiErr) && apiErr.Field == "targets" { ... }
type Error struct {
	// Type is the error.type string. Empty for a transport failure.
	Type string
	// Message is what the API said.
	Message string
	// Status is the HTTP status. Zero when no response was received.
	Status int
	// RequestID identifies the request in our logs. Quote it at support and it
	// can be found in one query.
	RequestID string
	// Field names the offending body key, when the API could identify one.
	Field string
	// RetryAfter is the server's own backoff, when it sent one.
	RetryAfter time.Duration

	wrapped error
}

func (e *Error) Error() string {
	out := "easydata: " + e.Message
	if e.Type != "" {
		out += fmt.Sprintf(" (type=%s", e.Type)
		if e.Status != 0 {
			out += fmt.Sprintf(", status=%d", e.Status)
		}
		if e.Field != "" {
			out += ", field=" + e.Field
		}
		if e.RequestID != "" {
			out += ", request_id=" + e.RequestID
		}
		out += ")"
	}
	return out
}

func (e *Error) Unwrap() error { return e.wrapped }

// Is matches on the Type string, which is what makes the sentinels above work
// against an Error built from any response.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	// A sentinel carries only a Type. Comparing the whole struct would mean a
	// real error never matched one, because it also has a message and a status.
	return t.Type != "" && t.Type == e.Type
}

// Transport reports whether the request never produced an API response - DNS,
// TLS, socket, timeout. Distinct from every error with a Type, which all mean
// "the API answered and said no".
func (e *Error) Transport() bool { return e.Status == 0 && e.Type == "" }

// Retryable reports whether repeating this request could succeed.
func (e *Error) Retryable() bool {
	return e.Transport() || e.Status == 429 || e.Status >= 500
}

// errorFrom builds the error for one response body.
//
// An unrecognised Type is carried through rather than flattened: the vocabulary
// is allowed to grow, and an SDK a version behind should still hand the caller
// something they can read and report.
func errorFrom(status int, raw []byte, retryAfter time.Duration) *Error {
	var envelope struct {
		Error struct {
			Type      string `json:"type"`
			Message   string `json:"message"`
			Field     string `json:"field"`
			RequestID string `json:"requestId"`
		} `json:"error"`
	}
	_ = json.Unmarshal(raw, &envelope)

	msg := envelope.Error.Message
	if msg == "" {
		// A body that is not our envelope is a proxy or a gateway answering.
		// Keep some of it: it is the only evidence of what actually replied.
		msg = fmt.Sprintf("HTTP %d", status)
		if len(raw) > 0 {
			snippet := raw
			if len(snippet) > 200 {
				snippet = snippet[:200]
			}
			msg += ": " + string(snippet)
		}
	}

	return &Error{
		Type:       envelope.Error.Type,
		Message:    msg,
		Status:     status,
		RequestID:  envelope.Error.RequestID,
		Field:      envelope.Error.Field,
		RetryAfter: retryAfter,
	}
}

// AsError is the errors.As shorthand, for reaching Field or RequestID.
func AsError(err error) (*Error, bool) {
	var e *Error
	ok := errors.As(err, &e)
	return e, ok
}
