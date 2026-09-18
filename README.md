# EasyData for Go

The official client for the [EasyData](https://easydata.win) LinkedIn
enrichment API.

```bash
go get github.com/easydata-win/easydata-go
```

No dependencies outside the standard library. Go 1.23+, for `iter.Seq2`.

## One record

```go
ed, err := easydata.New("") // reads EASYDATA_API_KEY

out, err := ed.Sync(ctx, easydata.OpProfilesEnrich,
    "https://linkedin.com/in/satyanadella", nil)

var person struct {
    FullName string `json:"full_name"`
}
if err := out.Result.Into(&person); err != nil {
    return err
}
```

`Sync` takes **one** target and answers with **one** record, in the response.
It costs double, it accepts no webhooks and no enrich, and for a paged operation
it returns one upstream page. Those bounds are refusals, not quiet downgrades.

If the deadline expires first you get `Complete == false` and a real `BatchID`,
never a 504 - so you keep the handle to results you may already have been
charged for.

## A batch

Everything is a batch, including a batch of one. There is no ceiling to discover
between one target and fifty thousand.

```go
b, err := ed.Submit(ctx, easydata.OpProfilesEnrich, targets, &easydata.SubmitOptions{
    ExternalID: "crm-sync",
    FindEmails: easydata.Bool(true),
})

for entry, err := range ed.Results(ctx, b.BatchID, nil) {
    if err != nil {
        return err
    }
    if !entry.OK() {
        log.Printf("%s: %s", entry.Input, entry.Error.Type) // CreditsUsed is 0
        continue
    }
    var person Person
    if err := entry.Into(&person); err != nil {
        return err
    }
    save(person)
}
```

`Results` is an `iter.Seq2[ResultEntry, error]`. Results **stream**: it yields
rows while the batch is still processing, so a long batch starts producing
immediately rather than after it finishes. It holds the cursor open until the
batch reaches a terminal state, sleeping for the server's own poll interval
between empty reads - bound that with `ctx`.

The error is yielded once and the walk then stops, so a caller who ignores it
iterates nothing rather than reading a failure as an empty batch. `break` stops
the walk immediately.

```go
ed.Results(ctx, id, &easydata.ResultsOptions{NoWait: true}) // readable now
ed.ResultsPageAt(ctx, id, cursor, 100)                      // one page + cursor
ed.Wait(ctx, id)                                            // counters, not rows
```

## Streaming instead of polling

```go
for entry, err := range ed.Stream(ctx, b.BatchID, nil) {
    if err != nil {
        return err
    }
    save(entry)
}
```

Same cursor, same entries, same order - the server pushes over a held
connection, so a long batch costs one request instead of hundreds against your
rate limit. Reconnects are handled and are exact: the event id IS the cursor, so
a dropped connection resumes with no duplicates and no gaps.

Use webhooks instead if you run a server with a public URL. This is for a client
with nowhere to deliver to: an agent on a laptop, a CLI, an edge function.

## Retries and double-billing

Retries cover exactly what is safe to repeat: `429`, `5xx` and a transport
failure. A `4xx` is returned immediately, because repeating a refusal only
spends the rate budget on a certain no. A cancelled context is never retried.

Every submission carries an `Idempotency-Key` that the client mints, so the
retry is free: a retried submit resolves to the batch the first attempt created
rather than creating a second and charging for it. Set `IdempotencyKey`
yourself if your caller's retry needs the same guarantee.

```go
ed, _ := easydata.New("",
    easydata.WithMaxRetries(5),
    easydata.WithHTTPClient(&http.Client{Timeout: 180 * time.Second}),
)
ed.RateLimits.Remaining // read off the last response, including a 429
```

`RateLimits` fields are `nil` when the deployment publishes no ceiling. That is
what "no limit" looks like on the wire: an absent header, never a zero.

## Errors

```go
if errors.Is(err, easydata.ErrQuotaExhausted) {
    // waiting for the month is the fix
}
if errors.Is(err, easydata.ErrEmailUnverified) {
    // also a 403, but clicking the link fixes it
}

if apiErr, ok := easydata.AsError(err); ok {
    log.Printf("%s field=%s request_id=%s", apiErr.Type, apiErr.Field, apiErr.RequestID)
    if apiErr.Transport() {
        // never produced a response: DNS, TLS, socket, timeout
    }
}
```

The sentinels match on the `Type` string, so an `*Error` built from a response
this version has never seen still compares correctly.

## Webhooks

```go
func handler(w http.ResponseWriter, r *http.Request) {
    // The RAW bytes: the signature is over what arrived, not over a body you
    // re-encoded after decoding it.
    body, _ := io.ReadAll(r.Body)

    d, err := easydata.VerifyWebhook(secret, r.Header, body, 0)
    if err != nil {
        w.WriteHeader(http.StatusBadRequest)
        return
    }
    if seen(d.ID) { // stable across every retry of the same delivery
        return
    }

    switch d.Event {
    case easydata.EventBatchResult:
        var p easydata.ResultPayload
        _ = d.Into(&p)
        handle(p.Result)
    case easydata.EventBatchCompleted:
        var p easydata.CompletionPayload
        _ = d.Into(&p)
        finish(p.BatchID)
    }
}
```

Four events: `batch.started`, `batch.result` (one per row), `batch.completed`
and `batch.failed`. The fields are in `d.Data`, one level down.

**Never verify against `X-EasyData-Timestamp`.** It sits outside both signed
messages, so a replayed delivery can set it to anything. The `t` inside the
signature header is the only copy that cannot be edited without breaking the
signature, and it is the one these functions read.

For the asymmetric scheme `webhookID` is required - one key signs for every
customer, so checking `wid` is what stops another customer's genuine delivery
verifying against your receiver.

```go
d, err := easydata.VerifyWebhookEd25519(publicKey, r.Header, body, myEndpointID, 0)
```

## Everything else

```go
ed.Batch(ctx, batchID)
ed.Batches(ctx, easydata.BatchFilter{Status: "completed", ExternalID: "crm"})
ed.Cancel(ctx, batchID) // delivered rows stay delivered and charged
ed.Account(ctx, &account)
ed.Usage(ctx, "2026-09-01", "2026-09-30", "", &usage)
```

Operations: `OpProfilesEnrich`, `OpProfilesActivity`, `OpProfilesPosts`,
`OpProfilesComments`, `OpProfilesReactions`, `OpCompaniesEnrich`,
`OpPostsEnrich`, `OpSalesSearchPeople`, `OpSalesSearchEmployees`,
`OpSalesSearchCompanies`.

## Reference

- [API reference](https://easydata.win/docs/api)
- [The batch model](https://easydata.win/docs/batches) - what a credit is, and why
- [OpenAPI 3.1](https://easydata.win/openapi.yaml)
