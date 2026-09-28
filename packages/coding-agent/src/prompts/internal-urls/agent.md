`agent://<id>`: output; nested IDs dotted, `/key/index` JSON path; write = message, `agent://all` broadcast only.

- **Retry ladder** for a busy peer:
    1. `write agent://request/<peer>?timeoutMs=30000` — ordinary; if the peer is mid-interruptible wait (bash/eval), the message cuts in immediately via the steering poll; if it's in an exclusive long tool call, the reply surfaces at the next tool boundary (size the timeout accordingly).
    2. If that times out with no reply, retry with `&urgent=1`: `write agent://request/<peer>?timeoutMs=30000&urgent=1` — forces immediate delivery even when the peer's wait is non-interruptible (backgroundable work detaches; exclusive tools still run to completion).

What `?urgent=1` actually preempts: within ~250ms of the steering poll — interruptible waits cut short; backgroundable foreground work (bash/eval) detaches; exclusive long tool calls still run to completion, then remaining batched tools are skipped. Use urgent only when the deadline is real. Both urgent and non-urgent messages cut interruptible waits; the differential shows up only when the wait is exclusive.

**Correlated replies**: a requester's `executeRequest` resolves on `candidate.replyTo === requestId`. The reply side must set `replyTo` via a write path; the write tool's URL accepts `?replyTo=<msgId>` as a query param so the outgoing frame carries the field: `write agent://<sender>?replyTo=<msgId> content="…"` The incoming inject already instructs you to do this for hub RPC replies. Without it, the requester falls back to the 120s `[rpc-timeout]` placeholder even when the body replies.
