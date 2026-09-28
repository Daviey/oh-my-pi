`agent://<id>`: output; nested IDs dotted, `/key/index` JSON path; write = message, `agent://all` broadcast only.

- **Retry ladder** for a busy peer:
    1. `write agent://request/<peer>?timeoutMs=30000` — ordinary; the reply surfaces at the peer's next step/tool boundary, so size the timeout to its longest in-flight tool.
    2. If that times out with no reply, retry with `&urgent=1`: `write agent://request/<peer>?timeoutMs=30000&urgent=1` — cuts into the peer's current turn.

What `?urgent=1` actually preempts: interruptible waits are cut short within ~250ms of the steering poll; backgroundable foreground work (bash/eval) detaches; but an exclusive long tool call still runs to completion, then remaining batched tools are skipped. Use urgent only when the deadline is real; normal sends queue for the next step boundary, which is safe when the peer is likely idle or mid-turn on short work.
