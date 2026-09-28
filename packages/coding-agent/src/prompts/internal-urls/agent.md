`agent://<id>`: output; nested IDs dotted, `/key/index` JSON path; write = message, `agent://all` broadcast only.

Write messages with `?urgent=1` to interrupt a busy peer's current turn at the next steering poll (~250ms) instead of queuing behind its tool boundary:

    write agent://request/<peer>?timeoutMs=30000&urgent=1 content="…urgent reply needed"

Normal sends (no urgent=1) queue for the next step boundary — safe when the peer is likely idle or already mid-turn on short work. Use urgent only when the timeout would expire before the peer's turn naturally yields: RPC timeouts, follow-ups to a slow peer, or messages that must break in before a long-running tool call completes.
