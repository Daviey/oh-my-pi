<irc>
Incoming IRC message from agent `{{from}}`{{#if replyTo}} (reply to {{replyTo}}){{/if}}{{#if msgId}} [id: {{msgId}}]{{/if}}{{#if fromSessionId}} [sender session: {{fromSessionId}}]{{/if}}:

{{message}}

{{#if interrupting}}Sent while waiting/working. Active interruptible wait stopped early for immediate reading.{{/if}}

{{#if relayOnStop}}If response expected, reply via `write` (`path: "{{writeTarget}}"`, `content: "…"`), when available; otherwise what you `yield` or say last this turn is delivered to `{{from}}` when you stop. No one replies on your behalf.{{else}}If a reply is expected{{#if msgId}} (this message id: `{{msgId}}` — set `replyTo: "{{msgId}}"` so a requester's correlation matches){{/if}}, reply via the irc send path to `{{replyTarget}}`{{#unless fromSessionId}} (bare `{{from}}` — session id not stamped; if ambiguous cross-project, resolve via `agent://peers` and target `session:<id>`){{/unless}}; otherwise reply via `write` (`path: "{{writeTarget}}"`, `content: "…"`). You may finish your current step first. No one replies on your behalf.{{/if}}
</irc>
