# Indexer Search Recovery

Book searches use categorized dynamic discovery through the top-level Prowlarr
connection when configured. Without it, existing `torznab` sources are unchanged.
Audio uses category 3030; ebooks use 7020. Disabled or known-inapplicable sources
are excluded, but sources in a temporary Prowlarr cooldown remain planned.

Identical requests are coalesced and cached briefly within the process. Provider
cooldowns honor Retry-After. Errors/checkpoints expose safe provider names and
status metadata, not credential-bearing URLs or upstream response bodies.

HTTP 429 means that the endpoint is unavailable, not necessarily that traffic
exceeded its rate limit: Prowlarr can mask another upstream failure with a
cooldown. Inspect Prowlarr health and test credentials before assigning a cause.
Explicit configuration failures fail actionably; a manual retry probes the
repaired connection rather than replaying a stale failure indefinitely.

Incomplete searches are not successful empty searches. Healthy partial results
can proceed if the agent finds a suitable release. Otherwise acquisition sleeps
durably in Reflow until a deferred source can retry, without spending generic
step attempts. Cancelling a waiting acquisition remains supported.

Original successful search checkpoints retain their legacy array shape and step
names. A retry of one medium does not repeat completed acquisition work for the
other medium. Process-local caches are only an optimization; queued waits and
completed work survive restart in the execution journal.

Focused acceptance:

```sh
bun test test/unit/indexer-durability.test.ts test/integration/indexer-search-execution.test.ts
```
