# Shared Indexers And RSS

Books, movies, and TV share `src/media/indexer-gateway.ts`: discovery, bounded XML
reads, normalized Torznab results, coalescing, and provider-wide cooldowns.
RSS requests use `t=search` without a title query, scoped to movie, TV, audiobook,
or ebook categories (2000, 5000, 3030, and 7020 respectively, with configured book
category overrides preserved).
RSS capability is independent of interactive-search capability. Secrets and raw
download URLs stay server-side.

## Intake And Recovery

With Prowlarr or configured Torznab sources and library targets present, the worker
polls applicable feeds every 15 minutes. It does not periodically search each title.
Prowlarr discovery selects enabled torrent indexers with the requested category
and RSS capability; search-only indexers are not silently treated as feed sources.
Initial requests, explicit retry/upgrade checks, and the existing failure-recovery
paths may still perform targeted searches. Hourly movie reconciliation only
repairs managed views and pending publication.

Migration 42 adds provider feed checkpoints, a release inbox, and per-target
consideration receipts. A fresh feed consumes one current page. Subsequent polls
page back to the previous head, at most 10 pages of 100 items per provider.
The available provider history may be shorter. A missing boundary is recorded
as a persistent coverage gap, not a successful claim of complete coverage or a
trigger for library-wide searches. Historical gaps are backend diagnostics, not
permanent Activity/Connections alerts or a reason to rank a provider as unhealthy.

Migration 54 adds `indexer_feeds.gap_diagnostics_json`. New gaps atomically record
the paging stop reason (available history ended, pagination unsupported, repeated
page head, or the 10-page budget exhausted), pages/entries/distinct entries,
expected checkpoint count, reported total, pagination capability, and the previous
successful/attempted poll times with a preceding-failure flag. These are observed
conditions, not proof that entries were deleted or a provider was down throughout
the interval. Publication dates do not establish overlap.

`gap_since` retains the first historical warning; diagnostics describe the latest
observed gap using their own `detectedAt`. Successful later overlap retains that
historical evidence. Older gaps without diagnostics have an unknown cause.
Failed pages do not write a new gap diagnosis or advance the checkpoint. The
admin feed RPC exposes validated diagnostics, never provider URLs or credentials.
Connections deliberately omits historical gap counts, badges, warnings, and
expanded gap details. Current poll errors, cooldowns, unknown observations, and
retry times remain visible. Inspect `indexerFeeds.status` when investigating feed
history; hiding a non-actionable warning never clears its persisted evidence.

A failed page does not advance the head or commit a partial batch. Other
providers continue. Provider retry times survive restart. Inserting new inbox
entries, advancing the feed checkpoint, and creating matching jobs/outbox commands
are one synchronous SQLite transaction. Replays deduplicate by feed, provider
release identity, and meaningful content changes; changing seeder counts alone
does not create new work. Stale replay cannot rewind a newer head.

Expected provider failures are checkpointed **inside** each durable page step as
discriminated `{ kind: "success", page }` or `{ kind: "failure", failure }`
results. An `IndexerError` must not escape into Reflow's generic step retry/error
serialization before the feed can isolate it. Failure results contain only the
gateway's safe metadata, never the Error object, cause, request URL, or upstream
response body. Previously completed bare-page checkpoints remain replayable.

A failed page discards all buffered pages for that feed, records its safe health
and retry time in a separate durable step, and continues with the other
providers/media. The sync job does not sleep or fail merely because a provider is
unavailable or rejects its credentials. The next poll skips feeds whose persisted
retry time has not arrived; due feeds can try again. Gateway cooldowns also apply
across a provider's categories. HTTP 429 from Prowlarr does **not** establish actual
upstream rate limiting, and a reported cooldown does not establish its cause.
Cancellation, lost execution ownership, changed indexer configuration, unexpected
programming errors, and storage/checkpoint failures still abort the operation;
they are not converted into provider downtime or successful empty feeds.

## Movie Consumption

The feed matcher uses the existing agent credential/model to route batches to
potentially matching wanted movies. It is deliberately broad and does not use
quality/title heuristics to suppress uncertain editions before the normal agent
can inspect them. It receives title/catalog identity attributes, not download
URLs or arbitrary release descriptions. Projected fields are bounded, and oversized
batches split to stay within 32 KiB of serialized input without dropping pairs.

Matched candidates run through normal semantic selection, metainfo validation,
download, probing, quality policy, and Plex verification. Ordinary RSS evaluation
does not launch per-title searches. An actual stalled or invalid download can
trigger bounded replacement searches after its feed candidates are exhausted.
Equal/worse releases keep the current movie without searching, and
`movies.automaticUpgrades=false` excludes already-available targets. Missing
targets remain eligible. Busy targets wait durably rather than lose candidates.
Receipts are scoped to release, target, policy and version revision, with a causal
matching-job receipt to prevent replay from dispatching again after promotion.
Queuing the acquisition and its receipts is atomic. Selection/import explanations remain on
the acquisition, and routing explanations on the consideration association.

## Book Consumption

`BookFeedMatcher` uses the shared agent model/key to route audio and ebook inbox
entries against books missing that media. It performs broad semantic matching,
without a title-regex prefilter. Inputs contain bounded title/author/media and
identity attributes, not download URLs, API keys or arbitrary descriptions.
Oversized inputs split at 32 KiB of serialized UTF-8 JSON while preserving every
target/release pair. Unknown indices, cross-media matches, and empty evidence
are rejected before any acquisition is queued.

Books have no automatic edition-upgrade policy. Existing playable/downloadable
media is excluded from routing, checked again at dispatch, and checked again
before RSS acquisition/snatching. Books missing the other media remain eligible.
No existing manifestation, asset, download or file is removed or replaced.

`book_feed_match` queues ordinary `acquire` jobs with server-owned
`feedCandidateIds`, not client-authored torrent URLs. Queuing and consideration
receipts commit together. Receipts use `target_kind=book:audio` or `book:ebook`,
a revision of real catalog/media state plus edition preferences, and the causal
matching-job ID. Replaying a dispatched match cannot queue it again after import
or a preference change. An active acquisition/download/import for the same media
causes a durable wait, followed by a fresh readiness check, rather than dropping
candidates. An ebook-only outage does not hold up audio (an acquisition with
absent/empty media requests both formats).

The feed path resolves current inbox payloads, reuses `selectSearchCandidates`
for evidence-backed edition selection and ordered multipart groups, then uses
`runSnatchGroup`. Selected inbox IDs are checkpointed with the decision so current
URLs can be resolved again after selection or replay, immediately before snatching.
Initial readiness and pre-group eligibility are durable checkpoints. Once an owned
multipart group starts, replay completes its remaining parts even if its first
container has already imported and made the book appear ready.
Routing reasons remain on consideration associations and join selection reasons
on the manifestation's existing selection note. Failed-release exclusions are
forwarded to normal selection. A failed content hash stays excluded. A known
different hash can be reconsidered when a provider reuses the failed GUID and/or
download URL; snatching still validates the actual metainfo identity.

## Search and Metainfo Traffic

Automatic searches and cross-seed backfills use the existing agent key to plan a
primary title query and one optional alias. The gateway prefers a catalog ID
when the provider advertises support, using only catalog-backed numbering for
scoped TV searches. An empty ID search can fall back to the primary text query;
an empty text-only search can try the alias. Provider errors do not trigger
alternate requests. Backfill requests one page of 50 results by default, or
fewer if the provider advertises a lower maximum.

Inspection, acquisition and cross-seeding share cached metainfo for the same
provider listing and exact download URL. Unknown-hash acquisitions only reuse
recent entries. Confirmed nukes, already-owned identities and unsupported
client operations are skipped before downloading metainfo.

The admin metrics endpoint reports `podible_indexer_traffic_total` by purpose
and event. Only wire requests count as `search_id`, `search_text` or
`metainfo_fetch`; cache hits, joined requests, failures and prefetch skips are
separate counters. The reuse bucket includes RSS and other reuse triggers as
well as backfill, so it is not an exact measure of snatches per backfill search.

New matched releases are pooled with earlier considered evidence for that book
and media (up to 1,000 recent IDs, prioritizing the current batch). Thus a part
rejected because its companion was missing can be reconsidered when that companion
arrives in a later poll. Only the new unseen releases get new receipts; earlier
considerations retain their original acquisition/reason associations. The selector
is instructed not to acquire a known incomplete multipart edition.

Neither an empty routing result nor an edition rejection triggers a title search.
Initial/explicit acquisition is unchanged, and a real later download/import
failure may use the existing event-driven reacquisition path. This changes book
candidate intake, not the book content/view layout.

## Administration

Movie sync includes feed health, persisted coverage warnings, latest ingestion
and matching operations, and **Poll feeds now**. Admin-only JSON-RPC and typed
tRPC expose `indexerFeeds.status({})` and `indexerFeeds.sync({})`. Neither accepts
client-authored indexer URLs or returns credentials/download payloads.

## Typed Persistence

The new inbox repository uses Kysely only to compile SQL. `typedGet`, `typedAll`
and `typedRun` execute on the caller's existing synchronous `bun:sqlite`
connection and preserve transaction ownership. SQLite scalar values are not
silently converted into JSON, dates, booleans or domain enums; Zod validates
stored payloads.

`bun run generate:db-types` builds an isolated in-memory database from the real
migrations, finalizes the empty legacy job journal, and generates
`src/db/schema.ts`. `bun run check:db-types` fails on drift without editing files.
CI runs that check. Neither command opens production or Reflow's database.

## Verification

Run `bun test`, `bun run check:db-types`, `bun run typecheck`, `bun run build`,
`bun run validate:runtime-scripts` and the four browser acceptance scripts.
RSS acceptance uses synthetic upstream/agent/torrent/Plex seams with actual
SQLite, outbox, Reflow and filesystem logic. Book tests additionally cover
category-scoped audio/ebook intake, missing-only dispatch, receipt rollback and
replay, busy-target waits, URL rotation during selection, and a search-free ebook
snatch/import. This is not a claim of production
RSS matching accuracy or a new live upgrade canary. See
[upgrade acceptance boundaries](movie-upgrade-validation.md).

Focused feed-isolation checks:

```sh
bun test test/unit/indexer-feeds.test.ts test/unit/indexer-durability.test.ts test/unit/indexer-gateway.test.ts test/integration/indexer-feeds.test.ts test/integration/indexer-feeds-rpc.test.ts test/integration/book-feeds.test.ts test/integration/book-feed-scheduling.test.ts
bun run typecheck
```

The feed tests use the real SQLite Reflow journal to exercise provider cooldowns,
HTTP 429/503/403 isolation across movie/TV/audio/ebook, later-page failures without
partial ingestion, storage failure/restart/retry, cached-page replay, persisted
retry times with a fresh gateway, and eventual recovery. They also verify fatal
programming/configuration changes, in-flight cancellation, and that sensitive
upstream error bodies never reach failure checkpoints, health, or logs. Typed
source injection owns all four media (return `[]` for unwanted media), including
TV, so fixtures never fall back to real discovery. The legacy no-argument source
injection remains movie-only.

Wire behavior follows [Torznab/Newznab pagination and empty-query search](https://torznab.github.io/spec-1.3-draft/external/newznab/api.html)
and [Prowlarr's RSS/search/pagination capabilities](https://github.com/Prowlarr/Prowlarr/blob/develop/src/Prowlarr.Api.V1/Indexers/IndexerResource.cs).
