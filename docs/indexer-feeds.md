# Shared Indexers And RSS

Books and movies share `src/media/indexer-gateway.ts`: discovery, bounded XML
reads, normalized Torznab results, coalescing, and provider-wide cooldowns.
RSS requests use `t=search` without a title query, scoped to movie categories.
RSS capability is independent of interactive-search capability. Secrets and raw
download URLs stay server-side.

## Intake And Recovery

With Prowlarr configured and movie targets present, the worker polls applicable
movie feeds every 15 minutes. It does not periodically search each title.
Initial requests, explicit retry/upgrade checks, and the existing failure-recovery
paths may still perform targeted searches. Hourly movie reconciliation only
repairs managed views and pending publication.

Migration 42 adds provider feed checkpoints, a release inbox, and per-target
consideration receipts. A fresh feed consumes one current page. Subsequent polls
page back to the previous head, at most 10 pages of 100 items per provider.
The available provider history may be shorter. A missing boundary is recorded
as a persistent coverage gap, not a successful claim of complete coverage or a
trigger for library-wide searches. Gaps are visible in Movie sync.

A failed page does not advance the head or commit a partial batch. Other
providers continue. Provider retry times survive restart. Inserting new inbox
entries, advancing the feed checkpoint, and creating matching jobs/outbox commands
are one synchronous SQLite transaction. Replays deduplicate by feed, provider
release identity, and meaningful content changes; changing seeder counts alone
does not create new work. Stale replay cannot rewind a newer head.

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

The inventory/gateway are media-neutral. The first automatic feed consumer is
movies; book search already uses the shared gateway, but book RSS consumption and
legacy book-storage/client migration are deliberately not part of this change.

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
SQLite, outbox, Reflow and filesystem logic. This is not a claim of production
RSS matching accuracy or a new live upgrade canary. See
[upgrade acceptance boundaries](movie-upgrade-validation.md).

Wire behavior follows [Torznab/Newznab pagination and empty-query search](https://torznab.github.io/spec-1.3-draft/external/newznab/api.html)
and [Prowlarr's RSS/search/pagination capabilities](https://github.com/Prowlarr/Prowlarr/blob/develop/src/Prowlarr.Api.V1/Indexers/IndexerResource.cs).
