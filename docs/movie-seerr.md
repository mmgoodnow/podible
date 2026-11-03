# Independent Seerr Movie Acquisition

Seerr supplies request intent and catalog metadata. Podible searches releases,
selects/verifies them with its movie policy agent, downloads through its own
managed Transmission, adopts verified content, and publishes to its isolated Plex
movie section. It does not adopt Seerr/Radarr downloads or issue commands to
Seerr/Radarr. The optional read-only [shadow audit](movie-shadow.md) remains
separate and never gates import.

## Settings

Merge these fields into the existing administrator settings JSON. Preserve all
other settings and secrets, especially the legacy book `torznab` and `rtorrent`
configuration. There is no global `movies.enabled` flag and no movie indexer array.

```json
{
  "prowlarr": {
    "url": "http://prowlarr:9696/prowlarr",
    "apiKey": "<administrator-provided Prowlarr key>"
  },
  "movies": {
    "seerr": {
      "url": "http://seerr:5055/seerr",
      "apiKey": "<administrator-provided Seerr key>",
      "pollIntervalMinutes": 1
    },
    "managedRoot": "/media/MediaStorage/Media/podible-movies",
    "plexUrl": "http://172.18.0.1:32400",
    "plexToken": "<working Plex server token>",
    "plexSectionId": "15",
    "plexViewPath": "/media/MediaStorage/Media/podible-movies/views/plex/movies"
  }
}
```

Use actual service URLs, including URL bases only when configured. Keys travel in
`X-Api-Key` headers for discovery/feed calls; redirects are not followed. Seerr
provides catalog search/details when `movies.tmdbAccessToken` is absent. That
existing setting accepts either a TMDB read-access bearer token or a legacy v3
API key. An operator may temporarily supply Seerr's bundled key explicitly;
Podible does not automatically discover it or embed it in the codebase. Replace
it with your own credential in the same setting when available. The agent uses the
existing `agents.apiKey`. Keep the working server token in `movies.plexToken`;
do not assume an existing owner-auth token is valid for Plex library operations.

Prowlarr is one **top-level** URL/key connection. Movies dynamically discover
`GET /api/v1/indexer` and merge `GET /api/v1/indexerstatus`, refreshing after one
minute. Searches query all enabled, searchable torrent providers whose category
capabilities include movie category 2000 or its children. Explicit `enable=false`,
Usenet providers, non-searchable providers, and known non-movie categories are
excluded. A future `disabledTill` does **not** disable/remove a configured source:
it is planned but deferred. Missing capabilities remain deferred until discovery
can classify them. No static movie provider list needs maintenance.

Discovery failures, HTTP 429/auth/service failures, and malformed Torznab results
are not successful empty searches. All-unavailable searches wait in durable Reflow
steps. Partial results can acquire a suitable available release immediately; if
none qualifies, the job durably waits for missing providers and searches again,
without exhausting its three actual source/download attempts. A genuinely
successful search with no deferred providers and no suitable candidate can end
blocked. Partial caches expire no later than the next provider retry time.

If `prowlarr.url` is empty, movies retain the legacy `settings.torznab` fallback.
Movie acquisition does not replace the book rTorrent client or rewrite its legacy
Torznab configuration. Book searches also consume this shared connection when it
is configured, selecting audiobook category 3030 or ebook category 7020. See
[indexer recovery](indexer-recovery.md) for category discovery and outage handling.

## Feed And Backfill

The worker polls only when the Seerr URL and key are configured. First activation
persists the latest request ID as a no-history baseline, even across a crash
before Reflow saves its receipt. Normal sync imports approved **new** movie
requests with IDs greater than that baseline. Historical requests remain excluded
even if subsequently modified. A pending new request is captured when it is later
approved, because polling uses `sort=modified` and a two-minute overlap, not an
added-only feed. Seerr request statuses 2 (approved), 4 (failed after approval),
and 5 (completed upstream) qualify; pending=1, declined=3, and TV do not.

Migration 41 adds per-instance feed cursors and unique
`(instance, external_request_id)` origins. Every import atomically creates/reuses
its principal, movie request/target, origin, acquisition and job/outbox. A failure
before commit leaves none of them; a crash after commit reuses the origin instead
of duplicating work. Cursors advance only after the batch imports successfully.
Changing the Seerr connection fences a resumed sync and establishes a separate
feed namespace for a new sync.

An existing Podible Plex user is matched **only by Plex ID**, retaining its role.
An unknown requester becomes a local, nonadministrator `seerr:<instance>:<id>`
principal, with no session and no precreated Plex login. Requester display names,
emails, or claimed permissions cannot grant access. A 4K request gets a 2160-only
target while retaining explicit cut, language, codec, and HDR constraints. Stock
policy imposes no unrequested cut/language/HDR restrictions.

Administrator-only JSON-RPC and typed tRPC expose:

- `movieDualWrite.status({})`: baseline, activation time, connection readiness,
  isolated Plex section, and latest sync status, with keys/raw errors omitted.
- `movieDualWrite.list({"limit":50})`: current-instance origins and actual native
  request/job/stage/download progress and events; no provider URLs or passkeys.
- `movieDualWrite.sync({})`: queue normal new-request sync, never historical backfill.
- `movieDualWrite.backfill({"backfillRequestIds":[731,730,729]})`: queue real,
  independent acquisition for these explicitly selected IDs. Accepts one to five
  positive integer IDs; duplicates are idempotent. The complete batch must be
  approved movies before any acquisition starts. This is not a read-only audit.

For example, send the following authenticated JSON-RPC request to `/rpc`:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "movieDualWrite.backfill",
  "params": { "backfillRequestIds": [731, 730, 729] }
}
```

The administrator Movie sync page at `/movie-dual-write` includes independent
acquisition, manual sync, bounded backfill, and polled progress. The read-only
Radarr comparison audit is a collapsed section on the same page, loaded only
when opened; old `/movie-shadow` links redirect there. Nonadmins and
anonymous callers cannot inspect or control this feed through either RPC transport.

## Canary And Acceptance

The production integration owner handles backups, deployment/CD, private settings,
and the live canary. The designated isolated section is **15, Podible Dual-Write**,
with the sole root shown above. Preserve main movie section 2. The deliberately
selected canary IDs are **731, 730, 729**; do not expand this to an automatic
historical import. Verify successful sync, three origins/native requests, actual
Transmission transfer and probing, and verified publication into section 15.
Reissuing the same IDs must not create additional origins or acquisitions.

Local acceptance uses realistic GET-only Seerr/Prowlarr HTTP payloads, the actual
Reflow execution runtime, and an independent torrent/download/publication fixture.
It covers pending-to-approved transitions, bounded/validated backfill, role-safe
mapping, 4K constraints, transaction failures, crashes before cursor advancement,
real SIGKILL/restart after saved metadata and fanout, dynamic category/health
refresh, total/partial provider outages, and deferred waits beyond three retries.
The fixture proves native execution and isolated publication logic; it is not
evidence that a production transfer or Plex scan has succeeded.

```sh
bun test test/unit/movie-*.test.ts test/integration/movie-*.test.ts
bun run typecheck
bun run validate:runtime-scripts
```

Upstream wire contracts were checked against the primary implementations:
[Seerr request filtering/sorting](https://github.com/seerr-team/seerr/blob/develop/server/routes/request.ts),
[Seerr request statuses](https://github.com/seerr-team/seerr/blob/develop/server/constants/media.ts),
[Seerr movie metadata](https://github.com/seerr-team/seerr/blob/develop/server/models/Movie.ts),
[Prowlarr indexers](https://github.com/Prowlarr/Prowlarr/blob/develop/src/Prowlarr.Api.V1/Indexers/IndexerResource.cs),
and [Prowlarr health](https://github.com/Prowlarr/Prowlarr/blob/develop/src/Prowlarr.Api.V1/Indexers/IndexerStatusResource.cs).
