# Read-Only TV Upstreams

These adapters observe upstream intent and metadata. They do not add, monitor,
search, refresh, import, delete or command anything in Seerr/Sonarr, adopt an
upstream download, or create acquisition jobs. Native acquisition, persistence,
Reflow scheduling, public API and UI belong to their respective application layers.

## Server Integration Contract

```ts
import { SeerrTvClient, selectedSeerrTvSeasons, TvUpstreamError } from "../src/tv/upstream";
import { SonarrCatalog, SonarrObserver } from "../src/tv/sonarr";

const feed = new SeerrTvClient(settings.movies.seerr);
const connection = {
  url: settings.tv.sonarrUrl,
  apiKey: settings.tv.sonarrApiKey,
  lookbackDays: settings.tv.lookbackDays,
};
const catalog = new SonarrCatalog(connection);
const observer = new SonarrObserver(connection);
```

`SeerrTvClient` imports/extends `movies/seerr.ts` settings and request schemas;
there is no second Seerr credential setting. Parent configuration maps the existing
server connection into flat `tv.sonarrUrl` / `tv.sonarrApiKey`. No `tv.enabled`
flag, discovery of private keys, or settings mutation happens in these adapters.
URL bases such as `/sonarr` and `/seerr` are preserved. Both clients accept an
optional injected `TvFetcher` for tests.

- `feed.instance`: the same URL-derived namespace as the movie Seerr feed.
- `feed.latest()`: `Promise<SeerrTvRequest | null>`, one newest-added TV request.
- `feed.changes(since)`: `Promise<{ requests: SeerrTvRequest[]; through: string }>`.
- `feed.request(id)`: explicit, identity-checked TV request detail; never called by a scan.
- `selectedSeerrTvSeasons(request)`: sorted, explicitly selected eligible seasons.
- `approvedSeerrTv(request)`: whether at least one such season is eligible.
- `catalog.instance`: SHA-256 of the normalized URL, truncated to 20 hexadecimal characters.
- `catalog.get(key)`: `Promise<TvCatalogEntry>` for an exact TVDB/TMDB/local key.
- `catalog.resolve({tvdbId?, tmdbId?, sonarrSeriesId?})`: exact external-ID lookup.
- `catalog.resolveSeerr(request)`: uses that request's explicit external IDs/association.
- `observer.observe()`: `Promise<SonarrObservation[]>`, comparison evidence only.

`SonarrObservation` contains `key`, `entry`, `episodeKeys`, `releaseTitle`,
`sourcePaths`, `libraryPath`, `observedAt`, and
`sonarr: { outcome: "imported" | "blocked", messages: string[] }`.
Observation keys include the Sonarr instance, endpoint and upstream record ID.
Paths are diagnostic evidence, not authorization to ingest content.

## Seerr Baseline And Cursor Ownership

First activation should atomically persist `latest().id` (or zero for an empty
feed) and a timestamp as the no-history baseline. The adapters persist nothing.
For normal polling the caller must exclude IDs at or below that original baseline,
even if an old request is subsequently modified. Do not start historical detail
lookups or acquisition fanout as part of activation.

Changes use `mediaType=tv`, `sort=modified`, descending order, 100 rows per page,
and an inclusive two-minute overlap before `since`. Pending rows are retained in
the returned batch so later approval is observable. Requests are deduplicated by
ID. `through` is the maximum validated update time, never earlier than `since`.
Empty/older-only scans do not move it forward to wall-clock time.

Eligibility matches the movie feed's approved lineage: request status 2 (approved),
4 (failed downstream after approval), or 5 (completed); **each selected season**
must independently have one of those statuses. Pending=1 and declined=3 are never
included. Explicit season zero is permitted; no specials or other seasons are
added implicitly. Missing/empty seasons, missing statuses, duplicate season
numbers, wrong media type, invalid IDs or malformed timestamps are errors, not an
instruction to request every season.

The caller selects stable episode keys using the catalog's provider-specific
aired-season mappings, not episode-key text or an assumed ordinal. In anime
projection, intersect the selected stable episode keys with explicit work
membership. An aggregate TVDB request may project to more than one separately
keyed work. Preserve all per-entry origins; finding one existing origin does not
mean the entire external request has already been handled.

Persist `through` only after the whole validated batch and every projected entry's
request/origin/jobs have committed. A malformed later page or any failure yields
no partial batch/cursor. Retrying the overlap requires idempotent repository
operations. The adapter does not own baseline, cursor, origin, or Reflow state.

## Identity And Numbering

The raw catalog is the foundation `TvCatalogEntry` input contract. Series keys
prefer `tvdb:<series.tvdbId>`, then `tmdb:<series.tmdbId>` if supplied. Episodes
prefer **`tvdb:<episode.tvdbId>`**, not the series TVDB ID, aired coordinates,
absolute number, title or a Sonarr-local ID masquerading as a global ID.

When no global ID exists, keys are `sonarr:<instance>:<localId>` in the separate
series/episode domains. Local episode identity is explicitly lifecycle-ephemeral;
deleting/readding an episode or changing Sonarr instances is not a stable match.
Sonarr external references always carry `instance` and source provenance.

Numbering is separate from identity, with source-labelled schemes:
`tvdb:aired`, `tvdb:absolute`, `sonarr:<instance>:aired`,
`sonarr:<instance>:absolute`, `sonarr:<instance>:scene`, and
`sonarr:<instance>:scene-absolute`. Only present wire numbers are mapped; zero
absolute numbers and unverified scene mappings are not published as usable maps.
Renumbering never changes the preferred TVDB episode key. Conflicting episode
IDs, duplicate stable keys or episodes attached to a different series fail closed.

`anime` means only `seriesType === "anime"` in Sonarr. It is not a conclusion
about a work's origin, and Western animation may carry that upstream classification.
Raw Sonarr aggregates are not split or merged into anime works. `relations` is
empty because titles/alternate titles do not establish relationships. Separate
anime enrichment can attach explicitly sourced AniDB/TVDB mappings and AniList
relationships while preserving stable episode identity and explicit membership.

Resolution never searches by title. A TVDB reference requires exact TVDB equality;
TMDB-only resolution requires exact TMDB equality in Sonarr's series list. When
both IDs are known, a conflicting supplied ID is an error. A Seerr Sonarr-local
association is usable only with an external identity cross-check. Standard and
4K associations use `externalServiceId` and `externalServiceId4k` respectively;
the standard association is never silently reused for a 4K request. Ambiguous or
unresolved metadata does not fall back to a similarly titled series.

## Bounds And Failures

Seerr scans are capped at 100 pages / 10,000 scanned rows. Each Sonarr history
and queue scan is capped at 10 pages / 1,000 rows; history stops once a complete
page predates the configured lookback. Short/inconsistent, repeated or misdirected
pages fail rather than reporting a successful truncated scan. Catalog array
limits are 10,000 series and 50,000 episodes for a series.

Sonarr history supplies imported events (`downloadFolderImported`); queue supplies
completed transfers/import-blocked or import-pending records. A queue pack without
an explicit episode association is not evidence for every episode in a season.
Catalog reads are deduplicated per series during one observation pass.

HTTP methods are GET only. API keys travel only in `X-Api-Key` headers; redirects
are refused. URLs reject credentials, queries and fragments. Transport/configuration,
HTTP and validation errors do not retain raw exceptions, URLs, bodies or headers.
Server credentials use ECMAScript private fields, not serializable settings fields.
Observation diagnostics redact URLs, configured keys and credential-like values.

`TvUpstreamError` exposes `provider`, `status`, `retryable`, and `retryAfterMs`.
HTTP 429/408/5xx and transport timeouts/outages are retryable. `Retry-After` accepts
seconds and HTTP dates; absent/invalid values use 60 seconds. The request/body
timeout is 30 seconds. There is **no internal sleep/retry loop**: the parent must
checkpoint a safe failure receipt and use a durable Reflow wait before retrying.
HTTP authentication/redirect errors and invalid metadata are not successful empty
feeds or catalogs.

## Verification And Primary Sources

```sh
bun test test/unit/tv-upstream.test.ts
bun run typecheck
bun run validate:runtime-scripts
```

Wire formats were checked against Sonarr's v3 implementation on `main` and Seerr
on `develop`. This adapter targets `/api/v3`, not the distinct Sonarr v5 API.

- [Sonarr episode resource and numbering fields](https://github.com/Sonarr/Sonarr/blob/main/src/Sonarr.Api.V3/Episodes/EpisodeResource.cs).
- [Sonarr series resource](https://github.com/Sonarr/Sonarr/blob/main/src/Sonarr.Api.V3/Series/SeriesResource.cs).
- [Sonarr series GET and TVDB filter](https://github.com/Sonarr/Sonarr/blob/main/src/Sonarr.Api.V3/Series/SeriesController.cs).
- [Sonarr episode GET by series](https://github.com/Sonarr/Sonarr/blob/main/src/Sonarr.Api.V3/Episodes/EpisodeController.cs).
- [Sonarr history GET and included resources](https://github.com/Sonarr/Sonarr/blob/main/src/Sonarr.Api.V3/History/HistoryController.cs).
- [Sonarr queue GET and supported sort keys](https://github.com/Sonarr/Sonarr/blob/main/src/Sonarr.Api.V3/Queue/QueueController.cs).
- [Seerr request filtering/sorting and joined seasons](https://github.com/seerr-team/seerr/blob/develop/server/routes/request.ts).
- [Seerr explicit season request schema](https://github.com/seerr-team/seerr/blob/develop/server/entity/SeasonRequest.ts).
- [Seerr request status constants](https://github.com/seerr-team/seerr/blob/develop/server/constants/media.ts).

Local fixtures verify wire parsing, failure behavior and GET-only requests. They
do not establish that production sync, acquisition, or publication has succeeded.
