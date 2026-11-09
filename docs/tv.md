# TV Acquisition And Comparison

Podible independently acquires newly approved Seerr TV requests while Sonarr
continues operating normally. `/tv-sync` combines actual native acquisition
progress with a separate, read-only Sonarr comparison audit. A "Would import"
audit is never presented as a completed native download.

## Content Model

Works, episodes, relationships and numbering schemes are separate. This is not
an animation-versus-live-action hierarchy: any work may have separately identified
sequels, related entries, specials and alternative provider groupings. Sonarr's
anime classification is only an enrichment hint. Provider seasons and absolute
numbers do not establish episode identity.

Seerr approval is interpreted using TMDB episode membership, not matching season
numbers. A persisted episode crosswalk can split or combine provider season
groups while leaving TVDB/Plex identities unchanged. Exact provider IDs and
unique title/date anchors avoid unnecessary research; tool-first agent batches
resolve title/date disagreements and retain their evidence on episode metadata.
Incomplete mappings preserve the whole approval for automatic retry, not a
partial download. See [Tom and Jerry crosswalk](tom-and-jerry-shadow.md).

An episode has stable provider references. Multiple works may reference an
episode, with entry-scoped numbering. Explicit AniDB/TVDB mappings and AniList
relationships can split an aggregate into logical works; unknown mappings retain
the original provider entry with warnings. See [anime metadata](tv-anime.md).

Seasons are the product request unit. Requests consume every known episode in
the explicitly approved upstream seasons, including future episodes. Episode IDs
remain the internal import, availability and provider-projection units; they are
not an individual-episode request surface. Separately identified works and cours
still retain their sourced mappings rather than becoming artificial seasons.

## Settings

The existing administrator settings JSON accepts:

```json
{
  "tv": {
    "sonarrUrl": "http://sonarr:8989/sonarr",
    "sonarrApiKey": "<existing Sonarr API key>",
    "managedRoot": "/media/MediaStorage/Media/podible-tv",
    "plexSectionId": "<isolated TV section ID>",
    "plexViewPath": "/media/MediaStorage/Media/podible-tv/views/plex/tv",
    "pollIntervalMinutes": 1,
    "lookbackDays": 7,
    "maxCasesPerSync": 5,
    "maximumReleaseBytes": null,
    "pathMappings": [
      { "remoteRoot": "/media", "localRoot": "/media" }
    ]
  }
}
```

Reuse `movies.seerr` and `movies.plexUrl` / `plexToken`, top-level `prowlarr`,
and the existing agent API key/model. There is no enable flag, second Seerr/Plex
credential, TV indexer list, public torrent endpoint, or model selector.

## Execution And Safety

- First Seerr activation saves a no-history baseline before its checkpoint. Only
  explicitly approved seasons of new requests are consumed. Historical backfill
  requires up to five named request IDs and consumes their approved seasons in
  full. There is no episode cap or individual-episode backfill. Previously capped
  canaries expand to their approved season scope on their next successful sync;
  future episodes remain wanted without triggering a premature download.
- Anime projection may fan one upstream request out into several work requests,
  preserving exactly the selected source episode IDs. External requesters map by
  Plex ID, never display name; unknown requesters get a non-login local principal.
- Native acquisition uses Prowlarr TV categories, the shared indexer gateway,
  autonomous release research and probe-assisted file-to-episode verification.
  It retains unavailable providers as durable waits, not successful empty searches.
- The app's private Transmission uses a TV-specific ownership tag and root. It
  adds paused, checks exact metainfo/client identities and paths, retains stalled
  data, and tries another release after a stall or invalid import.
- Verified files are adopted into canonical objects. Declarative Plex symlinks
  use explicit TVDB aired mappings; set the isolated Plex library's default order
  to TVDB aired. Activation requires exact Plex episode IDs and file paths, not
  merely a successful scan or matching ordinals. Previous versions remain linked
  until replacement verification. Multi-episode files replace atomically.
- Partial packs publish only verified requested episodes and queue missing aired
  episodes separately. Completed, previously verified packs are checked before
  another indexer search: exact torrent/source provenance and fresh file evidence
  are required, and only still-missing requested episodes are adopted. Reuse does
  not add, resume or redownload the torrent. Fully accounted sources are skipped.
  An unfinished sibling pack only delays a batch when its exact owned metainfo
  indicates coverage of the whole batch and the client is making progress.
  If it stops, a scoped search can continue without stopping the sibling torrent.
  Rejections are recorded for the attempted episode IDs, not the whole show;
  stalls do not permanently rule out a release. Filename coverage is advisory
  and never bypasses file and Plex verification.
  Future requested episodes remain wanted. Routine discovery
  is event-driven via category RSS, not periodic searches per title. Feed routing
  uses the normal selector/importer and durable per-target/source receipts.
  New RSS acquisitions reserve only their assigned single-season episode batch
  (at most 64 episodes). A manual search can queue other missing episodes
  without duplicating the RSS batch. Existing unscoped jobs finish unchanged.
- Reflow owns execution/retries/checkpoints. Application transactions retain
  request scope, source evidence and outbox intent. A retry rechecks failed Plex
  observations without repeating successful source adds or canonical adoption.
- Withdrawal/intentional eviction fences acquisition. Re-request is explicit.
  This phase does not automatically delete media, remove seed copies or cross-seed.

## API And Scope

Native catalog/request contract (JSON-RPC names and typed tRPC procedures):

| Method | Access | Input | Output |
| --- | --- | --- | --- |
| `tv.search` | USER query | `{query}` | `{configured, unavailableReason, results}` |
| `tv.details` | USER query | `{tmdbId}` | `{configured, unavailableReason, series}`; series includes seasons |
| `tv.request` | USER mutation | `{tmdbId, seasonNumbers: number[]}` | `{request}` |
| `tv.myRequests` | USER query | `{}` | `{requests}`; own requests only |
| `tv.myRequest` | USER query | `{requestId}` | `{request}`; own requests only |
| `tv.withdraw` | USER mutation | `{requestId}` | `{request}`; ownership required even for administrators |

Search results contain `tmdbId`, `title`, `originalTitle`, `year`, `overview`,
and `posterUrl`. Details add `numberingScheme: "tvdb:aired"` and seasons with
`seasonNumber`, `title`, `episodeCount`, `airDate`, `requestable`, and
`unavailableReason`. Season numbers are canonical TVDB aired numbers, not assumed
equivalents of TMDB season numbers. Specials require an explicit selection of 0.
Requests require a nonempty, unique selection; there is no episode-level mutation.
Each normalized season selection is independently idempotent for its user, work
and quality policy; selecting another scope creates a separate request rather
than silently expanding or replacing an existing one. Known future episodes are
included, without an acquisition until an episode has aired. The persisted season
intent also admits newly cataloged episodes when metadata is refreshed.

The request view retains `requestId`, `targetId`, `entry`, `requestedEpisodes`,
`availableEpisodes`, `futureEpisodes`, `withdrawn`, `intentionallyAbsent`,
`acquisitions`, and `events`, and adds `seasonNumbers`. User responses do not expose
other users' requests, raw acquisition decisions or diagnostic event messages.

Discovery reuses `movies.tmdbAccessToken` (v3 key or bearer token). Canonical episode
identity comes from the existing GET-only Sonarr catalog, Sonarr's public SkyHook
full-show metadata, or an injected trusted catalog. Shows do not need to be
pre-added to Sonarr: when its local catalog cannot resolve a work, Podible reads
`https://skyhook.sonarr.tv/v1/tvdb/shows/en/{tvdbId}` without any credentials.
TMDB's explicit TVDB work reference is cross-checked against SkyHook's work and
episode ownership. Episode TVDB IDs and TVDB aired coordinates come directly
from that provider response; TMDB season/episode ordinals never establish a
mapping. Podible never adds a series, changes monitoring, submits a command,
or mutates Seerr. Missing,
ambiguous, ephemeral or conflicting identities are unsupported, not guessed from
season/episode ordinals. A discovery or mapping failure is an actionable response,
not a successful empty search. Native requests do not require Seerr.

The full-show contract is verified against Sonarr's official
[request builder](https://github.com/Sonarr/Sonarr/blob/main/src/NzbDrone.Common/Cloud/SonarrCloudRequestBuilder.cs)
and [SkyHook adapter](https://github.com/Sonarr/Sonarr/blob/main/src/NzbDrone.Core/MetadataSource/SkyHook/SkyHookProxy.cs),
which maps provider episode IDs separately from numbering. The
[series lookup response](https://github.com/Sonarr/Sonarr/blob/main/src/Sonarr.Api.V3/Series/SeriesResource.cs)
does not contain episodes, so lookup alone cannot supply canonical episode
identity. Both public v1 full-show and v2 official-order routes returned HTTP 200
for TVDB 81189 during a read-only check on 2026-11-04. This implementation uses
the stable v1 contract; unavailable or incompatible provider metadata blocks
requests visibly, rather than falling back to ordinal guesses.

`tv.status`, `list`, `get`, `sync`, `backfill`, and `retry` remain admin-only, alongside separate `tvShadow`
audit methods. No client-supplied download URLs or filesystem evidence are accepted.
See [audit details](tv-shadow.md) and [upstream contracts](tv-upstreams.md).

Integration wiring outside this change's scope: the existing JSON-RPC router
already mounts `tvRouter`, so new methods are discovered automatically. Add tRPC
query wrappers for `search`, `details`, `myRequests`, `myRequest` and mutation wrappers for `request` and
`withdraw` in `src/trpc.ts`, following the existing `tv` wrappers with each
method's `paramsSchema`, `resultSchema`, and `executeMethod`. No `shared.ts`
context field is necessary: `createTvRouter({catalog})` injects a test catalog.
The normal `tv_sync` worker refreshes native season metadata before optional Seerr
intake, and works with Seerr absent. The scheduler enqueues `tv_sync` for active
native season requests with the shared TMDB credential, independently of Sonarr
and Seerr. The comparison `tv_shadow_sync` job remains Sonarr-only. The exported
`refreshNativeTvRequests(repo)` hook rotates bounded batches of 25 requests,
retains the original policy and stable identities, logs unavailable mappings,
and admits newly announced episodes and changed air dates for feed routing.
It never restores intentionally evicted targets or withdrawn requests, and
never starts acquisition itself. Reads are GET-only with a 15-second deadline,
8 MiB response limit, eight concurrent requests, and a five-minute bounded cache.

## Production Canary Progress (2026-11-03)

The rollout uses the isolated Plex section `16` (TVDB aired order); Sonarr's main
library remains separate. The initial canaries used episode caps, which exposed
the mismatch between downloaded packs and requested coverage. That request mode
has been removed; the initial production observations were:

- Breaking Bad, Seerr request `721`: S02E01 only. The season pack completed and
  the verifier selected its matching 47-minute episode file, not the whole pack.
- Berserk, Seerr request `713`: its first episode only. The original BTN download
  was retained after a client-version rejection; autonomous recovery selected a
  different release which continued downloading. Stable Transmission subsequently
  passed a separate BTN announce check; see [runtime verification](transmission-runtime.md).
- Khom Khlang, Seerr request `726`: ten known season-one episode identities,
  including the future tenth episode. Nine episodes subsequently completed
  canonical adoption and publication. An independent, read-only Plex check confirmed
  exact episode IDs and file paths for all nine. Missing aired episodes continued through
  separate recovery acquisitions rather than marking the entire season available.

The current torrent-client API does not select individual files within a pack;
requests must therefore never be truncated to a one-episode canary while a whole
season pack is downloaded. The existing per-release byte limit still applies.
A fully downloaded pack is
not proof that any requested episode has passed import and Plex verification.
The future episode's RSS-driven acquisition remains an observation to complete
after it airs and a suitable release arrives.

### Read-Only Verification Snapshot (2026-11-04 07:36 UTC)

Automatic legacy-origin sync expanded the approved requests without a manual
backfill or request edit:

| Seerr Request | Entry | Approved Scope | Published Episodes |
| --- | --- | --- | --- |
| `721` | Breaking Bad | All 13 episodes of season 2 | 13 |
| `713` | Berserk (`anidb:98`) | All 25 episodes of season 1 | 0 |
| `726` | Khom Khlang | All 10 known episodes of season 1 | 9 |

The published counts are active selections, not torrent/file counts. Independent
read-only checks at 07:34 UTC confirmed the canonical/view file sizes and exact
Plex episode IDs and paths for all 13 Breaking Bad season-two episodes and Khom
Khlang S01E01-E09.

Breaking Bad recovery acquisition `12`, job `1959`, adopted the 12 missing files
from the original completed torrent
`8e0e788574918449df33432d50ce79f3e5a662b7`. Its journal contains 12 completed
`tv:reuse:0:adopt:*` receipts and the run completed without error. The acquisition
finished at `2026-11-04T07:09:31.946Z`. Both original and recovery acquisitions
reference the same recorded torrent, and a private-client query found exactly one Breaking
Bad torrent and no new-search steps for this recovery. All 13 are now published
and independently verified. The retained Berserk alternative is still downloading
(55.6% at 07:36 UTC); full-season canonical adoption and Plex verification
remain unobserved.

All eight configured TV category feeds last succeeded around 07:33 UTC, with
five durable anchors each and no current provider error or gap warning. The
requested Khom Khlang S01E10 has provider air time `2026-11-09T12:00:00Z` and is
not published. Future-release arrival, semantic routing, acquisition, and exact
Plex publication remain an end-to-end observation after airing, not a completed
canary or a reason to start periodic per-title searches.

The initial rollout exposed an ordering failure: both normal CD runs
[`41784606406`](https://github.com/mmgoodnow/podible/actions/runs/41784606406)
(`41a88af`) and
[`41784574337`](https://github.com/mmgoodnow/podible/actions/runs/41784574337)
(`bfa4b2d`) succeeded, but the older run's Watchtower step finished later and the
container ran the older build. The guarded normal CD run
[`41792008135`](https://github.com/mmgoodnow/podible/actions/runs/41792008135)
subsequently succeeded and the actual container now reports
`8344dbb21b12317a38a8b3e4c7199423bb905110`, a descendant of the worker commit.
Worker-gated HTTP startup completed, existing execution heartbeats and a new TV
feed cycle advanced after startup, and no startup worker error or restart loop
was observed. Application schema is 49, all 17 existing transcripts remain
successful, and Transmission reports 4.1.4. The sampled HTTP health read returned
200 in 29 ms at 07:36 UTC; this is one observation, not a production load benchmark. These
checks did not mutate production data, control torrents, trigger jobs, or deploy
manually.
