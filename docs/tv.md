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

An episode has stable provider references. Multiple works may reference an
episode, with entry-scoped numbering. Explicit AniDB/TVDB mappings and AniList
relationships can split an aggregate into logical works; unknown mappings retain
the original provider entry with warnings. See [anime metadata](tv-anime.md).

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
    "maximumReleaseBytes": 42949672960,
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
  requires up to five named request IDs and an episode cap (UI default: one).
  Capped selections retain episode identities across later catalog reordering.
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
  episodes separately. Future requested episodes remain wanted. Routine discovery
  is event-driven via category RSS, not periodic searches per title. Feed routing
  uses the normal selector/importer and durable per-target/source receipts.
- Reflow owns execution/retries/checkpoints. Application transactions retain
  request scope, source evidence and outbox intent. A retry rechecks failed Plex
  observations without repeating successful source adds or canonical adoption.
- Withdrawal/intentional eviction fences acquisition. Re-request is explicit.
  This phase does not automatically delete media, remove seed copies or cross-seed.

## API And Scope

JSON-RPC and typed tRPC expose admin `tv.status`, `list`, `get`, `sync`,
`backfill`, and `retry`, plus separate `tvShadow` audit methods. No client-supplied
download URLs or filesystem evidence are accepted. See [audit details](tv-shadow.md)
and [upstream contracts](tv-upstreams.md).

TV catalog discovery and user-facing native TV request creation are not yet
implemented. The unified books/movies/TV omnibar is a separate product step; it
must not disguise unsupported TV discovery as an empty result set.
