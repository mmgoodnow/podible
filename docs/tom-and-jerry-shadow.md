# Tom and Jerry: Seerr request 734

## Diagnosis

Read-only production metadata inspection on November 6, 2026, running build
`9228c66332ba87acbbfdf902921d5c6fa003e293`:

- Request: Seerr 734, TMDB 340782, TVDB 72860, Sonarr series association 834.
- Approved Seerr/TMDB seasons: 1, 2, 3.
- Main's independent Sonarr inspection: 177 episodes, grouped as season 0 (16),
  1940 (46), 1950 (68), and 1960 (47).
- GET-only Seerr season metadata returns 114, 13, and 34 episodes respectively.
  All three season `externalIds.tvdbId` values are null.
- Seerr's season response exposes TMDB episode IDs, but not episode-level TVDB
  IDs. Its source `mapEpisodeResult` omits episode external IDs.
- Direct authenticated TMDB episode external-ID GETs also return null TVDB IDs
  for S01E01 (TMDB episode 8051051), S02E01 (8051211), and S03E01 (8051224).

This is a real provider numbering disagreement, not missing releases. TMDB's
three season groups do not share Sonarr's decade-based coordinates. The sampled
external-ID gaps prevent a complete explicit identity bridge. We did not query
every episode or assume all other episode external IDs are absent.

No matching by array position, title, air date, equal episode totals, or season
ordinal was used. No production request, torrent, settings, or database data was
modified. The approval has **not** been translated or acquired by this fix.

## Bounded Fix

`syncTvRequests` now defers the entire approval when any approved season is absent
from the canonical TVDB aired coordinates. It must not silently import only the
matching subset of a mixed approval.

Before advancing the feed cursor, it durably retains the unresolved approval in
`app_state` under `tv-seerr-season-mapping:<Seerr instance fingerprint>`.
`tvSeerrMappingIssues(repo, instance)` exposes typed per-show diagnostics:

- Request ID and title, TMDB/TVDB work IDs.
- Approved and available canonical season numbers.
- An explicit mapping explanation and first/last-attempt timestamps.

Activity now shows an individual Tom and Jerry item with the two season-number
sets and an "Episode mapping needed" status. It does not label the whole TV sync
stopped or offer a download retry that cannot fix numbering. Successful unrelated
syncs do not hide unresolved approval; removal of the persisted issue clears the
card. Unit tests exercise the actual Ops response and Chrome checks its rendering.

Healthy requests continue, and the feed cursor can advance without losing the
unresolved request. Later syncs re-read up to eight deferred request metadata
records outside the normal feed window, oldest-attempt first. These are metadata
GETs, **not** periodic release/title searches. A failed deferred Seerr detail read
retains the issue and does not block healthy requests. Revoked approval removes
the deferred issue; successful normal ingestion removes it only after committing
the native origin and its requested coverage.

Existing configuration/identity errors, database failures, and shrinkage of an
already imported episode scope keep their existing fail-closed behavior. This is
not a general exception-swallowing framework or a new provider mapping system.

## Regression Evidence

`test/integration/tv-sync.test.ts` covers:

- The exact ordinal/year mismatch, healthy sibling ingestion, and cursor progress.
- Durable metadata retry after database reopen and eventual issue removal only
  when the supplied provider catalog becomes compatible.
- Rejection of partial approved-season truncation.
- Deferred detail-read failure without approval loss or sibling starvation.
- Eight-read retry bounds and oldest-approval fairness.
- Mapping-intent persistence failure preventing cursor advancement.

The corrected-provider fixture is an explicit upstream input change for testing
the retry lifecycle. It does not demonstrate a real Tom and Jerry identity bridge.
