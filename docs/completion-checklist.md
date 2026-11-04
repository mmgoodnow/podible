# In-Flight Completion Checklist

Scope: finish the existing TV, retention, RSS, book-transfer and performance
work. This is not the deferred subtitles, cross-seeding, unified search, or
complete Seerr-replacement backlog.

## Production Rollout And TV

- [x] Deploy SPA administration and stable Transmission 4.1.4.
- [x] Verify a successful BTN announce under the stable client.
- [x] Independently verify Breaking Bad S02E01 and nine Khom Khlang episodes in Plex.
- [x] Implement season-only requests and verified completed-pack reuse.
- [x] Implement one background Bun worker and a two-second application busy timeout.
- [x] Verify normal CD run 41792008135 serves 8344dbb with the background worker healthy, schema 49 and Transmission 4.1.4.
- [x] Verify approved season membership expands to 13 / 25 / 10 episodes.
- [x] Complete Breaking Bad season publication from its existing pack, with exact Plex IDs/paths for all 13 episodes and no new search/download.
- [ ] Complete Berserk download, import and exact Plex verification.
- [ ] Observe the actively airing Khom Khlang request through its next release.

## Performance

- [x] Reproduce intermittent production delays with authenticated Playwright CLI.
- [x] Verify private artwork caching: warm browser loads transfer zero cover bytes.
- [x] Repeat actual authenticated production PWCLI measurements after worker isolation: bootstrap 74/62/151 ms, warm covers 30/30 cached with zero transfer; 20 loopback reads 1-4 ms.
- [x] Fix reproduced session-writer stalls and the serial auth/Library waterfall; browser acceptance covers a single authenticated bootstrap.
- [x] Move stale-publication fencing and serialized side effects into shared CD; remove the app-specific workaround.

## Existing Book Torrents

- [x] Implement/test a resumable, piece-verified migration preserving raw bytes and old records.
- [x] Run production dry-run: 144 releases, 139 initially ready, five exceptions to diagnose.
- [x] Diagnose all five initial exceptions: four stale tracker messages and an XML-RPC-trimmed trailing-space directory; fixes retain full piece verification.
- [x] Re-run deployed dry-run at 8344dbb: all 144 planned, zero exceptions, no production changes.
- [ ] Back up application/execution/client state and preserve transcript fingerprints.
- [ ] Establish a confirmed quiescent maintenance window while Transmission stays available.
- [ ] Apply and reconcile the transfer, including LazyLibrarian ownership tags.
- [ ] Verify source bindings, piece checks, seeding, publication and transcript preservation.

## RSS And Upgrades

- [x] Share category intake across books, movies and TV with five checkpoint anchors.
- [x] Cover upgrades, replacement failure and crash recovery in acceptance tests.
- [ ] Capture a production Radarr/Podible RSS timing comparison using the existing Seerr canary.
- [ ] Record whether missing evidence requires a future release rather than claiming a synthetic replay proves live timing.

Production observation on 2026-11-04 UTC: existing request 4, A Dog's Perfect
Christmas (TMDB 1570488), is monitored in Radarr as movie 2346. Radarr reports
digital release 2026-11-04, no file, and an empty movie-specific history.
Podible has no matching inbox release or consideration. Neither system has
acquired this canary yet; a date alone is not evidence that a tracker release
exists. Movie, TV, audio and ebook feeds are polling successfully. Do not start
periodic per-title searches to manufacture a comparison.

## Retention And Recovery

- [x] Persist availability intent and fence reacquisition after intentional eviction.
- [x] Implement ownership/reference/seed-obligation auditing and the audit UI.
- [x] Implement/test global retained Plex history and stable movie identity across main/canary copies; live observations saw 23 accounts and exposed plays hidden by the empty canary copy.
- [x] Implement bounded disk-pressure policy and candidate ranking for owned movie content.
- [x] Implement durable, revalidated whole-resource eviction and declarative view retirement.
- [x] Preserve seed-store data, active acquisitions, protected/shared content and re-request identity.
- [x] Cover interruption, stale plans, failed effects and re-request recovery in real Reflow/SQLite acceptance tests.
- [ ] Verify production preview/no-op behavior before any destructive action.

The quiet `monitor-tv-and-rss-canaries` heartbeat follows the remaining external
release/download outcomes every 30 minutes, read-only. It must not start searches,
control torrents, deploy, or apply the pending book transfer. A maintenance-window
approval is still required for that separate one-off transfer.

Do not manually deploy, remove foreign library content, delete seed data, or
manufacture completion of downloads/releases that have not actually arrived.
