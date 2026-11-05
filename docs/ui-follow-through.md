# UI And Torrent Follow-Through

Status distinguishes inspected, implemented, verified, pushed, and deployed. A
proposal or a passing isolated test is not a shipped feature.

## Implemented And Pushed

- [x] Home All tab: content rails inspired by Badminton; outstanding movie/TV requests,
  recently added books/movies/TV, recent episodes, and discovery. Search replaces
  the rails. Recently added uses real import/publication timestamps, not record-ID
  ordering (`35d0291`). Recent episodes means recently aired and available, not an
  invented per-episode publication date.
- [x] Torrent health: one combined view across media, actual titles/links,
  problems first, unknown observations distinct, healthy records summarized (`94e193c`).
- [x] Automatic torrent cleanup: stop listing every routine protected record;
  summarize normal safety decisions, surface meaningful failures/action (`94e193c`).
- [x] Shared torrent storage: one client/hash/location/ownership record with
  domain associations. Preserve existing IDs, paths, markers, fences, evidence,
  and seed data. No file movement or rechecks (`d5ec0a2`).
- [x] Login: same-window Plex by default; remove popup choice and top strip
  (`e29698d`). Fixture Chrome checks callbacks, sessions, errors and timeouts.
- [x] Activity: remove worker execution history UI; retain backend diagnostics (`2d33068`).
- [x] Activity: explain failed TV comparisons using the show title and impact,
  not `tv shadow evaluate`. Production case 526/job 6229 is The Daily Show;
  inspection failed or returned an invalid mapping after three attempts. It is
  a read-only comparison, not an acquisition failure (`2d33068`).
- [x] Activity: say what Salt, Fat, Acid, Heat is waiting to do and which formats
  it concerns; waiting is separate from active work (`2d33068`). Journal reads
  expose bounded, known search-wait stages, not private execution payloads.
- [x] Remove excessive disclosures around Activity navigation and caveat text (`2d33068`).
- [x] Retire raw Movie sync / TV sync pages while retaining comparison data and
  backend APIs. A useful future presentation compares outcomes and timing, not
  job/case inventories; no unsupported metrics invented (`49eac88`).
- [x] Feed gaps: persist exact paging stop reason and bounded polling context;
  expose diagnostics without claiming missing anchors prove deleted releases.
  Historical causes remain explicitly unknown (`cf4af2d`).

## Final Integration

- [x] Complete route audit for healthy-item noise, primary numeric IDs,
  technical jargon, and disclosures that conceal trivial content.
  Final patch removes primary job/request numbers, replaces TV target-ID input
  with a named requested-season picker, and simplifies movie storage actions.
  Server safety checks and removal confirmations remain (`0cfe355`). Technical
  inspection fields remain available; this is not a complete visual redesign of
  every expanded admin audit.

## Already Pushed Before This Pass

- [x] Recovery hides healthy resources, separates unchecked mounts, and scopes
  its empty-state claims to the inspected page (`a86e877`).
- [x] Remove redundant Shared library captions (`61a026d`).
- [x] Provider cards consolidate saved media feeds and show problem counts
  visually (`f04c5c0`). Follow-up simplification remains above.
- [x] Fix tRPC same-origin mutations behind TLS termination (`0f7a57a`).
- [x] Cache Transmission compilation and parallelize browser CI, with fresh-runner
  SPA asset prerequisite fixed (`5e89e00`, `08ca70e`).

## Release Gates

- [x] Focused migration, ownership, retirement, feed and UI regression tests.
- [x] Final full suite: 1,346 passing; generated database types,
  typecheck, frontend build, and runtime entrypoint validation pass.
- [x] Final assembled tree checked before final commit.
- [x] All eleven fixture-backed Chrome acceptance suites and desktop/mobile
  visual inspection pass. No real Plex login or upstream-provider claim.
- [x] Final audit patch committed/pushed; deploy through normal CD only.
- [x] Normal CD run `41934689207` passed every verification/build gate; production
  independently reports running SHA `b300cbd93fe28f47bad1930bae57efc9712e9720`
  and schema 54. All 145 current book bindings have exactly the same ownership
  identity digest before/after consolidation; the legacy table is gone. All 17
  successful transcript identities/fingerprints/paths have the same digest, and
  the asset-file count remains 766. No torrents were controlled or files moved.

Browser integration caught stale selectors for new home rails and renamed
storage/torrent actions. Update against actual rendered behavior; do not skip
suites or weaken deployment gates. Desktop/mobile home and torrent-health visual
checks passed locally. These are fixture-browser results, separate from the live
build/schema/data verification above. Future changes must still distinguish a
push from a verified production rollout.

## Subsequent Pagination Feedback

- [x] Reproduce the Torrent health empty-page cause: raw binding pagination ran
  before healthy rows were hidden.
- [x] Filter and prioritize attention records before SQL pagination, retain
  unknowns separately, and show scoped inventory health totals.
- [x] Test later-page problems, all-healthy inventories, invalid ownership/index
  evidence, unchanged historical observations and migration replay.
- [x] Keep routine raw retirement paging inside its existing technical preview.
- [ ] Finish normal CD verification for the schema-55 pagination follow-up; the
  independently verified `b300cbd` rollout above predates this fix.

The full deduplicated history and broader product scope are tracked in
[feedback-and-roadmap.md](feedback-and-roadmap.md), including partial, optional,
superseded and deferred requests.
