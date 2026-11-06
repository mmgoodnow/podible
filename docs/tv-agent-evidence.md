# TV Agent Evidence And Bounded Work

## Why One Piece Stopped

On November 6, production build `9228c66` had 91/1,179 requested episodes
published. Independent read-only inspection accepted all 91 regular canonical
files, exact sizes, symlinks and exact TVDB episode GUID/server paths in Plex
section 16.

Acquisition 22 / job 9522 had finished without a selected release for absolute
episodes 92-130. Its persisted search actually included BeyondHD's S07 REPACK
Kitsune pack, 53.678 GiB and 78 reported seeders. Fresh metainfo inspection showed
39 files from "Alabasta's Hero..." through "The Seventh Member Is Nico Robin."
It was removed before selection by the 40 GiB TV POC default, not absent upstream.
The next earliest-season batch then prevented later seasons from being tried.

The existing production setting `tv.maximumReleaseBytes` was changed through
authenticated `settings.update` from 40 to 512 GiB after observing 54 TiB free.
No quality policy changed, and the setting update itself queued no downloads.
It is an operator-configured admission limit, not a claim that larger packs are
wrong content. Source defaults remain unchanged; there is no One Piece exception.

## Current Division Of Work

The application, not a model, owns durable requested episode identity, season
batching, work deduplication, filesystem ownership and exact Plex verification.
Explicit request/retry intake queues all missing aired seasons, in batches of at
most 64 episodes. Active batch reservations are subtracted before creating new
work. An unsuccessful batch stops itself without blocking siblings or starting
a periodic title-search loop. Publication/consumer mutations retain target lanes;
download sleeps let other batch executions progress. Legacy durable snapshots
retain their original scope and receipts.

Sibling batches in the same explicit intake share persisted broad-search evidence
for up to 30 minutes, fenced by configuration/work identity. This avoids repeating
the same rate-limited title query per season. A later batch with no selectable
offer can wait for an actively owned sibling pack, then inspect its unused members
without another search/snatch. Waiting batches do not wait on one another.
The 130-episode concurrent-pack regression proves one search/download supplies all
three batches and their exact 130 views.

Each batch uses two separate model runs:

- **Selector:** work identity, policy and limits, four sample target episodes,
  and ten candidate summaries. Stable server-side indexes reference candidates;
  download URLs, private attributes and metainfo stay server-side.
- **Import verifier:** only that batch's target scope, selected release identity
  and ten actual playable filename/size summaries. It does not inherit search
  conversation history or receive the whole series/whole probe upfront.

The earlier context-window failure serialized the entire 1,179-episode catalog
twice. Removing that duplication was necessary but insufficient: broad searches
and repeated manifest outputs could still bloat the conversation. Evidence is
now retrieved incrementally instead.

## Tools

| Tool | Evidence returned |
| --- | --- |
| `listReleases` | Pages of known candidate summaries, optional literal title-word filter, stable indexes and explicit blockers. No upstream search. |
| `searchReleases` | Targeted alias/numbering query; first page of newly discovered candidates and known total. Later pages use `listReleases`. |
| `inspectRelease` | Paged validated filenames/sizes, total bytes and admission blockers. Metainfo is cached per candidate within the run. |
| `getRequestedEpisodes` | Paged exact target IDs, titles, runtimes and sourced alternative numbering. Never the full unrequested catalog. |
| `listFiles` | Paged actual playable filenames/sizes after download/probing. |
| `getFileProbe` / `getFileProbes` | Relevant duration and video/audio/subtitle stream facts. Bulk access avoids one-file-per-turn exhaustion; long stream lists remain pageable. |
| `assessCandidate` / `assessCandidates` | Deterministic eligibility/quality from actual probe facts plus semantic identity/source claims. |
| `assessCrossSeedability` | Safe diagnostics for a proposed semantic same-release group, not proof of byte reuse. |

Prompts explicitly encourage research before rejecting a batch: alternate
queries, real filenames versus episode titles/absolute numbering, and further
pages. A torrent's season number is evidence, not canonical episode identity.
No free-form shell, arbitrary URLs, settings writes or download controls are
available to the agents. A page has a 12,000-character item budget and an explicit
continuation offset rather than silently dropping records. The run budget is
32 turns; tool response paging reduces growth but is not a universal tokenizer
or conversation-compaction implementation.

## Filters And Limits Audit

These are different constraints and must not be conflated:

- **Visible admission limits:** configured maximum release bytes and maximum
  300 torrent files. Oversized/previously attempted offers are now visible to
  selection with blockers; final selection cannot override admission. Metainfo
  size/count is checked again before download, independent of release claims.
- **Research budgets:** eight distinct metainfo fetches, three additional upstream
  searches and 32 turns. Re-reading manifest pages does not consume a fetch.
  Pages contain at most 30 records; bulk probe/assessment tools have smaller
  explicit bounds. Cross-seed assessment is limited to 30 indexes in TV.
- **Upstream result window:** initial Torznab search asks for 100 results per
  source. Providers may return fewer/cap history. Tool pagination pages the
  retained result set, not unavailable upstream history. Targeted searches can
  recover older/more-specific releases; this is not exhaustive provider paging.
- **Execution scope:** only requested, missing, aired episodes enter a new batch.
  Future intent remains persisted. Other seasons are separate tasks, not discarded.
- **Explicit quality policy:** allowed source/resolution tiers, codecs, required
  audio language/cut and HDR constraints are checked from probed files. Unknown
  facts are not silently accepted; preferences and extra tracker coverage cannot
  establish identity or authorize a downgrade.
- **Safety/identity:** foreign client records, unsafe paths, wrong works/episode
  IDs, duplicate mappings, unrequested combined-file contents, changed source
  bytes and nonexact Plex identities/paths remain prohibited. These are not
  ranking heuristics that the model should bypass to find more results.

The request UI says that no verified match was **selected**, not that a show is
unreleased or impossible to obtain. Completed seasons are collapsed by default;
remaining seasons show their own actual queued/search/download/check/publication
stage. Raw execution events/IDs are not the normal progress interface.

## Verification Boundary

SDK fixtures exercise late-page recovery, metainfo caching, visible policy
exclusions, no secret leakage and compact probe retrieval. Real SQLite/Reflow
fixtures require two independent season downloads to start before either can
finish, verify both publications, and prove no-match starvation is gone. Browser
acceptance checks desktop/mobile coverage, authentication and withdrawal.
Production rollout/acquisition observations belong in `one-piece-canary.md`;
passing local fixtures is not a claim that the remaining episodes are available.
