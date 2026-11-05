# Consolidated Feedback And Roadmap

Reconciled November 4, 2026 from this conversation, the current checkout, and
the dated verification records linked below. Repeated requests/screenshots are
consolidated; later decisions supersede earlier experiments. Questions that did
not request a change are captured where they establish a product constraint.
Messages explicitly withdrawn as being in the wrong chat are excluded.

## Reading The Status

- **Addressed**: implementation or the requested decision exists. This does not
  establish perfect quality or fresh production verification of every old case.
- **Partial**: a useful slice exists, with an explicit remaining boundary.
- **Open**: requested but not implemented or verified complete.
- **Deferred / paused**: deliberately outside current scope, not forgotten.
- **Superseded**: replaced by a later user decision, not an outstanding task.
- **Historical verification needed**: a specific old repair/experiment was
  discussed, but the current audit did not independently establish its outcome.

Latest independently verified application rollout: `b300cbd`, schema 54, through
normal CD. The later `d1ff98d` commit records that evidence. That verification
preserved all 145 current book torrent bindings, 17 successful transcript
identities, and 766 asset-file records. Local acceptance passed 1,346 tests and
all eleven Chrome suites. These are baseline facts, not claims about changes
made after that rollout.

## UI Feedback

| ID | Your feedback / desired outcome | Status and remaining work |
| --- | --- | --- |
| U01 | Remove oversized marketing headers, slogans, ornamental numbering and filler. | **Addressed.** Includes "A good story awaits", "Follow your curiosity", "From wanted to watching", "A place on your shelf", "Your personal edition", "Press play. Turn a page", and "Between the lines". Preserve content, not marketing chrome. |
| U02 | Rewrite books in the SPA, make it the root, remove `/app` navigation leftovers and classic screens. | **Addressed.** Root-level SPA; legacy app aliases redirect. Classic forms/pages removed; API, auth callbacks, feeds and opaque media URLs remain. |
| U03 | Bring Settings, Users, Ops and login over; remove DB/curation pages and the wipe-database button. | **Addressed.** Administration is SPA-based; destructive database-reset UI and classic diagnostic pages retired. |
| U04 | Settings must remain discoverable. | **Addressed.** Settings is primary admin navigation, not buried behind `/app` or classic links. |
| U05 | Too many inscrutable destinations: Movie sync, TV sync, Retention, Recovery, Torrent health. Better interaction design, not explanatory paragraphs. | **Addressed in the primary navigation; partial deeper redesign.** Library, Activity and Settings are primary. Contextual drill-ins remain; expanded storage audits still contain technical material. |
| U06 | Remove worker execution-history inventories rather than hiding them in another disclosure. | **Addressed.** Activity no longer lists every successful worker job. Backend diagnostics remain available. |
| U07 | Movie/TV comparison pages probably need not exist; a genuinely useful presentation might compare outcomes. | **Raw pages addressed; outcome dashboard optional/uncommitted.** Legacy routes redirect to Activity; comparison APIs and evidence remain. No invented timing/winner metrics. |
| U08 | Show content: beautiful movie/TV posters and cards like books/Seerr, not giant text rows. | **Addressed.** Artwork-led grids, persisted TV artwork, fallback art handling, progress/availability on cards. Further visual refinement remains possible. |
| U09 | Combine Library/Discover and Movies/Requests; one local-first search across books, movies and TV, remote results arriving independently. | **Addressed.** Unified Library, media filters and independently loading catalog results; pending private requests stay actor-scoped. |
| U10 | All/Home should have Badminton-style rails: requests, recently added media, recent episodes and buzzy things to request. | **Addressed with limits.** Rails use real import/publication dates and cached TMDB trending. Recent episodes means recently aired and currently available. Pending book requests and a distinct recently-downloaded-episode rail are not supplied. |
| U11 | What happened to the rails? Do not drop the request. | **Addressed.** Rails shipped in `35d0291`, verified in the `b300cbd` rollout. Search/media-specific grids remain. |
| U12 | "Follow movie" is confusing. | **Addressed.** Available titles offer "Add to my library"; unavailable titles offer "Request movie". Adding existing content does not cause a duplicate download. |
| U13 | "Shared library" is meaningless/redundant when the card already says Ready in Plex. | **Addressed.** Redundant captions removed; ownership/request distinctions remain internal where appropriate. |
| U14 | A normal release wait should not say Blocked/Needs attention; distinguish not-yet-released from not-found. | **Addressed with evidence limits.** Pending release/future episode states differ from failures. A release date is not proof that a tracker release exists. |
| U15 | SFAH "Scheduled wake" does not tell me what is happening. | **Addressed.** Waiting is separate from active work; known journal stages identify the format/search retry. Already imported formats do not appear as unfinished work. |
| U16 | "tv shadow evaluate" and a job number explain nothing. | **Addressed.** The affected show and read-only comparison impact are shown. Inspected old case was The Daily Show, not a failed download. |
| U17 | Retry failed with "Unable to transform response". | **Addressed.** Same-origin tRPC mutations work behind TLS termination; rejections retain the proper error envelope. HTTPS proxy acceptance covers it. |
| U18 | Library fails because omitted cursor becomes null. | **Addressed.** Omitted pagination cursor is preserved; filtering/pagination/bootstrap regressions covered. |
| U19 | Too many `<details>` tags, including trivial explanations/navigation. | **Addressed in touched routes.** Consequential actions/status remain visible; specialized diagnostics can still be disclosed. Not a ban on all disclosures. |
| U20 | Healthy things should not dominate problem screens; book/job IDs are meaningless. | **Addressed.** Torrent problems use titles/links; healthy/protected rows summarized; primary job/request numbers removed; TV retention uses named requested seasons. |
| U21 | Recovery should show missing media, not healthy titles or "no retained desired presence" for everything. | **Addressed.** Known healthy resources hidden; uncertain mounts remain distinct. Missing bytes do not silently create new demand. |
| U22 | Torrent health has empty pages because all 25 healthy rows are hidden. | **In progress.** Current raw-binding pagination precedes UI filtering. Fix must paginate relevant nonhealthy records, preserve unknowns, and avoid client-side crawling of every raw page. |
| U23 | Connections need a visual summary, not long repeated text per media type. | **Addressed.** Provider cards aggregate saved feed observations with visual issue counts. Media-specific feed diagnostics remain available. |
| U24 | Availability may not be segmented by media type in practice. | **Addressed as a modeling/display distinction.** Provider cooldowns can be shared; a feed's saved error/coverage still belongs to that feed. One success does not erase another feed's evidence. |
| U25 | Login has a weird top strip; always continue in this window. | **Addressed.** Same-window Plex flow, no popup fallback choice, corrected background layout; mobile and callback/session acceptance covered. |
| U26 | TMDB logo is broken; Search cursor looks like a blue beachball. | **Addressed.** Local attribution asset and normal pending-search cursor, covered by browser acceptance. |
| U27 | Book says ebook ready but download button appears only after refresh; PDF must work too. | **Addressed.** Downloadable edition read model and refetch/publication behavior; real PDF download tested. Ready status alone must not substitute for a usable returned download. |
| U28 | Open Library cover picker overlaps everywhere. | **Historical verification needed.** Current SPA has artwork selection and browser coverage; this audit did not reproduce the original Hyperion grid at the exact reported viewport. |
| U29 | Cover updates should be visible without hard refresh. | **Addressed.** Revisioned artwork URLs; wait for refreshed artwork before reporting selection success. |
| U30 | Prod page loads/covers feel slower; use authenticated Playwright CLI, not just API timings. | **Addressed for reproduced causes.** Worker isolation, session-write and bootstrap fixes; dated real browser measurements in performance.md. Not a promise that all future latency is solved. |
| U31 | Make the API/interface map command-clickable. | **Addressed.** TypeScript API map alongside explanatory documentation. It is a map, not a second implementation. |

## Books, Manifestations And Metadata

| ID | Your feedback / desired outcome | Status and remaining work |
| --- | --- | --- |
| B01 | Multiple editions coexist; a multipart edition contains ordered containers, not unrelated single picks. Clean up obsolete selection paths. | **Addressed in the main model.** Book -> manifestations -> ordered assets/files. Do not claim every legacy helper has been deleted; compatibility media endpoints intentionally remain. |
| B02 | Red Rising should acquire both parts; multipart streaming needs real concatenation/remuxing. | **Addressed implementation; historical cases need care.** Ordered acquisition and manifestation streaming/remux exist. No fresh paid reacquisition or playback of old manifestation 59 was performed for this ledger. |
| B03 | Clients must not construct asset URLs. Expose opaque stream/chapter/transcript/download URLs through RPC in the correct shape. | **Addressed.** Playback options/selected editions carry server-returned URLs. Multi-container transcripts are manifestation-shaped rather than a misleading single-container shortcut. |
| B04 | Report issue should be available to users, scoped to the edition, and escalate reimport -> reacquire. | **Addressed.** Edition-specific user flow; existing good editions are preserved. Admin-only diagnostic/import controls remain separate. |
| B05 | Report issue apparently deleted 57's asset, created 58, then renamed 57. Validate and fix the identity bug, not just the symptom. | **Prevention implemented; exact historical repair not reverified here.** Stable selection/import publication identity and non-destructive issue flows exist. Do not relabel the old database state as a newly reproduced bug. |
| B06 | Clean up empty manifestations, but do not destroy intentional/in-flight edition identity. | **Addressed with guards.** Prune only empty, unreferenced manifestations; active/referenced records may legitimately remain. |
| B07 | Remove accidental low-level manifestation auto-creation; even a single manual release needs explicit manifestation identity. | **Addressed for uniform manual creation and publication.** Stable identity is chosen above import; `forceManifestation` is used by user manual selections. Not a claim that all legitimate scanner/default-edition creation disappeared. |
| B08 | Only one actual file can imply a constrained import; an arbitrary multi-file set can be arranged many ways. | **Addressed.** Reimport short-circuits only when there are zero/one importable files, not merely one detected multi-file set; multiple files remain eligible for alternative agent selection. |
| B09 | Admin should see the manifestation/container/source/file tree. | **Addressed.** Book diagnostics in SPA; paths/internal provenance remain admin-only. |
| B10 | Manual search/snatch must be user-level for audio and ebook, preserving selected release order. | **Addressed.** `library.searchReleases` and `library.createManifestationFromSearch`, plus book management UI. |
| B11 | Do not let users submit arbitrary torrent URLs; avoid expensive repeat Torznab validation. SQLite cache is fine. | **Addressed.** Expiring actor/book/media-scoped stored result sets; create uses search ID and ordered indexes. Raw download URLs stay server-side. |
| B12 | Persist agent reasoning on domain objects, not only a trace table; label/note alone is not enough. | **Addressed.** Manifestation selection notes, asset import notes and screen acquisition/consideration explanations. Logs remain supplementary. |
| B13 | Cover is work-level decoration; choose the prettiest cover, not necessarily GraphicAudio branding. Duration/word count can be useful book rollups. | **Accepted design.** Book artwork and size estimates remain useful projections. Playback/transcription identity belongs to the selected manifestation. |
| B14 | Transcription controls are manifestation-level, not an ambiguous book-level action. | **Addressed.** Per-edition generation/status/force controls and playback audio options. |
| B15 | Cover changes should be user-level, not admin-only. | **Addressed.** Artwork selection uses authenticated user handlers. |
| B16 | Generate missing artwork with rich book context; make AI generation visible with a watermark. | **Addressed.** Description/title/author/context prompt, durable generation/watermark/publication, and legacy-watermark support. A concurrent user-selected cover is preserved. |
| B17 | Refresh Open Library descriptions/metadata and support a full library refresh; publish operational scripts in the image. | **Addressed.** Metadata/full-refresh operations and runtime-script packaging validation. This is not a newly requested periodic metadata-refresh schedule. |
| B18 | Series membership on book detail, click through to books; search can supply members if series objects do not. | **Addressed.** Series RPC/local+catalog lookup and SPA navigation. Upstream completeness is not guaranteed. |
| B19 | Edition-derived series caused poisoning; title regexes are weak. Accept corroborated editions, retain position separately. | **Addressed conservatively.** Multi-edition agreement and conflicting-position suppression. Correct real memberships can still be absent when evidence is insufficient. |
| B20 | Book language is polluted by arbitrary Open Library editions; fix more than Wool. Import agent should identify actual language. | **Partial.** Manifestation/release language feeds transcription instead of trusting book metadata; unknown defaults to English. General non-English behavior and historical metadata cleanup remain unproven. |
| B21 | Emily Wilson's Odyssey / translation identity should not be bolted on. | **Open design.** Distinguish work, translation/expression, translator/language, and concrete audio/ebook edition; preserve seamless default selection. No completed multilingual acquisition UX. |
| B22 | Insulate users from duplicate/misgrouped works and poor Open Library metadata. | **Partial.** Conservative hydration, language/series handling and independent errors exist. General catalog reconciliation/provider fallback/translation-aware dedupe remain open. |
| B23 | Foundation search returned nothing because "Isaac" contained "aac"; agents never saw filtered candidates. Avoid sequel regexes and brittle prefilters. | **Addressed direction and candidate flow.** Semantic selection/RSS routing must see broad uncertain candidates, rather than only deterministic survivors. Catalog search correctness is not universally proved. |
| B24 | Agents are cheap; prefer them for ambiguity instead of hand-coded exceptions; hardcode the shared text model. | **Addressed.** Current text-agent constant is `gpt-6-luna`; no general per-agent model settings surface. This does not mean transcription/image models use the text model. |
| B25 | Initial request "Luna 6.1" was clarified to GPT-6 Luna. | **Superseded by clarification.** Do not restore a nonexistent/unconfirmed `gpt-6.1-luna` choice. |

## Chapter Quality And Transcription

| ID | Your feedback / desired outcome | Status and remaining work |
| --- | --- | --- |
| C01 | Chapters are unreliable/off-by-one, drift or swallow several chapters; opening credits must not shift narrative labels. | **Partial.** Agent/judge pipeline, edition scoping and explicit estimates exist. Fresh corpus-wide correctness and affected production-book freshness are not established by this audit. |
| C02 | Build a reusable real-book harness; raw audio/transcript/EPUB are inputs, not Podible's derived chapter JSON. | **Addressed.** Local corpus/harness and lightweight answer-key convention; heavy artifacts live under ignored `tmp/chapter-cases`, not disposable `/private/tmp`. |
| C03 | Expand corpus to 16 and personally/manual-audit expected chapters before optimizing. | **Artifacts present; quality verification open.** Sixteen local answer-key files currently exist. File count is not proof every key is correct or that all cases were freshly rerun. |
| C04 | Include multi-MP3, absent/wrong/equal-division markers, drift, nested structure and diverse books. | **Addressed in corpus design.** Named cases include Red Rising, ADSOM, Hyperion, Freakonomics, Project Hail Mary and later nonfiction/fiction additions. Coverage still has a long tail. |
| C05 | Autoresearch: small hypotheses, test, accept/revert; report failures rather than deck-chair rearrangement. | **Partially evidenced historically.** Harness/comparison artifacts exist; do not invent a fresh aggregate score or claim every past experiment followed the protocol perfectly. |
| C06 | Remove book-specific hacks, including Hyperion "THE TALE"; generic inline all-caps is okay. Audible/production boilerplate detection is okay. | **Accepted policy; not a new zero-special-case audit.** Prefer semantic structure and generic extractors; chapter quality is not defined by maximizing one book's score. |
| C07 | Epilogues, narrated part dividers and meaningful nested subchapters should be allowed. Non-narrated copyright/TOC material should not become chapters. | **Addressed as classifier/curation policy; accuracy remains partial.** Need audible evidence, not an anime/book/title-specific rule. |
| C08 | Do not blindly trust embedded markers or poison the agent with the deterministic final plan. | **Addressed in agent tooling.** Embedded timings are evidence with diagnostics; agent inspects EPUB/transcript. Deterministic candidate hints still exist and require judging. |
| C09 | Use Agents SDK, safe ripgrep, EPUB structure, fuzzy search, estimated location and transcript windows; no arbitrary shell. | **Addressed.** Chapter-specific tool surface and constrained runner. Older tool-submission shapes were superseded by subsequent algorithms. |
| C10 | Submission must be a tool: validate, return feedback, continue; future judge can sit inside validation. | **Addressed.** Proposal/validation/judge loop, not merely a structured final answer with no revision path. |
| C11 | Raise turn budget, parallelize, log incrementally and provide readable agent transcripts, not only a giant final JSON. | **Addressed infrastructure.** Bounded parallel node work and incremental debug/event artifacts; reasoning summaries only when the model supplies them, not guaranteed private thinking. Runtime/cost still need monitoring. |
| C12 | Try agent-directed recursive fulcrums; fuzzy alignment alone produced false positives. Validate correct prose on both sides and induce research. | **Superseded as the primary algorithm.** Recursive experiment informed evidence/judge requirements; current path is node-parallel rather than recursive partitioning. Residual internal names are not proof recursion is active. |
| C13 | Instead use N chapter agents in parallel plus a before-pass for audible nodes. | **Addressed.** Node-parallel curation after audible-structure selection. Concurrency/budgets remain bounded. |
| C14 | Preserve judge acceptance, including deterministic shortcuts. Judge surrounding prose, not random title occurrences. Wider windows should be an explicit tool for bad ASR, not only short headings. | **Addressed mechanism; reliability not closed.** Candidate/judge code uses contextual evidence and wider lookup tools. No claim judges cannot still be wrong. |
| C15 | Let LLMs resolve ASR variants instead of building another fuzzy matcher with an LLM rubber stamp; quality over LLM-free operation. | **Accepted requirement.** Deterministic checks serve guardrails/candidate search, not the product goal. Needs continued failure-case evaluation. |
| C16 | For genuinely missing transcript data, estimate from word count/EPUB position instead of losing chapters. | **Addressed.** Explicit EPUB-position estimate fallback, distinct from a judged aligned boundary. Estimates are not promoted to confirmed matches. |
| C17 | Mark estimated chapters with a structured string union, not bool; distinguish curated, estimated and raw. No needless backward-compat scaffolding. | **Partial.** Served chapter JSON carries `source: curated | epub_position_estimate`, absent for raw. However `book.chapters` drops provenance in its response schema and the SPA does not display it. Propagate the existing field through RPC/UI rather than inventing another representation. |
| C18 | Force regeneration via a force parameter, not automatic algorithm-version cache busting. Preserve expensive successful transcripts. | **Partial.** Manifestation-scoped force controls and artifact preservation exist, but transcript fingerprints still include algorithm/model identity. A newly run job can invalidate reuse after those constants change; force is not currently the sole invalidation mechanism. No bulk regeneration is authorized by this ledger. |
| C19 | Failed chunks should retry individually; add 429 backoff; increase OpenAI parallelism and consider transcoding limits. | **Addressed infrastructure.** Paid chunks checkpoint independently; bounded transcription/transcoding concurrency and retry/backoff exist. Quota exhaustion is not fixed by more retries. |
| C20 | Fix only the bad-language transcripts in prod; do not invalidate all successful caches. | **Targeted mechanism addressed; specific past jobs need historical verification.** Alchemised, ACoL and other retranscriptions are not newly re-requested by this inventory. |
| C21 | ACoL answer key should preserve narrated Epigraph then I, not force opening material into chapter I. | **Recorded semantic correction.** Current local key exists; this audit did not listen/re-adjudicate that passage. |
| C22 | Classify unresolved nodes as bad transcript, ambiguous structure, weak evidence or budget exhausted; explain how classification is known. | **Partial.** Diagnostics classify failures, but labels are model/evidence assessments, not independent proof of cause. Keep audit context and uncertainty. |
| C23 | Compare performance/reliability/accuracy to historical recursive runs; use existing runs. | **Historical reports exist; no fresh conclusion here.** Reconcile answer keys and historical comparisons before advertising new aggregate quality numbers. |
| C24 | Evaluate GPT Transcribe vs Whisper on several real 20-minute excerpts, WER with/without glossary, sound effects/formatting normalization, 1x/2x speed and cost. | **Experiment implemented/completed historically.** Isolated scripts/results; EPUB-provisional WER is not audio-verified ground truth. Glossary extractor was noisy. No new paid evaluation in this audit. |
| C25 | Consider alternatives to OpenAI because timestamps matter. Do not migrate production just for a lower text WER. | **Provider decision paused/open.** Runtime remains `whisper-1`; replacement needs usable timing and explicit successful-transcript reuse. External-provider research is not an integration. |

## Execution, Storage, Performance And Delivery

| ID | Your feedback / desired outcome | Status and remaining work |
| --- | --- | --- |
| A01 | Stop embedding execution machinery in domain statuses and bespoke checkpoints. Acceptance tests first, then full refactor and prod verification. | **Addressed.** Reflow checkpoints/leases/retries, domain intent + transactional outbox, real SQLite/subprocess crash acceptance and dated production migration evidence. |
| A02 | Prefer generic execution persistence plus domain code; explain retained application jobs concretely. | **Addressed decision.** `jobs` retains stable identity/input/policy/domain references; journal owns lifecycle. It is not a second execution state machine. |
| A03 | Remove the remaining jobs facade entirely? | **Optional/open, not promised.** Direct run references plus a small outbox remain a possible simplification, not required for the completed Reflow consolidation. |
| A04 | Single database is appealing, but Reflow owns global user_version; keep separate schema ownership. | **Accepted.** Application and execution databases remain separate; coordinated backups are required. |
| A05 | Kysely may help; do not force the whole domain model into generic workflow tables. | **Partially adopted.** Generated application DB types and typed SQL in newer shared modules; no repository-wide migration promised. |
| A06 | Bun wherever possible, including build/typechecking; isolate async worker work from HTTP. | **Addressed.** Bun build/check, one background Worker thread, main-thread HTTP. Not a process pool or distributed-worker design. |
| A07 | SQLite WAL, short transactions and a 2-5 second busy timeout should be sufficient for one server. | **Addressed at 2 seconds.** WAL and bounded writer scopes; separate thread/connection, not sharing a JS SQLite handle unsafely. Contention can still occur. |
| A08 | Native iPhone and browsers should stay logged in without arbitrary monthly expiry. | **Addressed long-lived-session design.** Renewed browser cookie and revocable hashed-token sessions; expired/revoked tokens still fail. Not a promise of unrevocable login forever. |
| A09 | Cache Transmission compilation; explain the test image; parallelize browser suites; remove unnecessary migration-per-push cost. | **Addressed.** Cached runtime stages, real-daemon image gate, three browser groups, legacy migration lab manual opt-in. Normal production image is still independently built/published. |
| A10 | Audit tests that only prove mocks work, rather than deleting useful fault injection. | **Partial.** Evidence audit in ci.md; obsolete UI tests replaced with real API acceptance. Generic all-job SIGKILL deduplication remains a candidate, not silently removed coverage. |
| A11 | Deploy via normal CD, fix Watchtower/CD properly, do not hotpatch prod. | **Addressed convention and fixes.** Watchtower API and shared stale-publication fencing fixed. Running SHA, not CI success/image label alone, is deployment evidence. |
| A12 | Existing rTorrent books, including LazyLibrarian, should move to private Transmission; one-off maintenance is okay. | **Completed production one-off.** 144 transferred, including 24 LL, zero downloaded bytes; later current inventory has 145 book bindings. Do not repeat the transfer. |
| A13 | Books can use lasting canonical storage and declarative symlink views too. | **Addressed.** No archive-unpacking stage; canonical/view reconciliation preserves identity and transcript caches. One-off import migration is distinct from ongoing layout policy. |
| A14 | Rechecks are expensive at scale; future movement should not routinely recheck all bytes. | **Partial/open.** Intact proven seed reuse and replay avoid unnecessary checks. A general trusted relocation/client-state transfer protocol without rechecks is not complete. |
| A15 | Use subagents effectively, maintain todo lists, commit/push small changes, and do not lose back-to-back feedback. | **Addressed process; ongoing obligation.** Separate owners, parent integration, this ledger plus completion/UI checklists. A checked box requires evidence, not delegation alone. |
| A16 | Kill the scheduled canary monitor. | **Recorded completed in completion-checklist.md; not freshly inspected here.** No Codex monitoring automation was recreated; application-owned RSS/schedulers remain. |

## Broader Superapp Product Plan

| ID | Desired capability / constraint | Status and next boundary |
| --- | --- | --- |
| P01 | One app can eventually replace Sonarr/Radarr/Seerr functions, not just wrap their UIs. | **In progress.** Independent movie/TV acquisition, native requests and library publication exist. Full replacement is not complete. |
| P02 | Movie POC with quality scoring and automatic upgrades designed from the start. | **Implemented; live confidence partial.** Crash/stall/replacement/publication tests exist. Genuine production quality-upgrade evidence is still missing. |
| P03 | Docker-bundled private Transmission; torrent internals should not be the normal user interface. Keep client abstraction. | **Addressed.** Stable 4.1.4, private daemon, shared client contract. Single-binary extraction idea superseded by Docker. |
| P04 | Plex-facing symlinks into managed content; linking declarative and reconciled, not one-off migrations every code change. | **Addressed.** Owned canonical content, declarative views and exact publication verification. Foreign content is not adopted/deleted casually. |
| P05 | Run the stack locally for realistic tests, including torrent client/probing/Plex behavior. | **Partial evidence layers.** Real daemon lab, actual files/SQLite/HTTP/browser tests; synthetic catalog/Plex seams are not a full local real-Plex-stack or production playback claim. |
| P06 | Shadow should independently acquire new Seerr requests, not merely read Radarr decisions. Backfill a few existing titles. | **Addressed.** Independent dual-write leg and read-only Radarr/Sonarr comparisons; isolated Plex libraries. No automatic duplicate historical backfill. |
| P07 | Agents should resolve ambiguous imports automatically, not create a manual approval queue. Use ffprobe/mediainfo-like facts. | **Partially addressed.** Semantic selection/import plus probe evidence and bounded reacquisition; unsupported identity/mapping/safety cases can still block. Never equate unverified import with success. |
| P08 | Discover suitable enabled indexers/categories dynamically through Prowlarr; keep temporary outages retryable. | **Addressed.** Shared gateway/discovery/cooldowns, persisted failure isolation and event-driven retries. Do not mistake Prowlarr's 429 for a proven rate limit. |
| P09 | No periodic per-title searches. Use RSS plus initial/manual/real-failure searches. | **Addressed.** Shared durable inbox, per-target consideration receipts and bounded feed polling across books/movies/TV. |
| P10 | RSS should survive deleted anchors, page backward, and explain gaps: volume, downtime, or limited history? | **Partial.** Five anchors, up to ten 100-item pages, durable stop diagnostics. Old causes remain unknown; historic gaps are not automatically exhaustively backfilled or converted to title searches. |
| P11 | Prove RSS with an upcoming real release requested through Seerr, compare Radarr and Podible. | **Completed dated production canary.** A Dog's Perfect Christmas acquired through live feed, exact Plex verification, Radarr comparison in movie-rss-canary.md. Not a controlled speed benchmark. |
| P12 | TV shadowing soon, including at least one actively airing show. | **Implemented; final arrival observation open.** Breaking Bad 13/13 and Berserk 25/25 verified. Khom Khlang 9 episodes verified; next real release remains unobserved in the recorded evidence. |
| P13 | Season-only requests; a season pack must not publish only the single initially requested episode. | **Addressed.** Explicit season intent/episode membership and completed-pack reuse; preserve exact identities and requested coverage. |
| P14 | Model connected works, cours, specials and alternative numbering without forcing everything into Western seasons; not inherently an anime distinction. | **Partial.** General work/episode/numbering/relations model plus sourced anime sidecar. Broad live mapping coverage and related-work UX remain unproven; do not classify by animation alone. |
| P15 | Domino's-style request progress with natural drill-in. | **Addressed base flow.** Request/search/download/verify/Plex status and contextual actions. Progress must not invent telemetry for unknown multipart members. Notifications are separate unfinished work. |
| P16 | Stalled downloads and failed imports should trigger alternatives; someday shared-byte tracker rescue. | **Acquisition recovery implemented; rescue deferred.** Existing version remains available during a failed upgrade. Cross-seed rescue is not implemented. |
| P17 | Disk pressure should evict low-use media from Plex watch totals, preferably without Tautulli; protect new requests and seed obligations. | **Partial, functioning narrow policy.** Movie and verified single-season TV eviction, global retained Plex history, pins, active-playback/seed/reference fences. Seed-store and multi-season eviction remain unsupported. |
| P18 | Deleted/evicted titles must be requestable again; don't repeat Seerr's stuck deleted state. | **Addressed within native lifecycle.** Retained demand vs current presence, intentional-absence fencing and re-request recovery. Not all external Seerr workflows are replaced. |
| P19 | Make recovery after losing/replacing an array easy; retain wanted intent and reuse surviving bytes. | **Partial.** Explicit bounded recovery, additive link repair, selected-byte loss invalidation/outbox and source reuse. Full lost-array orchestration, bulk planning and every reselection case remain open. |
| P20 | Keep the library "live": seed as widely as possible, replace nuked releases, remove truly unregistered torrent records safely. | **Partial.** Continuous observations and narrowly confirmed orphan record-only retirement. Library-bound retirement, nuke-driven replacement and coverage optimization remain open. |
| P21 | Four healthy trackers and one unavailable is generally fine; don't treat every coverage imperfection as urgent. | **Accepted policy.** Mixed success is not record-wide unregistration. Maintenance should prioritize useful availability/coverage, not chase perfect tracker counts. |
| P22 | Conservative minimum seeding, tentatively 30 days; unregistered-before-30-days must not be impossible to retire. | **Addressed scoped exception.** Actual seeding, not age, protects live/unknown obligations; proven all-tracker-unregistered unused record-only retirement does not wait for an impossible seed counter. No seed-byte deletion authority. |
| P23 | Torrent health/ownership shouldn't have separate book vs movie/TV persistence. | **Addressed.** Shared registry per client/native identity, domain associations separately; current migration verified without file moves/rechecks. Internal import/publication differences remain legitimate. |
| P24 | rtgc-style scalable administration without exposing raw torrent internals as the product. | **Partial.** Problem-first titled health view and bounded collection. Bulk exception actions/coverage management beyond orphan retirement remain open. Pagination bug U22 is part of this usability gap. |
| P25 | Cross-seeding discovery, matching, shared-byte rescue and tracker coverage. | **Explicitly deferred.** Keep architectural room; not part of the current POC completion claim. |
| P26 | Torrent-specific derived symlink layouts, with tracker-specific small sidecars and slightly incompatible torrents. | **Explicitly deferred.** Canonical/view separation leaves room; no materialization/collision/verification policy implemented for this feature. |
| P27 | Subtitles might be useful, but Plex on-demand and embedded WEB-DL subtitles make them lower priority. | **Deprioritized, not canceled.** No complete Bazarr replacement; alignment/embedded-preservation need design when resumed. |
| P28 | Complete Seerr replacement: approvals, quotas, request administration and notifications. | **Open beyond native base requests.** Independent acquisition and withdrawals are not the full product. |
| P29 | Single-server deployment is fine; do not overbuild distributed coordination. | **Accepted boundary.** In-process resource lanes and one background worker; horizontal scaling would require new coordination, not assumed safety. |
| P30 | TMDB for movies/TV; temporarily unblock catalog credentials, later use own key. | **Addressed integration; operator follow-up remains.** TMDB-backed catalog with existing credential paths. Replacing a temporary key with the user's own is not verified complete here; never publish credentials. |
| P31 | `movies.enabled` should not exist; reuse clear configuration rather than adding arbitrary feature switches. Show the settings surface. | **Addressed.** No top-level movie enable flag; catalog, storage, quality, Plex, comparison and Seerr integration settings have explicit purposes. Integration-specific enable controls are separate from making movies a first-class feature. |

## Historical One-Offs And Unverified Old Incidents

| Item | Disposition |
| --- | --- |
| Merge/push `mg-open-library-cover-rpc` and retain `.claude` work. | Historical requests, not current feature backlog. Exact old branch/commit outcomes were not reconstructed in this audit. |
| Red Rising database surgery for manifestation 57/58. | Current identity/preservation fixes exist; old surgery outcome not independently rechecked here. |
| Prod EPUB download 500s and book 75 not loading. | Historical incidents. Current book download/SPA acceptance exists; no live repro or fresh root-cause assertion for those exact requests in this ledger. |
| Annie's 401s and monthly session expiry. | Long-lived session work exists. Do not attribute a new 401 to that old incident without current logs. |
| "One day" catalog search timeout/empty results. | Historical search incident; independent provider-error handling exists. Exact current query quality was not rerun. |
| MAM credential failure / Prowlarr cooldown. | User reported credential repair; current shared failure isolation exists. A new cooldown still needs fresh evidence rather than repeating the old diagnosis. |
| Alchemised/ACoL/other targeted retranscription, wrong-language audio reports. | Historical paid operations; no new jobs triggered by making this inventory. Verify their persisted outcomes before calling those exact incidents closed. |

## What To Finish Before Adding More Scope

1. Fix U22's problem-list pagination and verify it through the real UI, including
   an all-healthy inventory and a problem beyond the first healthy raw page.
2. Finish reliable activity/status presentation and future gap evidence; do not
   manufacture causes for old warnings or add routine per-title searches.
3. Collect real production upgrade and next-airing-TV evidence when available.
   No new scheduled Codex monitor without user instruction.
4. Close the existing narrow recovery/retention limitations intentionally before
   turning on broader deletion, library-bound retirement, or array-wide recovery.
5. Keep chapter quality, multilingual/catalog quality and provider replacement
   explicit open threads rather than smuggling them into a finished-POC claim.

## Evidence Map

- [Latest UI follow-through and production evidence](ui-follow-through.md)
- [Completion checklist and remaining canaries](completion-checklist.md)
- [UI interaction principles](ui-interaction-model.md), [home rails](home-rails.md),
  [unified Library](unified-library.md), [book SPA](book-spa.md)
- [RSS contracts and failure/gap semantics](indexer-feeds.md),
  [real RSS canary](movie-rss-canary.md), [upgrade evidence limits](movie-upgrade-validation.md)
- [TV](tv.md), [related works/mapping sidecar](tv-anime.md),
  [retention](retention.md), [TV retention](tv-retention.md), [recovery](recovery.md)
- [Stewardship](stewardship.md), [orphan retirement](torrent-retirement.md),
  [book storage/migration](book-storage.md), [private daemon](transmission-runtime.md)
- [Execution boundaries](durable-media-execution.md), [worker model](runtime-workers.md),
  [performance](performance.md), [CI/test audit](ci.md)
- [Corpus policy](chapter-curation-corpus.md),
  [transcription experiment](../scripts/transcription-eval/README.md)

Some older documents still contain obsolete route names or historical algorithms.
Use current code and the dated latest rollout over stale prose. This ledger is
an inventory, not authority to start every deferred feature or paid experiment.
