# TV Sync And Comparison Audit

`/tv-sync` combines two distinct administrator-only surfaces: native independent
acquisition from approved Seerr TV requests, and a read-only Sonarr comparison
audit. Native controls can queue real downloads, verified episode imports, and
isolated Plex publication. They are not read-only audit actions.

Only the Sonarr comparison audit is read-only: it does not download, adopt,
publish, change Sonarr monitoring, or gate native acquisition. Its **Would import**
label is a counterfactual audit result, not evidence that independent acquisition
or publication completed. Actual native availability comes from the separately
reported available/requested episode coverage.

## Connection And Worker Wiring

The server uses the existing flat `tv` settings: `sonarrUrl`, `sonarrApiKey`,
`pathMappings`, `lookbackDays`, `maxCasesPerSync`, and `pollIntervalMinutes`.
There is no `tv.enabled` flag or second Seerr credential setting. Sonarr URLs
include their configured URL base. Keys remain server-only.

Path mappings explicitly translate Sonarr-reported source roots into local,
server-visible roots. There is no RPC parameter for selecting an arbitrary path,
changing credentials, or expanding the observed filesystem boundary.

The parent integration registers `TV_SHADOW_SCHEMA_SQL` as migration 48,
`BooksRepo.tvShadow`, `WorkerContext.tvShadowServices`, `tv_shadow_sync` /
`tv_shadow_evaluate` dispatch, the RPC/tRPC router, and `/tv-sync` navigation.
The shadow implementation exports:

```ts
type TVShadowServices = {
  observe(): Promise<TvShadowObservation[]>;
  inspect: TvDecider["inspect"];
  probe: typeof probeMovie;
};

syncTvShadow(ctx, services?);
evaluateTvShadow(ctx, job, services?);
queueTvShadowCase(repo, caseId);
configuredTvShadowServices(ctx);
```

The default observer uses Sonarr GET history/queue/catalog reads. It never sends
Sonarr commands. The TV verifier uses the existing agent configuration and shared
movie stream-facts/quality policy; no fictional movie identities are introduced.

## Capture And Execution Authority

Every pass observes fresh source state and inventories the currently mapped files.
The whole Sonarr lookback is not a durable cached step: replaying a provider
snapshot must not hide changed filesystem evidence.

For each observation, one SQLite transaction upserts its catalog entry, resolves
the observation's exact provider episode keys to stable internal episode IDs,
inserts a deduplicated immutable case, creates the evaluation job/outbox intent,
and attaches that job. A failure at catalog, case, job, or attachment insertion
rolls the entire unit back. Every mutation uses the repository `beforeWrite`
execution guard.

The case table stores only evidence, its hash, the owning job reference, a
write-once result, and capture time. It has no status, attempts, leases, retry
schedule, or error columns. Read-model execution status comes from the actual
job/Reflow journal. Results do not mark jobs succeeded; the execution runtime does.

Polling timestamps, upstream verdict/messages, and the advisory library path do
not repay evaluation when the actual evidence is unchanged. Changes to catalog,
policy, mapped files, file size, timestamps, device, or inode produce a new case.
Old input snapshots and completed results cannot be overwritten. By default the
list hides superseded snapshots of the same observation key; history is optional.

## Independent Episode Mapping

The captured entry includes stable internal episode IDs and provider-scoped
numbering, aliases and explicitly sourced relationships. The verifier receives
only the entry, exact wanted episodes, policy, release title, and actual file
names/sizes/probes. Sonarr's outcome, diagnostic messages, and raw filesystem paths
are not supplied as answers to the verifier.

The verifier independently maps file indexes to wanted internal episode IDs.
Unknown or duplicate files/episodes are invalid plans, never accepted maps.
Partial packs and whole multi-episode files are supported without inventing
coverage. Probe-derived codecs, dimensions and audio tracks override model claims;
the existing movie quality evaluator supplies hard-policy outcomes and reasons.

Missing files, missing playable audio/video or duration, failed probes, absent
episode identity, and incomplete wanted coverage are **unobservable**, not proof
of a wrong release. A verified identity with known disallowed quality may be
**Would reject**. Unknown quality can remain **Evidence incomplete**. Routine
fully verified eligible maps become **Would import** without human approval.

The filesystem inventory is checked before probing, between probes, after
inspection and immediately before saving a result. If evidence changed, even a
cached probe/inspection receipt yields an unobservable result. A later sync can
capture a new case; the old result is not rewritten into a confident rejection.

## Explicit Anime Projection

For Sonarr-classified anime, the default observer calls `enrichAnimeEntry` before
capture. Explicit AniDB/TVDB mappings may split an aggregate into separately keyed
logical works with source-labelled numbering and AniList relationships. The
classification is not a claim about a work's country of origin.

Each projected observation intersects work membership with the original exact
selected episode keys. Its unique case key includes the work key; `sourceKey`
retains the original Sonarr observation identity. The union of projected wanted
keys must equal the original wanted set, with each key assigned once. No title
matching, coordinate-as-identity substitution, or unrelated season fanout occurs.

Unavailable, malformed, incomplete or ambiguous enrichment keeps the original
observation with an explicit enrichment-pending warning. Unresolved provider
episodes are not silently dropped. Known public mapping/relationship source URLs
remain provenance; provider URLs and configured credentials are redacted.

## Filesystem And API Boundaries

`src/media/observed-files.ts` is a neutral inventory shared with movie shadow.
The movie wrapper retains its existing four-field file fingerprint contract.
TV adds change-time/device/inode evidence. Only explicitly mapped absolute roots
are inspected. Source symlinks, including intermediate links pointing back inside
the root, fail closed. Traversal outside a mapped root and unmapped paths cannot
authorize probing. Inventory is bounded at 5,000 visited entries, depth 6, and 50
media files; exceeding a bound produces unavailable evidence, not a partial pack.

All `tvShadow` methods require administrator authentication:

- `status({})` returns configuration booleans/counts, poll bounds, and the latest sync job. Configured is not a live-health claim.
- `list({cursor?, limit?, includeHistory?})` returns `{items, nextCursor}`; limit defaults to 25 and is capped at 100.
- `get({caseId})` returns `{case}` with immutable evidence, independent maps/probes and job-derived execution state.
- `sync({})` queues a read-only observation and returns `{jobId}`.
- `retry({caseId})` retries only a failed/cancelled unfinished evaluation and returns `{jobId}`.

Unknown mutation parameters, paths, URLs and credentials are rejected. Raw
provider/model/probe errors are not exposed in the admin read model. The compact
UI shows connection configuration, observation progress, recent cases, and
expandable work relationships, provider number maps and actual probe evidence.
It has no approval form and no claim that an audited file was natively acquired.
`TvSync` accepts optional children for a separate parent-owned native status view.

The page also shows native `tv.status/list/get` data in a distinct **Independent
TV acquisition** section. Available/requested episode counts, future/undated
episodes, actual job state, selection/mapping decisions, and execution events are
not inferred from comparison results. Native sync and retry use `tv.sync/retry`;
explicit recovery of intentionally absent content is labelled **Re-request
episodes**. Expanded request details refresh independently; collapsed requests do
not fan out individual detail queries.

The bounded canary form uses `tv.backfill` with one to five distinct explicit
Seerr request IDs and an episode limit from 1 to 50, defaulting to **one episode
per request**. Submission starts real acquisition, not an audit. Existing native
coverage and recorded backfill limits are preserved by the server; a lower form
value does not shrink an existing request. Queue receipts never claim successful
download or publication.

## Acceptance

```sh
bun test test/unit/tv-shadow.test.ts test/unit/tv-upstream.test.ts test/unit/tv-anime.test.ts test/integration/movie-shadow.test.ts
bun run typecheck
bun run validate:runtime-scripts
bun run build
bun run test/browser/tv.ts
```

Tests cover real Reflow dispatch/journal status, atomic outbox rollback and stale
execution guards, immutable deduplication, exact anime projection, independently
mapped files and probe quality, changed/resumed sources, symlink/path boundaries,
credential redaction, and administrator-only method contracts. Movie shadow
regressions exercise the neutral extraction. Chrome acceptance uses an isolated
`TV_FIXTURE` mode and real auth/RPC handlers. It checks native coverage versus audit
verdicts, anime source diagnostics, bounded default-one backfill payloads, actual
queued sync/retry state, path rejection, desktop/mobile layout, and runtime/network
errors. New native work remains queued, with no external provider calls or
transfers. Screenshots are under `tmp/movies-lab/tv-sync-*.png`.

Synthetic fixture coverage is not production audit, real transfer, or Plex
publication acceptance.
