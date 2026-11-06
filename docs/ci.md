# CI Gates And Caches

`.github/workflows/docker.yml` retains push and manual triggers. Publication still
uses `mmgoodnow/cd/.github/workflows/docker.yml@main`, with inherited secrets,
only after the `verify` gate succeeds. No deployment commands are added here.

## Job Graph

- `unit`: frozen Bun install, ffmpeg/unzip/dictionary prerequisites, a local
  frontend build, and the full `bun test` suite, including integration tests.
  It starts independently. HTTP integration tests require real `web/dist` SPA
  shells; another runner's build and a developer's ignored local assets are not
  available here. This small duplicate build preserves parallelism without
  downloading/waiting for `build`'s artifact.
- `build`: frozen install, generated DB type check, backend/web typecheck, frontend
  build and runtime-script validation. It uploads `frontend-dist` once.
- `browser`: three independent runners after `build`, consuming that exact dist.
  Library runs `spa`, `books`, `library-performance`; screen runs `movies`,
  `movie-shadow`, `tv`, `unified-library`; admin runs `admin`, `auth`, `recovery`,
  `stewardship`. All eleven entrypoints run exactly once. Each runner installs Chromium/dependencies
  once, then runs its suites sequentially with a five-minute per-suite timeout.
  Matrix fail-fast is disabled so another group's failure does not erase coverage.
  Library and screen explicitly install ffmpeg, unzip and wamerican: both use
  `createBookSpaData` through book/unified fixtures, including real audio encoding.
  Admin explicitly installs openssl for its TLS certificate fixture. These packages
  are installed on each relevant isolated runner, never inherited from `unit` or
  assumed present in the Ubuntu runner image.
- `runtime`: starts independently, builds/loads the final runtime image and runs
  `test/lab/transmission-inner.ts` with `--network none` and a read-only test mount.
- `verify`: preserves the existing aggregate check name and rejects any failed,
  cancelled or skipped required job. `docker` still depends on this gate; there is
  no publication shortcut on a failed matrix or runtime build.

Browser screenshots are separate seven-day artifacts per group. These standalone
Bun/Playwright scripts are not Playwright Test specs, so CLI `--shard` would not
partition them. Separate runners avoid fixture/environment/screenshot collisions;
no suite concurrency is introduced inside a runner.

## Build Cache

The runtime lab uses Buildx's container builder, `linux/amd64`, `load: true`, and
`push: false`. Only its local test tag is loaded, not published as a release.
Authenticated registry cache import reads both
`ghcr.io/<lowercase-owner/repository>:cache-lab-amd64` and CD's `:cache-amd64`.
Export writes only `:cache-lab-amd64,mode=max`. This is a cache-manifest write
before verification, not an application-image publication or deployment.

`mode=max` retains intermediate stages, particularly Transmission's compile,
which plain `docker build` on a fresh runner did not restore. Distinct writers
avoid overwriting production's metadata-bearing cache during concurrent pushes.
Git metadata build args can invalidate late runtime layers, but not the independent
Transmission stage. CD does not import the new lab cache, so newly built lab-only
layers are not promised to accelerate the subsequent publication build; changing
that would require a separate shared-workflow change.
The arm64 publication continues using CD's separate `:cache-arm64`; the amd64 lab
does not claim arm64 runtime acceptance. The shared CD repo is not modified.

Cache misses remain normal full builds. Application source is not copied into
the Transmission stage, while changes to its base, build arguments or commands
correctly invalidate that stage. The Dockerfile is unchanged: release 4.1.4 still
requires SHA256
`86e7aa020cdf64826447034bc85f4d3b7c848d8c0891d9dc9b9fddfb2c805d22`
before extraction. A cache hit reuses that content-addressed build result; the
real daemon acceptance still runs on every push. Neither credentials nor live
daemon state is added to the image/cache.

Unit, build and browser runners share a lockfile/Bun-version-keyed download cache,
not `node_modules`; each isolated runner still performs its own frozen install.
The runtime job needs no host Bun install. Browser binaries are not cached:
Playwright notes that browser-cache restore costs can rival download costs and
Linux system dependencies still require installation. Avoiding ten separate
browser runners limits that setup to three installations.

Sources: [Docker registry/max-mode cache](https://docs.docker.com/build/ci/github-actions/cache/),
[Buildx load/path-context inputs](https://github.com/docker/build-push-action),
[Playwright CI guidance](https://playwright.dev/docs/ci#caching-browsers).

## Migration Lab

`test/lab/book-migration-inner.ts` is no longer a per-push gate. Its source and
unit tests remain. Run the workflow manually with `book_migration_lab: true` to
exercise it alongside all normal gates, or run locally against a built image:

```sh
docker run --rm --network none -v "$PWD/test:/app/test:ro" \
  --entrypoint bun podible-transmission-test:ci run test/lab/book-migration-inner.ts
```

## Test Evidence Audit

Keep distinct evidence levels rather than dropping mocks indiscriminately:

- `test/unit/transmission-packaging.test.ts` checks Dockerfile text, checksum order
  and build flags. Useful cheap packaging invariants, but not proof compilation
  or dynamic linking works; the real image build/version check and daemon lab
  supply those gates.
- `test/unit/toolchain.test.ts` parses workflow YAML and resolves workflow/job/step
  environment values for every Bun setup step. It checks version alignment rather
  than requiring a repeated literal, but does not execute GitHub Actions.
- `test/unit/transmission.test.ts` uses mocked HTTP to force lost add acknowledgments,
  session challenges and deferred-stop races. Valuable fault injection; it cannot
  prove real Transmission protocol compatibility or a private tracker's whitelist.
- `test/unit/browser-capture.test.ts` mocks the page object to check bounded retries
  and error propagation. Its fixture bytes are not visual/compositor evidence;
  actual Playwright screenshots remain browser acceptance.
- `test/integration/job-command-replay.test.ts` uses real SQLite and Reflow across
  reopen/replay boundaries with injected acknowledgment failure. This is durable
  integration evidence, not just an asserted mock call sequence.
- `test/unit/tv-model.test.ts` exercises real SQLite identity, numbering,
  transactional rollback and publication constraints. The `unit` directory does
  not imply mock-only tests.
- `test/integration/all-job-execution.test.ts` now keeps exhaustive runtime and
  compile-time job registration coverage, but runs its substituted-handler
  SIGKILL/restart scenario for six representative policy shapes rather than every
  job name. Those cases cover serialized book-media work, two-wide manifestation
  work, wide release work with a long retry budget, wide unscoped work, and both
  serialized/wide global maintenance. Fixtures assert their real lock keys and
  exponential/linear retry policy. Separate live blocking tests exercise actual
  workflow limits 1, 2, and 16. A new workflow policy/locking shape needs a new
  representative; adding another job name still requires exhaustive registration
  coverage but not an identical child-process pair.
- The three **real-handler** SIGKILL cases in that same file remain unchanged:
  cover generation, metadata hydration, and reconciliation retain domain effects
  across process death. `media-execution-crash.test.ts` still interrupts the real
  book pipeline at search/selection, external snatch, download handoff and asset
  publication, including cancellation and ownership takeover. Movie upgrade and
  Seerr fanout crash suites remain intact too. Reducing the generic loop is not
  permission to replace these with a substituted handler.
- The basic SPA reader-navigation assertion now checks the actual visible links.
  Checking only for absence of the removed "Movie sync" label was vacuous after
  that label disappeared for administrators too.
- Browser suites exercise rendered UI and actual fixture-server API paths, but
  fixture providers do not prove production TMDB/Sonarr/Plex availability. The
  network-isolated runtime lab proves local daemon behavior, not external tracker
  acceptance. Migration acceptance is useful on migration changes, not every push.

Toolchain and reader-navigation assertions check effective configuration and
visible behavior rather than obsolete literals. The retired Movie sync/TV sync
pages and their orphaned form-parser tests are removed. Their backend scenarios
now run in `test/integration/diagnostic-api.test.ts` through the `unit` gate:
actual HTTP authorization, durable movie review, queue receipts, TV episode
evidence, published coverage and strict season-only scope. The browser entrypoints
keep their names: `movie-shadow` verifies legacy redirects and absence of raw
diagnostic polling; `tv` verifies user-safe native requests, privacy, withdrawal,
and desktop/mobile screenshots. Backend shadow collection/evaluation, Seerr
dual-write, protocol and recovery safety coverage is retained.

### Audit Decisions

The useful distinction is observable application behavior versus rehearsing a
mock's configured response, not the presence of mocks or the directory name.

| Surface | Decision | Evidence retained |
| --- | --- | --- |
| Generic per-job process-death loop | Consolidate to six policy shapes | Saved step not rerun, run ID preserved, one publication after actual SIGKILL/restart; every registered job still executes in the exhaustive test |
| Real-handler crash/replay and command outbox | Keep | Domain/filesystem effects, durable ownership, cancellation and transactional receipts across process boundaries |
| Client protocol fault injection | Keep | Uncertain add acknowledgments, authentication/session challenges and stop/resume races are difficult to reproduce reliably with a live daemon |
| Real Transmission/image labs | Keep on normal CI | Actual daemon compatibility, transfer/restart, canonical inode/layout and packaging behavior; mocked HTTP cannot replace them |
| Migration-only daemon lab | Keep opt-in | The one-off migration is complete; it does not justify repeating an expensive migration rehearsal on unrelated pushes |
| Workflow/toolchain assertions | Keep as narrow configuration contracts | Version alignment, required gates and exhaustive suite assignment; do not claim these prove deployment or rendered UI |
| Browser acceptance | Keep the three isolated CI groups | Real rendered actions and fixture API/auth boundaries; remove assertions for obsolete copy, not meaningful user flows |

Connections regressions now prove historical gap evidence survives the real
backend diagnostic RPC while none of it appears in collapsed or expanded UI.
They still assert current errors/cooldowns, unknown data and keyboard disclosure.
This deliberately replaces the old positive assertions for permanent gap badges;
it does not erase backend history or weaken provider-failure handling tests.

No broad mock-test purge was justified by this bounded audit. The removed cost is
duplicated generic process orchestration, not domain safety or protocol evidence.

## Timing And Validation

Expected warm critical path is approximately
`max(unit, build + slowest browser group, runtime)`, plus aggregate gate and
existing publication overhead. Previously all stages and all browser suites were
serial. This is a prediction, not a measured GitHub speedup: cold cache, runner
queues, cache transfer, package installation and uneven browser durations matter.
Three browser runners can increase total runner-minutes while lowering wall time.

Confirm in the first CI runs that the Transmission compile step reports `CACHED`,
all three browser groups pass, the network-isolated daemon lab actually executes,
and `docker` remains skipped if any required job fails. Local syntax/tests or a
native-architecture Docker build cannot demonstrate GHCR hit rates, Linux amd64
execution timing, or the remote publication job's elapsed time.

Local verification on 2026-11-04 used native Linux arm64 Docker Desktop:

- A compile-cache-miss full runtime image build took 164.39 seconds.
- Rebuilding with changed `GIT_SHA=ci-cache-probe` took 1.67 seconds; both
  Transmission prerequisite and compile stages reported `CACHED`.
- The current real-daemon acceptance lab passed with `--network none` in 14.06
  seconds, including transfers, book import, TV ownership and supervisor restart.
- Actionlint 1.7.7, YAML structure/all-ten-suite coverage, Bash syntax and aggregate
  failure/cancellation/skip checks passed. Shellcheck/Pyflakes integrations were
  disabled because those optional tools were unavailable.
- Thirteen focused toolchain/packaging/client/capture/replay tests, DB-type check,
  backend/web typecheck, frontend build and runtime-script validation passed.

Those local checks did not write GHCR caches/images or dispatch a workflow. The
subsequent normal-push GitHub observations are recorded below; the local timings
alone are not evidence of end-to-end CI improvement.

### Observed GitHub Rollout

Run `41919862928` verified all three browser groups and the real-daemon lab. Its
runtime image build/load took 32 seconds, compared with 191 seconds in the prior
uncached verification run. The unit runner correctly failed six HTTP integration
assertions because its fresh filesystem lacked the SPA assets. It now builds
those assets before testing; assertions and the publication gate were retained.

Corrected run `41921125639` passed every verification job. From the first job
starting at 21:59:16 UTC on November 4 to aggregate verification completing at
22:02:59 UTC, verification took 3m43s, versus 10m51s in run `41913276196`.
Both architecture builds and manifest checks also passed. Deployment was correctly
skipped because a newer commit had superseded this run; this timing is not proof
that its build was deployed. Queue, cache and network variability still apply.

The later UI integration pass on November 4 passed 1,346 tests, generated DB type
checks, backend/web typechecking, build, runtime entrypoint validation, and all
eleven Chrome acceptance entrypoints locally. Normal CD exposed stale recovery
navigation selectors; the home rails also required the auth fixture to open a
pending book through the Books tab rather than expect it among published rails.
These selectors were updated against rendered navigation without skipping any
suite, weakening auth/removal assertions, or bypassing the publication gate.
