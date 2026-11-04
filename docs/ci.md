# CI Gates And Caches

`.github/workflows/docker.yml` retains push and manual triggers. Publication still
uses `mmgoodnow/cd/.github/workflows/docker.yml@main`, with inherited secrets,
only after the `verify` gate succeeds. No deployment commands are added here.

## Job Graph

- `unit`: frozen Bun install, ffmpeg/unzip/dictionary prerequisites, and the full
  `bun test` suite, including integration tests. It starts independently.
- `build`: frozen install, generated DB type check, backend/web typecheck, frontend
  build and runtime-script validation. It uploads `frontend-dist` once.
- `browser`: three independent runners after `build`, consuming that exact dist.
  Library runs `spa`, `books`, `library-performance`; screen runs `movies`,
  `movie-shadow`, `tv`, `unified-library`; admin runs `admin`, `auth`, `recovery`.
  All ten entrypoints run exactly once. Each runner installs Chromium/dependencies
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
- Browser suites exercise rendered UI and actual fixture-server API paths, but
  fixture providers do not prove production TMDB/Sonarr/Plex availability. The
  network-isolated runtime lab proves local daemon behavior, not external tracker
  acceptance. Migration acceptance is useful on migration changes, not every push.

No tests were deleted. The toolchain assertion was updated for equivalent resolved
YAML configuration; protocol, integration and browser assertions are unchanged.

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

No GHCR cache/image writes or workflow dispatch were performed for verification.
The new three-runner browser matrix and remote registry-cache transfer have not
been executed here. The parent owns the full-suite and browser release gates;
these local results are not a measured end-to-end CI rollout improvement.
