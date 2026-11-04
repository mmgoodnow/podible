# Library Responsiveness

## Verified Evidence

The production cover-cache follow-up used Chrome through Playwright CLI, three full
Library navigations, and a 1280x720 viewport. No request routing, cache bypass, job
actions, or raw HAR capture was used. Visible-image completion required all visible
cover elements to complete; all 12 also had a nonzero natural width on each run.

| Build / Navigation | Cards ms | Visible Covers ms | Cached Covers | Cover Transfer Bytes |
| --- | ---: | ---: | ---: | ---: |
| Before cache fix, cold | 315 | 1362 | 0 / 14 | 641828 |
| Before cache fix, warm | 9947 | 10496 | 0 / 26 | 1264489 |
| Before cache fix, warm again | 230 | 330 | 0 / 28 | 1410864 |
| ad61656, cold | 1295 | 4450 | 0 / 13 | 606769 |
| ad61656, warm | 1698 | 1707 | 30 / 30 | 0 |
| ad61656, warm again | 5836 | 5842 | 30 / 30 | 0 |

This proves warm browser cover reuse, not consistently fast page loads. The last
follow-up navigation had 12 ms HTML TTFB but a 3750 ms `auth.me` request followed
by a 1665 ms `library.list` request. Another navigation had 1603 ms HTML TTFB.
There were no page, API HTTP, cover HTTP, or failed-request errors. The single
console error was the missing favicon (HTTP 404).

At 2026-11-04 06:50-07:00 UTC, direct container inspection still reported
`bfa4b2d4688a693ba3c1b6bf2f2757f4882b0b47`; its `server.ts` did not construct a
`BackgroundWorker`. Although the `41a88af` build/push completed successfully at
06:26:33 UTC, the older `bfa4b2d` build/push completed later at 06:27:12 UTC.
The observed deployment is consistent with overlapping builds overwriting the
mutable `latest` tag out of order. A successful CI run is not evidence that the
worker-isolated revision is serving traffic. No manual deployment was attempted.

Separate read-only production profiling of the real first 48 books measured
approximately 5-28 ms for list and playback enrichment together. This does not
explain multi-second network timings by query computation alone. A separate
SQLite connection also does not measure request event-loop queueing.

At 07:04 UTC, 20 independent loopback HTML requests to that old build returned
HTTP 200. Most completed in 0-57 ms, but two took 5289 and 5227 ms. The matching
server request logs recorded 0 ms inside those handlers. These anonymous controls
did not perform session activity writes and bypassed the CDN/public network. They
demonstrate server-side pre-handler queueing or scheduling stalls, not an
exclusively CDN/network problem. They do not establish the precise blocking stack
or measure the worker-isolated build, which was not deployed during the capture.

## Narrow Fixes

Optional session activity updates were throttled, but still inherited SQLite's
2000 ms busy timeout. A real independent WAL writer reproduced a 2111 ms
authenticated read before the fix. Ignoring `SQLITE_BUSY` after waiting did not
prevent a synchronous event-loop stall.

`touchSessionWithoutWaiting` temporarily sets that connection's busy timeout to
zero for the synchronous activity update, then restores its prior value even on
failure. Only optional bookkeeping uses this helper; durable writes retain their
existing timeout. Every request still reads the current session and checks expiry
and revocation. Busy/locked activity remains best-effort and rate-limited;
unexpected storage failures still fail visibly. The same lock regression now
completes in under 1 ms on the local test machine.

`library.bootstrap` returns the authenticated actor/session metadata and the first
Library page from one request. It is user-authenticated, read-only, input-bounded,
and served with `Cache-Control: no-store`. It uses the existing Library read and
playback logic; no new credential or acquisition path is introduced. The SPA no
longer waits for a separate `auth.me` request before starting its first Library
read. The returned page seeds only that actor's matching query; filters,
pagination, and subsequent refreshes retain their own query keys. Non-Library
routes keep the existing identity query.

These source fixes and local regressions are not a claim of post-deployment
production responsiveness. Verify the actual running SHA before labeling any
subsequent browser measurements as worker-isolated or bootstrap-enabled.

The outstanding production gate is the parent-owned commit/normal CD followed by
running-SHA verification and a new actual PWCLI measurement. Do not mark that gate
complete based on the successful but superseded worker image build alone.

## Acceptance

Owned files for this follow-up: `src/http/middleware.ts`, the session-touch helper
in `src/repo.ts`, `src/rpc/library-router.ts`, `src/trpc.ts`, `web/src/main.tsx`,
`web/src/books.tsx`, `test/unit/session-activity.test.ts`,
`test/integration/library-bootstrap.test.ts`,
`test/browser/library-performance.ts`, the anonymous-bootstrap assertion in
`test/browser/auth.ts`, and this document. No TV, retention, or migration files
were changed by this track.

Run `bun test test/unit/session-activity.test.ts
test/integration/library-bootstrap.test.ts`, plus the existing HTTP/RPC/book API
regressions, `bun run typecheck`, and `bun run build`. Chrome acceptance is
`bun run test/browser/library-performance.ts`; existing books and auth browser
suites cover the unchanged interactions and permission boundaries.

The contention test holds an independent WAL writer while an aged authenticated
session reads. It checks bounded latency, HTTP success, timeout restoration, and
revocation. Bootstrap API tests cover anonymous/expired/revoked denial before any
Library read, actor identity, bounded input, opaque/versioned artwork, matching
list results, no credentials, and no queued jobs. Browser acceptance holds the
old identity endpoint while checking that cards still render from exactly one
bootstrap without a duplicate first-page request; it also checks filtering,
pagination, and reader/anonymous isolation.

For production measurement, use an explicitly approved five-minute development
session for the existing verification user, a mode-0700 private transfer directory,
and an owned named Playwright CLI browser. Never print cookie values, storage
state, or raw HAR data. Record only paths, timings, status codes, transfer sizes,
and sanitized error metadata. In `finally`, revoke the exact token-hash session,
close/delete only the owned browser profile, and remove credential files.

Browser resource timing separates request start, response start, and body
transfer. Compare it with independent loopback HTML latency, sanitized HTTP
handler logs, and read-only SQLite timings before attributing a delay to the CDN,
network, application queueing, or database work. Handler time alone starts too late
to rule out an event-loop stall before dispatch. Preserve the distinction between
measured latency and inferred causes.
