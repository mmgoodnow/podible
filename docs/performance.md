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
| 8344dbb, cold | 461 | 1197 | 0 / 20 | 963894 |
| 8344dbb, warm | 1125 | 1132 | 30 / 30 | 0 |
| 8344dbb, warm again | 349 | 357 | 30 / 30 | 0 |

The ad61656 capture proved warm browser cover reuse, not consistently fast page
loads. Its last navigation had 12 ms HTML TTFB but a 3750 ms `auth.me` request followed
by a 1665 ms `library.list` request. Another navigation had 1603 ms HTML TTFB.
There were no page, API HTTP, cover HTTP, or failed-request errors. The single
console error was the missing favicon (HTTP 404).

Cover resource counts are sampled at visible-image completion; deferred offscreen
loads can change the count and cold transfer total between captures.

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

### Verified Worker/Bootstrap Follow-Up

At 2026-11-04 07:33 UTC, the production container actually ran
`8344dbb21b12317a38a8b3e4c7199423bb905110`, a descendant of `9ee32b9`, with worker
isolation, bootstrap, and nonblocking session activity present and HTTP ready.
The same SHA was confirmed at session revocation after measurement. The normal
CI/CD run completed successfully, including the new Library browser acceptance,
image manifest, and watchtower update; no manual deployment was performed.

All three actual production PWCLI navigations rendered 48 cards and loaded all
12 visible covers. Each issued exactly one `library.bootstrap` API request:
74, 62, and 151 ms end-to-end. There was no separate initial `auth.me` or
`library.list` request. The corresponding server bootstrap handler times were
26, 11, and 18 ms. Both warm navigations reused 30/30 covers with zero cover bytes
transferred. There were no page/API/cover/failed-request errors or non-read
requests; the one console error was the existing favicon HTTP 404.

Twenty new loopback HTML controls completed in 1-4 ms, versus the old capture's
5289/5227 ms stalls. Eight read-only first-page SQL/playback samples took about
5-17 ms. Browser HTML TTFB remained variable at 14, 1035, and 146 ms, while the
corresponding HTML handler times were 2, 1, and 0 ms. This bounded capture did not
reproduce the old multi-second server stall. It does not prove that all future
latency is eliminated or identify the exact cause of the remaining external
HTML delay; handler timing alone cannot rule out pre-handler queueing.

Verification used the approved existing user 36 and an owned PWCLI browser. The
temporary session 102 was capped at five minutes, then deleted by its exact token
hash (one row deleted, zero remaining). The owned browser/profile was closed and
deleted; both credential files were removed and the transfer directory remained
mode 0700. No cookies, tokens, or raw HAR were printed or retained in the report.

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

The production acceptance gate is complete for the bounded 8344dbb capture above.
Subsequent performance claims still require actual running-SHA verification;
a successful image build alone is not deployment or browser evidence.

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
