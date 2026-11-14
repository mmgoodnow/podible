# Time to Plex

The [Grafana generator](../scripts/grafana-dashboard.ts) adds a Time to Plex
section to the existing dashboard. It tracks acquisition queue to exact Plex
verification, Transmission completion to publication, publication to verified
availability, and individual filesystem reconciliation passes. The percentiles
are for successful observations, so read them alongside sample counts and the
Activity page's unfinished requests. No historical timings are fabricated.
The movie and TV stacks use the same completed-download cohort for both phases:
preparation is completion to publishing, and Plex time is the remainder of
completion to verified availability. Blank buckets have no qualifying samples
or no metrics. The adjacent count panel distinguishes idle buckets (zero
observations) from unavailable metrics. Filesystem link latency is shown as
separate p95 points, not added to the stacks.

For diagnosis, `podible_plex_publication_phase_seconds` breaks out each
attempt's lock wait, scan request, catalog verification and TV upgrade roster
check. These process-local histograms count unsuccessful operations too.
The scan request measures Plex's acknowledgement, not its asynchronous indexing.
Podible does not wait for intro detection or thumbnail generation; readiness
requires the exact catalog identity and file paths.

For recorded end-to-end runs, use
`bun run scripts/report-plex-latency.ts --db /path/to/podible.sqlite --limit 10`.
The report reads a single SQLite snapshot and compares each trace to the
20-second movie and 30-second TV targets. It reconstructs timings from saved
torrent observations and publication receipts. It omits reuse, missing
completion evidence, and ambiguous events, and does not recheck Plex live.

## TV View Enumeration

A read-only Cyprus measurement on November 14 projected 1,543 TV files with
9,052 SQL calls in 0.92-1.25 seconds per pass. The bulk projection took 16-17 ms
and matched the full set of view paths and backing paths in the same SQLite
snapshot. These are database/render timings, not filesystem reconciliation or
download-to-Plex timings.

## View Reconciliation

Run `bun run test/lab/view-reconcile-benchmark.ts 1543` to measure adding,
checking, and removing links from a synthetic view. On the same local
filesystem, a pass fell from 225-241 ms to 41-50 ms by amortizing read-only
checks and skipping journal writes when the view already matches. File changes
still recheck ownership, paths, and publication state immediately before writing.

The TV hot path audits the new version's links plus all manifest changes.
Unrelated links that already match the committed projection remain untouched.
Maintenance and interrupted journals still force a full audit. On the same
1,543-file lab, adding, rechecking, or removing one episode took 7-9 ms
versus 43-46 ms for a full audit.

## November 14 Workflow Breakdown

Read-only Reflow timings for TV acquisitions 164, 166, 168 and 169 showed
19-24 seconds in the import verifier, 1-7 seconds probing streams, and
18-62 seconds publishing. Only 0-5 seconds of that publication time was
spent waiting for the Plex event. The rest was inside Podible's publication
attempts (view reconciliation, lock acquisition, scan requests and exact
catalog checks). These runs precede the scoped reconciliation change.

Small TV import batches now inline up to 16 episodes and 16 probed files under
a 16 KB evidence limit, so the verifier can use runtimes and streams without
another model round trip. Larger or ambiguous batches keep the same tools.
Episode assignment, probe-derived quality and exact Plex checks are unchanged.

## Attack on Titan baseline

Read-only verification on Cyprus, 2026-11-10, running
`0189d9d3a921c665614dba3ecb282eae1007742b`: request 6314 expected 89
episodes. The selected 75 (25/25, 12/12, 22/22, 16/30) had regular canonical
files, matching library links, and exact Plex episode identity and path.
Both finales were missing. Acquisition 150 stopped after three stalled
alternatives. That is not evidence that the remaining episodes are unavailable.

One earlier successful batch (142) took 82m38s from its checking event
to its publishing event, then another 67s until Plex verification. The
checking interval includes multiple operations; it is not a measurement
of hashing alone. The changes following that baseline remove a full
Transmission file-list call per adopted episode and replace the per-MiB
full state check during read-only hashing with cooperative cancellation
and periodic ownership checks. Publication still revalidates source files.

## November 11 follow-up

Read-only verification on Cyprus, running `a6f7843`, found all 89 requested
episodes at their exact identities and paths in Plex section 16. The canonical
files and library symlinks also matched. TVDB numbers the two Final Chapters
as S04E29 and S04E30, not as Season 0 entries. A manual resume was needed after
the stalled batch: 75 episodes landed before it, 12 after it, and the automatic
continuation added the final two.

| Acquisition | Download complete to publishing | Canonical adoption/finalization | Publishing to Plex verified |
| --- | ---: | ---: | ---: |
| 160 | 14m 28s | 13m 38s (44.3 GB) | 91s |
| 161 | 3m 07s | 2m 37s (7.8 GB) | 24s |

These runs narrow the largest delay to adoption/finalization, not the Plex
scan. They predate the detailed finalization timers and larger read buffer in
`8e67afc`; they do not demonstrate the effect of that change on Cyprus.

Canonical torrent imports no longer reread payloads to compute SHA-256.
Transmission must report all bytes valid and none unchecked before sealing.
Podible then records the sealed file's identity, size and timestamps without
opening the payload. Independent-copy checks before deletion are separate.

A successful older publication is not a live-path timing canary for the
new build. Verify a fresh download completion, durable worker wakeup,
exact Plex path and identity, browser subscription update, and fresh Grafana
sample before declaring production latency improved.
