# Time to Plex

The [Grafana generator](../scripts/grafana-dashboard.ts) adds a Time to Plex
section to the existing dashboard. It tracks acquisition queue to exact Plex
verification, Transmission completion to publication, publication to verified
availability, and individual filesystem reconciliation passes. The percentiles
are for successful observations, so read them alongside sample counts and the
Activity page's unfinished requests. No historical timings are fabricated.

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
