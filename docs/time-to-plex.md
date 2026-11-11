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

A successful older publication is not a live-path timing canary for the
new build. Verify a fresh download completion, durable worker wakeup,
exact Plex path and identity, browser subscription update, and fresh Grafana
sample before declaring production latency improved.
