# Acquisition completion

New movie and TV acquisitions running in the managed worker use durable
completion events between download and import, and between Plex scan and
publication. Runs that already checkpointed the previous workflow keep its
original step order until they drain.

- Transmission runs a private completion hook. The hook writes an atomic
  receipt containing only the torrent hash in its filename. The worker watches
  the directory and removes receipts only after delivering the wake to Reflow.
- The workflow registers its interest before reading the torrent or Plex state.
  Reflow buffers wakes even if they arrive before the wait. Worker startup and
  independent daemon restarts check outstanding torrent waits once; an initial or reconnected Plex event stream
  similarly wakes pending publication checks. A wake is not evidence of
  success: torrent identity, download location, and Plex identity and path
  still have to verify.
- A download has an early stall deadline and a hard deadline. Publication has
  a final verification deadline. These bound the effect of a silently lost
  event without an interval of status requests. If a Plex scan is already
  running, there is one opportunity to request another scan after a wake.
- Movie and TV detail pages and library cards use the authenticated tRPC
  subscription `acquisition.changes` over SSE. On connect or reconnect, the browser reads the current
  authorized snapshot. Frames contain no titles, IDs or tracker details.
  Keepalive frames do not trigger reads. Byte counts are snapshots, not a
  per-second stream from Transmission.

Release discovery via RSS catch-up, provider backoff, and the Reflow
outbox/scheduler are separate from external completion detection. The
operational Activity dashboard and books are not part of this cutover.
