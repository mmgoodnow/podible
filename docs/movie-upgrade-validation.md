# Movie Upgrade Acceptance

Run `bun test test/integration/movie-upgrade-acceptance.test.ts`.

This complements the direct acquisition, RPC, maintenance, and publication tests
with an orchestrated upgrade failure window. Each case launches a separate Bun
process serving the actual authenticated JSON-RPC HTTP handler on loopback and
running the real Reflow dispatcher against temporary application/execution SQLite
files. Requests and upgrades use `movies.request` and `requests.checkUpgrade`;
availability is read through `requests.get`. No direct `acquireMovie` calls.

## Covered

- Invalid sessions and another user's upgrade attempts cannot queue work.
  Repeated requests/checks share an in-flight upgrade job.
- A stalled better release is paused and replaced without losing the old active
  version, its readable view link, or canonical bytes. Plex rejection leaves the
  old movie available during publication and after the upgrade job fails.
- Reflow runs the real `movie_reconcile` worker after rejection to abort stale
  publication intent and retire only the unselected view link. Completed torrents
  and both canonical versions remain, including after request withdrawal.
- Actual `SIGKILL` after the saved snatch or saved negative publication verdict,
  followed by a new process using the same databases, resumes the same job,
  acquisition, generation, and Reflow run. The recovered process is checked while
  Plex still rejects the upgrade; only a later positive verdict promotes it.
- Recovery does not repeat torrent add calls or create extra versions/content
  records. Rechecking the same release and a distinct equal-quality release
  creates search-only jobs, with no new snatches, probes, publication verdicts,
  version/revision changes, or duplicate availability events. The agent fixture
  deliberately proposes the equal-quality release, exercising the domain policy
  guard rather than relying on the fixture to filter it out.

## Evidence Boundaries

The HTTP session/RPC stack, job outbox, Reflow journal/leases/checkpoints,
acquisition/publication/maintenance workers, filesystem adoption, and view
symlinks are real. Maintenance is queued through `repo.createJob`, as a scheduler
would, rather than through a new test-only RPC.

Catalog, indexer, semantic agent, torrent client, media probe, and Plex verdicts
are deterministic local fixtures. Torrent state is persisted separately from the
application to model an external daemon surviving its crash; it is not a real
Transmission/rTorrent instance. The Plex double reads the actual published links
before returning a controlled verdict, but there is no real Plex scan, identity
matching, or playback. Synthetic `.mkv` bytes are not playable media.

The crash points are **after durable Reflow checkpoints**, not every instruction
between an external side effect and checkpoint persistence. These tests do not
prove host power-loss durability, real tracker/seeding behavior, concurrent
multi-worker takeover, or crashes inside link retirement/promotion. Fresh searches
remain allowed; suppression means no redundant download/publication, not that an
upgrade RPC never creates another job. No production services are contacted.
