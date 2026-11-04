# Private Torrent Stewardship

This slice observes only Podible's own private Transmission instance and durable
book/movie/TV bindings. It never controls torrents or deletes records or data.
Status reads stored observations and current bindings without querying the
daemon, writing state, initializing settings, or collecting on page load.

## Contract And Wiring

`stewardshipRouter` is registered under `stewardship` in JSON-RPC and typed
tRPC. The admin panel is available at `/stewardship` ("Torrent health"):

- `stewardship.status` is an ADMIN query. Input is
  `{source?: "books" | "screen", offset?: number, limit?: number}`; defaults are
  books, 0, 25, with a maximum page size of 50. `screen` includes movies and TV.
- `stewardship.collect` is an ADMIN mutation with `{operationId: UUID}`. It reads
  the next batch of at most 50 binding rows and returns a durable collection
  receipt. Replaying the UUID returns the same receipt without another query.
- Status returns `mode: "observe-only"`, `destructiveActionsEnabled: false`,
  `minimumSeedSeconds: 2592000`, pagination/coverage, last collection, and items.
  Each item includes media, a binding label, the latest sanitized observation,
  staleness, `retirementAllowed: false`, and the retirement boundary.

`createStewardshipRouter(adapters)` provides server-side fixture injection;
there are no client-supplied URLs, daemon credentials, torrent IDs, trackers,
metainfo, model choices or policy knobs.

Mount `StewardshipPage` from `web/src/stewardship.tsx` with the existing typed client:

```tsx
<StewardshipPage api={{
  status: input => rpc.stewardship.status.query(input),
  collect: input => rpc.stewardship.collect.mutate(input),
}} />
```

The page polls saved status only. Its explicit collect button has an idempotent
retry. It does not automatically collect during render/effects/query refreshes.
For continuous unattended observation, the normal background scheduler queues
`torrent_stewardship_collect` every `STEWARDSHIP_INTERVAL_MS` (five minutes)
when durable torrent bindings exist, including orphaned seed bindings. Reflow
runs `processStewardshipCollection(ctx, job)` with concurrency one. The handler
checkpoints its operation UUID and collection;
retries reuse the application receipt even if the collection checkpoint is lost.
An optional `{operationId: UUID}` job payload supplies an explicit receipt key.
Execution ownership/cancellation is checked before I/O and persistence. The
durable cursor rotates books and screen pages;
each pass is bounded and no per-title searches are performed. No new settings
or separately scheduled Codex monitor are required.
Use `hasTorrentStewardshipBindings(repo)` as the scheduler's eligibility hint,
not current books/targets: orphaned book torrent tombstones still retain seed
obligations after a book is deleted. The collector remains the ownership filter.

## Evidence And Privacy

The collector reuses `readTransmissionIdentity` and the existing private RPC
transport at the fixed loopback daemon. It requests only exact hashes belonging
to that client, never an empty/all-torrent request. rTorrent and other client
instances are excluded. It checks the expected media label, exact persisted
download directory, canonical directories, and book root/directory ownership
markers. The binding is rechecked inside the observation transaction after I/O.
Binding changes invalidate earlier observations rather than transferring health
to another owner. Movie/TV checks establish a private binding, not proof that
every canonical file or shared seed obligation is independently safe to retire.

Daemon requests have a bounded deadline and response size. Collections retain
the latest observation and at most eight history entries per binding in existing
`app_state`; collection receipts and the rotating cursor survive restarts.
Raw announce URLs, tracker hosts, passkeys, tracker messages, filesystem paths,
metainfo, peer lists and daemon credentials are not stored or returned. Tracker
sets use opaque digests, not plaintext provider identifiers.

The fields and numeric daemon states follow the official
[Transmission 4.0.6 RPC contract](https://github.com/transmission/transmission/blob/4.0.6/docs/rpc-spec.md).
Transmission 4.1 retains this legacy protocol while introducing a new protocol;
the existing adapter remains authoritative for transport compatibility. See the
[4.1 protocol note](https://github.com/transmission/transmission/blob/4.1.0/docs/rpc-spec.md).

Tracker health is independent of torrent progress:

- A recent successful announce is healthy. Missing fields, old results, timeouts,
  generic transport/HTTP errors and unknown messages remain unknown; they are
  not invented tracker failures or reasons for routine human intervention.
- Narrow explicit tracker rejection messages are confirmed errors. Unregistered
  classification requires an explicit unregistered/not-found response from every
  current non-backup tracker; mixed success/error results are not record-wide
  unregistration. Scrape errors alone are never registration evidence.
- Repeated unregistration requires at least three distinct consistent announce
  events, at least 15 minutes apart. Reloading the page or repeatedly collecting
  the same announce does not increase the count. Changed tracker sets, recovery,
  unknown telemetry, seed-counter regression, or long observation gaps reset it.
- The daemon's actual `secondsSeeding` is the only seed-time source. Missing,
  negative or invalid counters mean unknown/protected. Neither torrent creation
  dates nor download completion age count toward the 30-day minimum.
- Incomplete paused downloads are explicit observations. This collector does not
  resume them, reannounce them, or infer why they are paused.

## Retirement Boundary

No retirement handler is exposed or enabled, even with repeated unregistration
and at least 30 days actual seed time. Those facts do not prove current tracker
ratio/time rules, other clients/cross-seeds, active application writers, all shared
canonical/file references, or independently surviving seed copies. The existing
`removeRecord` primitive alone cannot establish those obligations. An autonomous
retirement extension must prove these at execution time and fence changed
bindings/obligations through a durable intent/outbox before issuing a narrowly
scoped record-only removal. It must never infer permission to delete seed data.
Until then, the system continuously observes and retains records, without
creating a human verification chore for every error.
