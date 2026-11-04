# Private Torrent Stewardship

Stewardship observes only Podible's own private Transmission instance and durable
book/movie/TV bindings. The collection/status services never control torrents.
The background job additionally considers narrowly fenced, unused orphan records
for record-only retirement; canonical and seed files are never deleted by it.
Status reads stored observations and current bindings without querying the
daemon, writing state, initializing settings, or collecting on page load.

## Contract And Wiring

`stewardshipRouter` is registered under `stewardship` in JSON-RPC and typed
tRPC. The admin panel is available at `/stewardship` ("Torrent health"):

- `stewardship.status` is an ADMIN query. Input is
  `{source?: "all" | "books" | "screen", offset?: number, limit?: number}`; defaults are
  all, 0, 25, with a maximum page size of 50. `screen` includes movies and TV.
- `stewardship.collect` is an ADMIN mutation with `{operationId: UUID}`. It reads
  the next batch of at most 50 binding rows and returns a durable collection
  receipt. Replaying the UUID returns the same receipt without another query.
- Status returns `mode: "observe-only"`, `destructiveActionsEnabled: false`,
  `minimumSeedSeconds: 2592000`, pagination/coverage, last collection, and items.
  Each item includes media, a binding label, the latest sanitized observation,
  staleness, `retirementAllowed: false`, and the retirement boundary.

The shared torrent registry has one row per client/native ID. Domain associations
remain separate, and collection provenance retains its stable book/screen keys
so existing observation digests do not change during migration. Combined status
uses a single bounded inventory offset, with no duplicated records at the
producer boundary. Title metadata is resolved in batches. Status and retirement
previews expose human titles/links separately from diagnostic binding labels.

The UI lists confirmed problems, separately discloses unknown/stale observations,
and summarizes healthy/protected records. Counts are explicitly page-scoped;
unseen pages are not claimed healthy. Routine protection is not an action item.

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
  dates nor download completion age count toward the 30-day seed protection.
  This protection remains in force for live/unknown tracker obligations and
  seed-file eviction; confirmed-unregistered orphan record-only retirement below
  is a separate policy, not a seed-data eviction authorization.
- Incomplete paused downloads are explicit observations. This collector does not
  resume them, reannounce them, or infer why they are paused.

## Retirement Boundary

The normal five-minute job can retire only complete **orphan** records with no
domain binding or admitted consumer, after every current tracker (including
backups) supplies three individually advanced unregistered announcements at
least 15 minutes apart. No minimum seed duration applies to this record-only
action: a confirmed-unregistered torrent's actual seed counter can freeze at zero.
The counter must still be known and nonregressing. Generic HTTP errors, timeouts,
scrape errors, live/unknown backups and a primary-only failure are insufficient.

Durable admission covers asynchronous book/movie/TV producers and binding
writes. Retirement holds the global worker lane, persists intent before its
conditional daemon command, retains a permanent re-add tombstone, and reconciles
uncertain acknowledgement without widening the operation. Pending claims fence
configuration changes. Record removal is never an HTTP mutation and never
requests local-data deletion. The UI exposes safe evidence and protection
reasons, not a queue of routine human approvals.

Library-bound records remain protected even when trackers reject them. Shared
seed-data eviction and broader release stewardship still need their own proven
reference/obligation policies. See [orphan retirement](torrent-retirement.md).
