# Sharing and transfer metrics

Activity (`/ops`, administrators only) shows sharing totals, transfer speed,
historical charts, tracker totals, reuse outcomes and current downloads. Books,
movies and TV all come from the same `media_torrents` registry and collector.
Opening the page or scraping metrics never queries or controls the daemon.
The separate library inventory read stats registered canonical files, with eight
concurrent inspections, never a recursive directory walk.

## Collection and retention

- A durable `torrent_metrics_collect` job samples the private Transmission client
  every minute, in batches of 100, bounded at 10,000 registrations per collection.
- Exact client identity, hash, ownership label and download directory are checked
  before counting telemetry. Book-specific ownership markers are checked too.
- Missing, stale, foreign or incomplete observations are unknown, not zero. A
  current aggregate requires a verified reading for every registered torrent.
  Charts break on unknown readings or gaps exceeding three sample intervals.
- SQLite migration 56 stores sanitized per-registration counters and snapshots.
  Minute samples last 24 hours; latest-sample-per-hour rollups last 90 days.
  Hourly speed points are sampled values, not hourly averages or maxima.
- Upload/download and seed-time counters accumulate observed deltas across client
  counter resets, and survive registry retirement. Initial values include earlier
  client lifetime activity; dates before instrumentation are not reconstructed.
- No existing torrents are rechecked, restarted, migrated or copied for metrics.

## Reading the numbers

**Logical seed size** sums complete actively seeding torrent sizes. The same
canonical payload can count multiple times. It is not physical disk usage.
Completion requires all torrent bytes verified, not just wanted-file progress.

**Seeding copies** counts active associations per observed owned content target.
Audio and ebook are separate book slots. A TV target is not an individual episode
or a payload file. Paused complete registrations are not seeding copies.

**Verified payload reused** records bytes already verified when a successful reuse
operation began. This is avoided payload acquisition, not a measurement of network
protocol overhead or physical disk savings. Sidecar and content-repair figures are
missing payload needed at that point, not retransmission-inclusive wire counters.
Old receipts without that accounting show unknown, not zero. Multi-file TV reuse
is shown separately; it is not labeled a proven complete-season quality upgrade.

**Reuse success** is successful completed reuse operations divided by terminal
attempts, including skipped/unsupported/failed attempts. Pending attempts are
excluded. This is not the proportion of the entire indexer catalog that matches.

**Cross-seed upload** is upload reported by cross-seeded registrations. We cannot
measure how much upload would otherwise have been missed. Likewise the client
ratio is not the tracker's credited account ratio, freeleech or bonus accounting.

**Tracker totals** use hostname-to-name mappings learned from the announce and
announce-list fields of metainfo fetched from a known indexer. This runs for book,
movie, TV and reuse acquisitions; the collector gradually backfills the retained
local metainfo cache without new network requests. Hosts sharing one name are grouped.
There is no manual naming UI or mutation API; old manual overrides are ignored.
Conflicting provider evidence leaves the hostname unnamed instead of guessing.
Only hostnames are persisted, never announce paths, passkeys or raw tracker error
text. Upload/download attribution is unknown for multi-tracker or changed-tracker
registrations rather than assigning their full counters to each tracker. Seed
size and seed time can overlap between tracker totals. These are association
totals, not proof that a tracker credited the corresponding minimum seed time.
Historical group identity is the saved grouping at sample time; regrouping does
not rewrite old samples.

## API and Prometheus

Typed tRPC and existing JSON-RPC share the same admin methods:

```ts
transferMetrics.status({ hours?: 24 | 168 | 720 | 2160 })
transferMetrics.library({}) // published counts and canonical file allocation
```

## Dashboard and library inventory

The dashboard separates published library size from logical seeding, traffic,
and reuse. Books, eBooks, audiobooks, movies, TV shows, seasons and episodes count
available published units, not wanted catalog entries. A season with any available
episode counts once. Multiple manifestations count once per book/format. Canonical
files deduplicate by device/inode across hardlinks and multi-episode files, both
per category and globally. Category rollups overlap and are not additive.

File size is the actual logical file length; on-disk size is `stat.blocks * 512`,
not an exclusive physical-block or reclaimable-byte measurement. Sparse allocation
can differ from logical size; reflink-shared extents cannot be measured exclusively
from stat. Pending downloads, obsolete versions, torrent sidecars and derived
caches are outside this published-library inventory. Missing or unreadable files
produce incomplete totals, never fabricated zero storage or healthy counts.

Tracker charts show multiple series on one shared scale with clickable legends,
numeric axes, a pointer/keyboard sample inspector and an optional vertical zoom.
Unchanged samples are explicitly labelled; unavailable historic speed observations
are gaps, not reconstructed zero rates. Tracker rates start with new collections.
Historical snapshots retain the grouping and attribution known at sample time.

`GET /metrics` exposes Prometheus text format using the existing admin session
authentication (Bearer app token or browser cookie). Anonymous requests return
401, non-admins 403. Responses are not browser-cached. No public monitoring port
or separate credential setting is introduced. Keep the session token in the
scraper's secret file, not this repository or a dashboard URL.

```yaml
scrape_configs:
  - job_name: podible
    scrape_interval: 60s
    scheme: https
    metrics_path: /metrics
    static_configs:
      - targets: [podible.example.com]
    authorization:
      type: Bearer
      credentials_file: /run/secrets/podible-admin-app-token
```

Grafana can use the existing Prometheus data source. Suggested panels:

| Panel | PromQL | Unit |
| --- | --- | --- |
| Logical seed size | `podible_seed_bytes` | bytes |
| Seed size by tracker | `sum by (tracker) (podible_tracker_seed_bytes)` | bytes |
| Seeding registrations | `podible_seeding_torrents` | short |
| Seeding copies | `podible_seeding_copies_per_content_target` | short |
| Upload/download | `podible_upload_bytes_per_second`, `podible_download_bytes_per_second` | bytes/sec |
| Cross-seed upload growth | `increase(podible_cross_seed_uploaded_bytes_total[24h])` | bytes |
| Reused payload | `podible_reuse_verified_bytes_total` | bytes |
| Sidecar payload | `podible_reuse_sidecar_payload_bytes_total` | bytes |
| Combined seed time | `podible_seed_seconds_total / 86400` | days |
| Telemetry availability | `podible_transfer_metrics_fresh` | boolean |

Unknown metrics are omitted, not exported as zero. Tracker/name labels are bounded;
titles, user IDs, hashes, file paths and torrent IDs are never metric labels.
Counters use `_total`, and quantities use bytes/seconds following the
[Prometheus instrumentation guidance](https://prometheus.io/docs/practices/instrumentation/)
and [naming conventions](https://prometheus.io/docs/practices/naming/).

## Verification

- Unit/integration coverage includes secret stripping, exact ownership, actual
  byte completion, paused copies, reset-aware counters, retired registrations,
  unknown legacy accounting, retention bounds, durable execution and auth.
- Chrome acceptance uses synthetic persisted history to exercise real RPC,
  chart controls, tracker renaming, Prometheus auth and mobile layout. It does
  not claim synthetic observations are live tracker verification.
- `test/lab/transfer-metrics-inner.ts` exercises real stable Transmission with
  synthetic movie, TV and ebook webseed transfers. It checks all three primary
  registrations, excludes a paused torrent and preserves counters through a
  daemon outage. This is a required normal-CD acceptance step, with no external
  network or production configuration.
- Native charts distinguish observations from outages. Production growth begins
  after normal CD deploys this instrumentation; this change does not backfill
  invented historical data.
