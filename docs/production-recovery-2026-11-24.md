# Production Failure Follow-Through

## AI Routing Correction

The November 21 status report incorrectly treated cumulative historical TV
Decisions fallbacks as current traffic. TV feed routing already defaults to
single-shot Responses. The held-out comparisons in `tv-feed-routing-eval.md`
favored that path on token cost and recall. The fix adds an explicit build-policy
metric and labels the Decisions counters as historical, Decisions-only totals;
it does not switch the TV matcher back to Decisions or reset its history.

## Monitoring

Prometheus was stopped with exit 255 at the November 18 host/Docker restart.
It was not OOM-killed and no application panic was retained. The specific reason
Docker did not restore this container is not established by the retained logs.

On November 24 it was started without restarting Podible, Plex, or downloads.
Readiness and `up{job="podible"}=1` were verified, along with fresh collection
timestamps and named BroadcasTheNet/MyAnonamouse tracker samples. The Compose
service and running container now use `restart: always`, so a saved stopped
state will not suppress restoration on the next daemon restart. The previous
Compose file remains in a server-local rollback copy. Lost scrapes are not
reconstructed or presented as observed history.

## Cross-Seed Receipt Relocation

Three new failed reuse jobs (166577, 166600, 169182) all failed while reading
completed source inventory for Homeland. Eight source registrations retained
legacy file receipts with absolute `sourceRoot` paths under `podible-tv`, even
though their current, sealed reservations and files had moved to `podible/tv`.
No missing payload bytes were established.

New version-2 finalization receipts bind the content-root owner, object UUID,
relative path, size, inode/device and timestamps, not the original torrent view
or absolute root. A finalized object can outlive that view. Legacy receipts can
use an explicitly supplied current reservation only if its validated ownership,
sealed state, object identity, size and inode/device agree; the actual file's
timestamps must still agree. Readers do not guess old/new paths or rewrite
receipts, and no payload is read or hashed.

Regression coverage moves a real test storage tree, removes the original view
and reservation, exercises legacy pre-move inventory, and rejects changed
ownership, object identity, sizes and inodes. Pack retirement passes the current
reservation as well; its seed-time, live Plex and deletion fences are unchanged.

## Provider and TV Findings

MAM's current Prowlarr warning masks an underlying 403 with an expired or invalid
login cookie and a not-signed-in response. Replacing the cookie and passing the
indexer test is required; a cooldown expiry cannot repair authentication. No
credential was exported or logged.

See `tv-aired-recovery.md` for the distinct Tom and Jerry quality-policy stop and
Homeland archive-only import failure. See `tv-shadow.md` for the reproduced WAL
snapshot upgrade race and bounded handling of moving Sonarr history pages.
Autobrr's webhook is configured, but FileList's direct origin was not trusted
and BTN is denied entry to its invite-only announce channel. RSS stays enabled
for catch-up; no periodic per-title search is introduced.
