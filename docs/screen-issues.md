# Movie and TV issue reports

Signed-in users can report a problem from a shared library title or a title
with their own active request. TV reports can point to a provider-stable
episode belonging to the exact local show, including a verified special.
Title labels and report ownership come from Podible, not the form. Descriptions
are limited to 1,000 characters. Administrators can read them; don't include
passwords or access tokens.

`issues.create` saves the report, a point-in-time first check, and a durable
diagnosis job in the same SQLite transaction. The job is deduplicated per
saved report ID and its payload is only `{ "reportId": 123 }`. The initial
check reads the local active publication projection for the exact
title/episode. If there is no active publication, it can also report an earlier
recorded file loss tied to the reporter's own active request. Neither a
publication record nor its absence establishes whether playback works now.
Filing does not wait for diagnosis, call Plex or a model, change a request,
or touch any files.

`issues.listMine` is owner-scoped with optional title filtering; saved reports
remain readable even if the title later leaves the library. The administrator's
read-only `issues.inbox` is the manual review path. Both are bounded, cursor-based
RPCs and omit reporter IDs and private diagnostic details.

The worker runs diagnosis automatically. It reuses the existing agent
configuration and rechecks the exact report scope
using the same local publication check as filing. The model sees only the
report category, movie/show/episode scope, and the filed and current finding
codes. It does not see titles, descriptions, IDs, paths, credentials, or logs.
A show-level publication finding means at least one episode is recorded, not
that the show is complete. Neither finding verifies current files or Plex.

Diagnosis uses a single-concurrency workflow with one tool-free model call
per attempt (up to 15 seconds). A recorded assessment is reused if the job
replays. An interruption before the assessment is saved may repeat the call.
Provider errors or missing configuration fail the job without exposing raw
provider details. There is no automatic retry on failure.
`issues.inbox` includes that assessment (or `null`); `issues.listMine` does not.
An administrator can call `issues.diagnose` to get the existing assessment or
job, or enqueue diagnosis for an older report. This call never contacts the
model. Failed jobs can be explicitly retried through the existing jobs API.

Podible validates the assessment's focus against the evidence and maps it to
a fixed check, not a command. `preview-recovery` points
administrators to the existing **Restore missing media** page or the read-only
`recovery.preview` RPC once they have identified the matching target. That
workflow independently checks whether repairing links or retrieving missing
media is supported and requires separate confirmation before any change.
The other recommendations are manual checks of Plex playback, tracks, or
content identity, or generic manual review. No recommendation runs on its own.
All reports remain open; there is no new repair or resolution workflow, inbox
web UI, notification delivery, or Seerr integration. Book reports are unchanged.
