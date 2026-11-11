# Movie and TV issue reports

Signed-in users can report a problem from a shared library title or a title
with their own active request. TV reports can point to a provider-stable
episode belonging to the exact local show, including a verified special.
Title labels and report ownership come from Podible, not the form. Descriptions
are limited to 1,000 characters. Administrators can read them; don't include
passwords or access tokens.

`issues.create` saves the report and a point-in-time first check together in
SQLite. That check reads the local active publication projection for the exact
title/episode. If there is no active publication, it can also report an earlier
recorded file loss tied to the reporter's own active request. Neither a
publication record nor its absence establishes whether playback works now.
The check does not read the description, call Plex or a model, change a
request, start a job, or touch any files.

`issues.listMine` is owner-scoped with optional title filtering; saved reports
remain readable even if the title later leaves the library. The administrator's
read-only `issues.inbox` is the manual review path. Both are bounded, cursor-based
RPCs and omit reporter IDs and private diagnostic details. All reports remain
open in this slice: there is no repair action, resolution workflow, notification
delivery, or Seerr integration. The existing book report is unchanged.
