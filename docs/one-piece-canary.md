# One Piece Production Canary

## Authorized Scope

Use normal user TV requests and normal CD. Independently acquire all regular aired
One Piece episodes, preserving full season intent; specials require a separate
explicit selection. No Sonarr mutation, manual deployment, stopped workers, or
periodic per-title searches.

## Started November 5, 2026

- Actual production build: `0c51f7c826e9ffe89e9bd11e20f66f3128fd7750`, schema 55.
- No existing One Piece entry/request was found before creation.
- Normal `tv.details` resolved TMDB `37854` to TVDB `81797`, with sourced Sonarr
  episode identities, absolute numbers, and TVDB aired coordinates.
- Normal `tv.request` created request **5**, target **5**, acquisition **15**, job
  **8756** at **08:31:13 UTC**. All regular seasons **1-23** are retained: **1,179
  known aired episodes**, no future/undated episodes at this snapshot.
- Specials season 0 contains 63 entries and was not selected. Absolute episode
  **590** is explicitly mapped to **S00E39**, TVDB episode `4543896`, "Dream 9
  Toriko & One Piece & Dragon Ball Z Cho Collaboration Special!! (2/2)". That is
  a provider classification, not a dropped or guessed regular-episode mapping.
- Temporary authentication stayed server-side on Cyprus and was revoked after
  each API inspection/request. No credentials were transferred to the workstation.

## First Observed Blocker

The initial selector failed with HTTP 400 "input exceeds the context window";
job 8756 exhausted five attempts by **08:31:42 UTC** without adding a torrent.
The full catalog was serialized both inside the work and as missing episodes.

The focused fix bounds each new acquisition snapshot to at most 64 missing aired
episodes in one provider season and removes the duplicate whole-series catalog
from selection/import agent inputs. Request membership is unchanged. Successful
publication queues the next missing batch, first trying already completed packs
through the existing proven-source reuse path. Existing durable snapshots retain
their receipts; retry through the normal TV API creates a fresh bounded execution.
The existing 300-file and 40 GiB torrent safety limits remain unchanged.

Regression coverage includes a 1,200-entry catalog excluded from agent input and
a 130-episode request publishing **64, 65, 129, 130** episodes across four batches,
with one pack download, later owned-pack reuse, and 130 exact canonical views.

## Completion Verification Gate

After normal CD and a normal request retry, inspect every completed version:

1. The selected file is a regular canonical object under the configured content
   root, with the exact persisted byte size and no symlink traversal.
2. Its declarative Plex view is a symlink resolving to that exact object.
3. Read-only Plex inspection finds the exact TVDB series and episode identifiers
   at the exact server paths in isolated TV section 16. Matching ordinals alone
   or a completed torrent is not sufficient.
4. Report active verified coverage separately from downloaded/unpublished bytes
   and all remaining requested episodes. Do not claim the whole show is complete.

At the first failure snapshot, **0 torrents, 0 active episodes**: no completed
One Piece files existed to verify.

## Normal CD And First Download

- Fix commit `7ed955dd7c344a25e5714f1ec36bb30fb18f1689` passed **1,356 tests**,
  typechecking, frontend build, DB-type/runtime-entrypoint checks, and local TV
  Chrome acceptance. Normal CD **41996037284** then passed every gate.
- At **08:44 UTC**, the running container's `GIT_COMMIT_SHA` independently
  confirmed that exact build. No manual deployment occurred.
- Normal `tv.retry({requestId:5})` returned job **8825**, acquisition **16**, at
  **08:44:46 UTC**, preserving all 1,179 requested episodes. The durable snapshot
  contains the eight requested season-one episode IDs, not the entire show.
- Autonomous selection inspected and chose BeyondHD's **One Piece S01 REPACK2
  1080p CR WEB-DL Dual-Audio DDP 2.0 H.264-Kitsune**. Its stated evidence was eight
  filenames matching requested episode titles. This is selection evidence, not
  independent proof of imported content or Plex publication.
- Private Transmission torrent **166**, hash
  `4e71ef2c0f8bd63b44cfa7be21183b112c7ddce4`, started at **08:45:06 UTC** in
  owned TV layout `acquisition-16-0`. At **08:47:16 UTC**, it had **8,519,726,253 /
  12,473,201,837 bytes**, state downloading, no tracker/client error.
- At that observation **0 episodes were active/published**. Independent completed
  file and exact Plex checks remain pending; do not equate these downloaded bytes
  or the selector's explanation with availability.
- At **08:50:15 UTC**, the complete pack was seeding with all **12,473,201,837
  bytes** present and no client error. The import plan mapped all eight filenames
  and approximately 25-minute playable streams to S01E01-E08. Canonical adoption
  subsequently progressed through file receipts `adopt:0` and `adopt:1` by
  **08:56 UTC**; this is protected byte-copy/hashing work, not a stalled download
  or another search. No active episode/Plex success has been claimed yet.
- At **09:02:31 UTC**, an independent, fresh GET of SkyHook's full TVDB 81797
  catalog agreed with the local request: **1,179 regular episodes**, with no
  missing regular identities. This checks that stale local Sonarr data did not
  silently truncate the known aired scope.
- By **09:04:23 UTC**, **six of eight** protected canonical adoption receipts
  were complete and the seventh was running, without an execution error.

## First Completed Season: Independently Verified

Publication began **09:08:22 UTC**, all eight episodes became active at
**09:09:10 UTC**, and job **8825 succeeded**. At **09:09:22 UTC**, a separate
read-only inspection independently verified **8 canonical files and 8 symlinks**:
regular canonical objects, exact persisted byte sizes, no canonical symlink
traversal, and each Plex-view symlink resolving to that exact object.
`PlexTvLibrary.verify` then accepted all eight exact **TVDB episode IDs and server
paths** in isolated Plex section **16**; ordinal-only matches were not used.

The shared canonical root is `/media/MediaStorage/Media/podible-tv/objects/`.
The Plex-view root is `/media/MediaStorage/Media/podible-tv/views/plex/tv/`.
Every server path follows `One Piece (1999) {tvdb-81797}/Season 01/One Piece -
S01E<NN> - t5v15.mkv`. All probe durations are approximately **1,500 seconds**.

| Episode | TVDB Episode ID | Exact Bytes | Canonical Relative Path |
| --- | --- | --- | --- |
| S01E01 | 361887 | 1582587454 | `files/b4/b41e7a7e-8c6a-4c07-9e67-a1b1039df1af.mkv` |
| S01E02 | 361888 | 1579947042 | `files/d5/d555ed8c-8bde-4449-b8a0-33986fe854c2.mkv` |
| S01E03 | 361889 | 1471995134 | `files/02/024f3985-2802-40fe-b0a3-382d94e27a55.mkv` |
| S01E04 | 361890 | 1578812818 | `files/8c/8c4308b2-16c8-4915-9806-529ee616dfdc.mkv` |
| S01E05 | 361891 | 1578636952 | `files/43/43c0a6d4-7e5b-4ab9-aba8-6d63e6b24c82.mkv` |
| S01E06 | 361894 | 1577779292 | `files/78/7885d7fa-9581-428f-8d28-45f7fa2743a1.mkv` |
| S01E07 | 361895 | 1581436680 | `files/96/963d1d7e-e121-4db9-a5b6-dd48df53d052.mkv` |
| S01E08 | 361896 | 1522006465 | `files/fa/fab3fea5-7267-4434-9950-e154dd9da525.mkv` |

This is **8/1,179 verified episodes**, not whole-show completion. Normal durable
continuation created acquisition **17**, job **8924**, at **09:09:10 UTC** for
the remaining **1,171** episodes. The completed season-one source was correctly
skipped as fully accounted before searching for the next missing batch. No manual
new title searches or separate requests were issued to force continuation.

At **09:13:05 UTC**, continuation had selected BeyondHD's **One Piece S02
REPACK2 1080p CR WEB-DL Dual-Audio DDP 2.0 H.264-Kitsune** after inspecting its
22 filenames against the requested titles. Torrent **167**, hash
`50f425010d3a3c72a66721e3fca5b66ce47abdfe`, was downloading in owned layout
`acquisition-17-0`, with **11,834,386,434 / 34,639,702,018 bytes** observed and no
client error. Its episodes were not yet imported or claimed verified; the eight
independently verified season-one episodes remained active.

### Newly Exposed Restart Blocker

At **09:15:28 UTC**, after normal CD advanced the running build to parent core
commit `5cc04e4d71be581cfcf0c0965c5fed85b1d995cc`, a fresh independent check
still accepted all eight season-one canonical files/symlinks and exact Plex
episode IDs/paths. However, continuation job **8924** had failed with **"Refusing
to control an unmanaged torrent"**. Do not describe this continuation as healthy.

Read-only raw Transmission inspection established that S02's record lost its
`Podible TV` label and owned download directory: it was stopped at the default
`/home/bun/Downloads`, reporting zero retained bytes. The previously downloaded
owned-layout data was not removed. S01 retained its correct ownership label,
directory and completed state. S02 had a persisted `.torrent` file but **no
`.resume` file**, whereas S01 had both.

At the failure, the container's stop timeout was unset. The supervisor allowed up to
10 seconds for application exit *before signalling the daemon*, followed by a
separate 10-second daemon timeout. Docker's default grace period can therefore
kill the daemon before its recent resume state is flushed. This is a concrete
shutdown-risk diagnosis, not permission to blindly relabel/control an unknown
torrent. The parent was notified through progress messages; no separate
supervisor owner had been assigned. Shared-runtime ownership must be coordinated
before implementing the tested fix and provenance-safe recovery. No manual
torrent control, worker stop, or deployment was issued during this diagnosis.

At **09:20:36 UTC**, live build `6487485f51c6ea0e637df8fd38cf39df3c6a6963`
still had **eight active season-one episodes** and the same failed continuation
job 8924. S02 remained stopped/unlabelled at the default directory. A resume file
now existed after the subsequent restart; the original absence is historical
evidence, not a claim that it remains absent forever. S01 retained its correct
label/path and complete seeding state. The observation establishes one lost
recent active-torrent record, not that every daemon restart strips all labels.

### Exact-Record Repair Tool

`scripts/repair-torrent-resume.ts` is a one-off, dry-run-by-default tool pinned to
request 5, target 5, acquisition 17, job 8924, torrent 167, and S02's exact hash.
It is not a new generic adoption, automatic recovery, or client-owned RPC surface.

```sh
bun scripts/repair-torrent-resume.ts --dry-run
bun scripts/repair-torrent-resume.ts --apply --require-build <FULL_RUNNING_SHA>
```

Only apply after normal CD deploys the tested supervisor shutdown fix and this
tool. The supplied full SHA must match the running container. Preconditions join
the exact registry ownership/location, original acquisition/job/run, persisted
snatch/location/metainfo receipts, on-disk engine metainfo, live file manifest,
retained regular files under the real configured owned directory, and historical
positive owned download progress. The failed job must be quiescent, the request
retained, and no competing acquisition/pending job command may be active.

An unlabelled stopped record at the private engine's default directory qualifies
only when it has zero known valid/unchecked data and the default directory has
no conflicting torrent files. Resume metadata must be absent or explicitly
confirm the same stopped/default-directory failure and torrent name. A newly
written damaged resume is not mistaken for an ongoing absence.

Apply durably records intent, fences retirement through existing admission,
points the exact existing record at the proven owned directory with `move:false`,
and restores only its proven `Podible TV` label. It then uses the existing adapter
to recheck/resume, requires recovered hash-valid pieces and new progress, and
retries **the original job** through authenticated server-local `jobs.retry`.
No data deletion, torrent removal/addition, file moves/copies, new acquisition,
title search or weakened global ownership guard is involved. Partial repair can
resume only with its matching durable intent. Credentials stay server-side.

Guard tests use real file-backed application/runner databases and source files,
with a synthetic daemon protocol; the actual Transmission adapter enforces the
restored ownership path. The repair suite passes **13/13**, the combined TV suite
passes **37/37**, typechecking passes, and normal TV Chrome acceptance passes.
The parent separately owns shutdown fix `3d40a1c` and reported a passing real
daemon immediate-stop/restart test. Production dry-run/apply results remain to
be recorded after normal CD deploys the repair script with that fix.

## Storage Observation For Reuse Work

Read-only `statfs` and `stat -f` on both configured movie and TV managed roots
reported the same device **2305**, **4096-byte blocks**, ext-family magic
**0xef53**. Host `findmnt --target` independently identified the mount as
**ext4**. Do not infer native reflink support from this observation: writable
cross-seeding variants must explicitly establish the required capability or
remain unsupported, rather than silently allocating another complete copy.
