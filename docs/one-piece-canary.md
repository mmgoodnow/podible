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
One Piece files existed to verify. Rollout, retry, and first publication remain
to be checked and recorded below.
