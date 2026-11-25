# Aired Episode Recovery: November 24 Read-Only Audit

Production build `bcbc607` was inspected through read-only/query-only connections
to `/config/podible.sqlite` and `/config/execution.sqlite`. Only persisted
metadata, derived counts and allowlisted failure categories were returned. No
provider searches, requests, downloads, settings changes, worker stops, deploys
or production writes were performed.

## Persisted Coverage And Causes

| Target | Requested / selected | Aired missing | Last relevant work |
| --- | --- | --- | --- |
| Tom and Jerry, target/request 2284 | 161 / 114 | 47, season 1960 | acquisition 51 / job 20500 |
| Homeland, target/request 26287 | 48 / 36 | 12, season 7 | acquisition 167 / job 80730; recovery 172 / 80937 and 173 / 80938 |

All acquisitions for both targets have `finished_at`; their current Reflow runs
are completed, with no pending acquisition wake. Completed execution does not
mean episode publication. Coverage above is persisted active-episode selection,
not a fresh filesystem or Plex audit.

**Tom and Jerry:** acquisition 51 tried a 47-member playable pack which stalled,
then completed another 47-member playable pack. Its import plan mapped no files:
sampled probe facts were 714-718 by 572-576 DVD-resolution video, below the stored
720/1080 HDTV/WEB/BluRay policy. A later sibling-source inspection also produced
an empty plan. The last search contained 249 offers with zero warnings or
deferred providers and selected no new candidate. Earlier provider deferrals
had been retried. This is a policy/verified-match stop, not proof that no release
exists, and not evidence for weakening quality or repeatedly searching the title.

**Homeland:** acquisition 167 selected three season packs, each with 12 playable
metainfo members. All three saved completion sequences went from `checking` to
`inactive`, with zero bytes downloaded, and ended in retained-stalled receipts.
This is consistent with the initial-check start defect already fixed by
`52c3c02` on November 15 UTC, after these November 14 jobs. It is not evidence
that peers were unavailable. That fix is already in the audited production
build; completed historical jobs do not automatically restart after deployment.

The stalled-pack handoff created two six-episode recovery batches. Acquisition
172 downloaded three complete releases whose manifests had 36/21/32 members,
including 34/19/30 RAR parts and **zero playable video members**. Acquisition 173
stalled on another playable pack, then completed two releases with 29/22 members,
27/20 RAR parts and again zero playable members. These five archive-only
downloads produced zero probes, empty import plans and no versions. Provider
deferral during acquisition 173 was retried successfully before this terminal
outcome. The later successful acquisition 187 / job 104565 covered season 3,
not season 7.

## Generic Fix

TV import currently consumes exact playable metainfo members; archive extraction
is not supported. Accepting archive-only torrents for download spent bandwidth
and bounded alternatives on a shape the application could never publish.

- A shared TV metainfo check requires at least one nonempty supported video
  member. A pack containing playable video plus archive sidecars remains allowed.
- Inspected candidates expose `no-playable-video` in both manifest and release
  listing evidence. Primary validation and optional-copy filtering enforce that
  blocker, including the existing bounded model-correction flow.
- Acquisition independently checks fetched metainfo before any client
  registration or download. Unsupported candidates are excluded by offer key
  and info hash for the remaining bounded alternatives.
- New target snapshots enable admission explicitly. Old cached snapshots keep
  their prior checkpoint sequence; current receipt/source identity checks are
  unchanged. Failed-job replay reconstructs exclusions from cached selections
  and metainfo, without refetching the rejected archive or snatching it.

No recurring per-title search, archive extraction, policy downgrade or implicit
historical-job retry is added. Genuine no-match work still waits for feed
evidence or explicit operator/user retry. The fix prevents unsupported work; it
does not establish that either missing season is now obtainable or recovered.

## Focused Verification

`bun test test/unit/tv-agents.test.ts test/integration/tv-acquisition.test.ts
--test-name-pattern 'archive-only|mixed playable'` passes four regressions:
visible/enforced selection blockers, mixed-pack eligibility, durable failed-job
retry without archive snatch, and bounded no-match without recovery fan-out.
The three archive-only regressions failed before the implementation. Aggregate
integration gates, build and final typecheck belong to the parent integration.
