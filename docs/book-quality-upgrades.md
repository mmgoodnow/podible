# Book acquisition and upgrades

Books, movies and TV use the same private torrent client contract, torrent
registry, canonical storage/torrent views, cross-seed executor and transfer
metrics. Book download monitoring now consumes `TorrentSnapshot` directly rather
than translating private Transmission telemetry into rTorrent-shaped state.

## Shared execution

- `src/media/download-lifecycle.ts` owns completion checks, remaining bytes,
  progress observations and adaptive polling. Movie/TV acquisition and book
  monitoring consume the shared helpers where their semantics match.
- Private inactive book downloads can resume through the verified neutral
  adapter. Legacy rTorrent observations are translated only at the compatibility
  boundary; they are never adopted or controlled as private torrents.
- Existing durable step names and historical rTorrent checkpoint replay remain
  supported. No deployment-wide rechecks, torrent moves or transcript cache
  invalidation are involved.
- `src/media/quality-decision.ts` applies the same basic upgrade rule for books
  and screen media: satisfy identity/compatibility requirements, verify the
  candidate, and require an actual improvement. Equal quality does not churn.

## Book-specific quality facts

Audio and ebooks are independent slots. A translation, language, narrator,
dramatization, abridgement or materially different edition is not a quality tier.
It cannot replace the current edition just because its container ranks higher.

The `BookUpgradeReviewer` uses the existing hardcoded text-agent model and
existing agent credentials, timeout and edition preference. It receives the
book context and current/candidate format, duration, size, part and source
evidence. It records an evidence-based assessment, not private chain-of-thought.
Every semantic requirement must be compatible or explicitly not applicable;
unproven compatibility declines the automatic upgrade.

EPUB is usually a useful improvement over PDF for equivalent prose, while a
visual cookbook or layout-dependent edition may favor PDF. M4B may improve
navigation/handling over MP3, but transcoding is not an audio-quality improvement.
These are contextual agent judgments, not unconditional extension rankings.

## Staging and promotion

1. Fresh RSS candidates can target imported book slots when automatic upgrades
   are allowed and a reviewer is configured. Existing retained inbox evidence
   and semantic release selection are reused; there is no recurring title search.
2. Compare the selected release with the incumbent before snatching. A rejected,
   equal, incompatible or unknown candidate does not start a replacement.
3. Create an explicit staged manifestation, persist the incumbent fingerprint,
   ordered part count and preflight assessment, then use the normal private
   download/import pipeline. Staged manifestations are not preferred playback
   or ebook-download defaults.
4. Wait for every ordered part. Check source ownership, media, file presence,
   regular canonical paths, sizes and readability. Review the actual imported
   evidence with the agent again, not just the original release title.
5. Recheck physical identity across the awaited review and incumbent/manual
   selection inside the promotion transaction. Only then increase preference.
   The old manifestation and all its files remain intact.

Review unavailable or inconclusive means keep the incumbent, not manufacture a
manual approval queue or mark an unverified replacement successful. Intent and
assessment are persisted under `book-quality-upgrade:<manifestationId>`; selection
and import reasoning remain on their existing manifestation/asset fields too.

## Manual choice and repair

New explicit manual snatches establish a format-scoped replacement hold. RSS
does not silently replace that choice. Existing preferences are not rewritten
or inferred to have been manual during deployment.

Reporting a wrong file is a separate, explicit repair instruction. Its
server-created review job supplies the repair provenance; an ordinary failed
upgrade does not grant that authority. A repair can bypass the old manual hold
and need not be equivalent to an edition the user says is wrong. It still stages
the replacement, verifies all ordered parts, respects newer manual choices and
retains the original bytes. No public arbitrary repair-provenance input is added.

## Verification and remaining boundaries

Tests exercise the real Agents SDK loop with a fixture model provider (no paid
API calls), full RSS-to-download-to-import promotion, multipart waiting, PDF versus
EPUB context, compatibility uncertainty, manual/concurrent changes and filesystem
mutation. Existing media crash/restart tests and Chrome acceptance continue to
exercise the shared pipeline; the real stable Transmission lab covers private
book transfer/import and restart.

This is shared infrastructure, not one universal domain object. Books still use
manifestations/assets and ordered audio parts; movies use versions and TV maps
episodes/numbering. Their import/publication adapters remain domain-specific.
No multilingual identity redesign, forced format conversion, legacy torrent
migration, bulk reacquisition or automatic deletion is part of this change.
