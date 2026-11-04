import { useState } from "react";
import type { z } from "zod";
import {
  tvRetentionAuditSchema, tvRetentionEvictResultSchema,
  type TvRetentionAudit as Audit, type TvRetentionSeason,
} from "../../src/media/tv-retention-types";
import { rpc } from "./rpc";

type EvictInput = TvRetentionSeason & { expectedRevision: string; operationId: string; reason: string };
type EvictResult = z.output<typeof tvRetentionEvictResultSchema>;
export type TvRetentionClient = {
  audit(scope: TvRetentionSeason): Promise<Audit>;
  evict(command: EvictInput): Promise<EvictResult>;
};
const defaultClient: TvRetentionClient = {
  audit: async (scope) => tvRetentionAuditSchema.parse(await rpc.tvRetention.audit.query(scope)),
  evict: async (command) => tvRetentionEvictResultSchema.parse(await rpc.tvRetention.evict.mutate(command)),
};
const bytes = (value: number | null) => value === null ? "Unknown (not zero)" : `${value.toLocaleString()} bytes`;

/** Standalone admin panel; parent owns route/auth navigation and RPC registration. No timers or automatic calls. */
export function TvRetentionAudit({ client = defaultClient }: { client?: TvRetentionClient }) {
  const [target, setTarget] = useState("");
  const [seasonNumber, setSeasonNumber] = useState("1");
  const [audit, setAudit] = useState<Audit | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [command, setCommand] = useState<EvictInput | null>(null);
  const [result, setResult] = useState<EvictResult | null>(null);
  async function inspect() {
    setBusy(true); setError(null); setResult(null); setCommand(null);
    try { setAudit(await client.audit({ targetId: Number(target), scheme: "tvdb:aired", seasonNumber: Number(seasonNumber) })); }
    catch (error) { setError(error instanceof Error ? error.message : "TV season audit failed; no removal was requested."); setAudit(null); }
    finally { setBusy(false); }
  }
  async function evict(input: EvictInput) {
    setBusy(true); setError(null); setCommand(input);
    try { setResult(await client.evict(input)); }
    catch (error) { setError(error instanceof Error ? error.message : "TV eviction could not be acknowledged; inspect or retry the same operation."); }
    finally { setBusy(false); }
  }
  function reset() { setAudit(null); setResult(null); setCommand(null); setError(null); }
  return <>
    <h1>Whole-season TV retention</h1>
    <section className="panel">
      <p>Audit a complete season before explicitly removing its owned canonical copy. Only a target whose entire selected and requested coverage belongs to this season can be evicted. Partial seasons, multi-season targets, unknown playback, and unsafe seed copies stay protected.</p>
      <p className="muted">Audits do not delete, change demand, search titles or control torrents. Every exact episode needs global Plex history; a watched subset is not a watched season.</p>
      <form onSubmit={event => { event.preventDefault(); void inspect(); }} className="actions">
        <label>TV target ID <input type="number" min="1" required value={target} disabled={busy}
          onChange={event => { setTarget(event.target.value); reset(); }} /></label>
        <label>TVDB aired season <input type="number" min="0" required value={seasonNumber} disabled={busy}
          onChange={event => { setSeasonNumber(event.target.value); reset(); }} /></label>
        <button type="submit" disabled={busy || !target}>{busy ? "Checking season..." : "Audit entire season"}</button>
      </form>
    </section>
    {error && <p role="alert">{error}</p>}
    {error && command && <button className="secondary" disabled={busy} onClick={() => void evict(command)}>Retry same eviction operation</button>}
    {audit && <section className="panel" aria-label="Whole-season audit">
      <h2>{audit.title} / Season {audit.season.seasonNumber}</h2>
      <p>Whole-season membership: {audit.membership.state}. Entire season requested: {audit.membership.wholeSeasonRequested ? "Yes" : "No"}. Desired presence: {audit.desiredPresence}.</p>
      <p>Mount: {audit.mount.state}. Watch coverage: {audit.watch.coverage}. Playback: {audit.watch.activePlayback}.</p>
      <p>Verified allocation: {bytes(audit.verifiedReclaimableBytes)}. Eligible plan: {bytes(audit.planReclaimableBytes)}. Seed safety: {audit.seeding.state}.</p>
      <h3>{audit.eligible ? "Eligible whole-target eviction" : "Season removal blocked"}</h3>
      <ul aria-label="Season removal blockers">{[...audit.blockers, ...audit.executionBlockers].map(blocker =>
        <li key={blocker.code}><code>{blocker.code}</code>: {blocker.message}</li>)}</ul>
      {audit.safetyEligible && !audit.removalSupported && <p>Observed safety evidence passes, but safe execution is not integrated for this scope. No removal can be requested.</p>}
      {audit.eligible && <button disabled={busy || result?.status === "queued"} onClick={() => {
        if (window.confirm(`Evict all ${audit.membership.episodeIds.length} selected episodes of ${audit.title}, season ${audit.season.seasonNumber}? Canonical copies and their view links will be removed. Original requests, torrent registrations and verified independent seed files remain. Explicitly requesting again restores demand.`))
          void evict({ ...audit.season, expectedRevision: audit.revision, operationId: crypto.randomUUID(), reason: "Administrator explicitly requested verified whole-season target eviction" });
      }}>Evict complete single-season target</button>}
      <h3>Exact episode history</h3>
      <ul>{audit.watch.episodes.map(episode => <li key={episode.episodeId}><code>{episode.episodeKey}</code>: {episode.state} / Plays: {episode.playCount ?? "Unknown"}</li>)}</ul>
      <h3>Owned canonical files</h3>
      <ul>{audit.files.map(file => <li key={file.id}><code>{file.relativePath}</code>: {file.presence} / {file.references}
        {file.lost ? " / Recorded content loss; never eligible" : ""} / {bytes(file.verifiedReclaimableBytes)}</li>)}</ul>
    </section>}
    {result && <section className="panel" aria-label="TV eviction result">
      <h2>{result.status === "queued" ? "Durable eviction queued" : "Eviction blocked"}</h2>
      {result.jobId && <p>Job {result.jobId}. Queueing is not confirmation that the season has been removed. Inspect the operation before making another request.</p>}
      <ul>{result.blockers.map(blocker => <li key={blocker.code}>{blocker.message}</li>)}</ul>
    </section>}
  </>;
}
