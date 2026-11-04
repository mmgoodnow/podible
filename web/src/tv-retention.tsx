import { useEffect, useState } from "react";
import type { inferRouterOutputs } from "@trpc/server";
import type { AppRouter } from "../../src/trpc";
import type { z } from "zod";
import {
  tvRetentionAuditSchema, tvRetentionEvictResultSchema,
  type TvRetentionAudit as Audit, type TvRetentionSeason,
} from "../../src/media/tv-retention-types";
import { rpc } from "./rpc";

type EvictInput = TvRetentionSeason & { expectedRevision: string; operationId: string; reason: string };
type EvictResult = z.output<typeof tvRetentionEvictResultSchema>;
type Request = inferRouterOutputs<AppRouter>["tv"]["list"]["items"][number];
export function tvRetentionChoices(requests: Pick<Request, "targetId" | "entry" | "seasonNumbers">[]) {
  const choices = new Map<string, { key: string; scope: TvRetentionSeason; label: string }>();
  for (const request of requests) {
    const seasons = new Set(request.seasonNumbers);
    for (const seasonNumber of seasons) {
      const key = `${request.targetId}:${seasonNumber}`;
      if (!choices.has(key)) choices.set(key, { key, scope: { targetId: request.targetId, scheme: "tvdb:aired", seasonNumber },
        label: `${request.entry.title}${request.entry.year ? ` (${request.entry.year})` : ""} / Season ${seasonNumber}` });
    }
  }
  return [...choices.values()].sort((a,b) => a.label.localeCompare(b.label,undefined,{ numeric: true }));
}
export type TvRetentionClient = {
  list?(input: { limit: number }): Promise<{ items: Request[] }>;
  audit(scope: TvRetentionSeason): Promise<Audit>;
  evict(command: EvictInput): Promise<EvictResult>;
};
const defaultClient: TvRetentionClient = {
  list: input => rpc.tv.list.query(input),
  audit: async (scope) => tvRetentionAuditSchema.parse(await rpc.tvRetention.audit.query(scope)),
  evict: async (command) => tvRetentionEvictResultSchema.parse(await rpc.tvRetention.evict.mutate(command)),
};
const bytes = (value: number | null) => value === null ? "Unknown (not zero)" : `${value.toLocaleString()} bytes`;

/** One bounded title lookup; audits and removals require explicit actions. */
export function TvRetentionAudit({ client = defaultClient }: { client?: TvRetentionClient }) {
  const [requests, setRequests] = useState<Request[]>([]);
  const [loading, setLoading] = useState(true);
  const [listError, setListError] = useState<string | null>(null);
  const [reload, setReload] = useState(0);
  const [selection, setSelection] = useState("");
  useEffect(() => {
    let active = true;
    setLoading(true); setListError(null);
    void (client.list ?? defaultClient.list!)({ limit: 100 }).then(data => { if (active) setRequests(data.items); })
      .catch(error => { if (active) setListError(error instanceof Error ? error.message : "TV titles could not be loaded."); })
      .finally(() => { if (active) setLoading(false); });
    return () => { active = false; };
  },[client.list,reload]);
  const choices = tvRetentionChoices(requests);
  const selected = choices.find(choice => choice.key === selection);
  const [audit, setAudit] = useState<Audit | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [command, setCommand] = useState<EvictInput | null>(null);
  const [result, setResult] = useState<EvictResult | null>(null);
  async function inspect() {
    if (!selected || loading || listError) return;
    setBusy(true); setError(null); setResult(null); setCommand(null);
    try { setAudit(await client.audit(selected.scope)); }
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
      {loading && <p role="status">Loading TV titles...</p>}
      {listError && <p role="alert">{listError} <button disabled={busy} onClick={() => setReload(value => value + 1)}>Try loading titles again</button></p>}
      {!loading && !listError && !choices.length && <p>{requests.length ? "Requested season numbering is unavailable for these TV requests." : "No requested TV seasons are available to inspect."}</p>}
      {requests.length === 100 && <p>Showing seasons from the latest 100 requests.</p>}
      <form onSubmit={event => { event.preventDefault(); void inspect(); }} className="actions">
        <label style={{ minWidth: 0, flex: "1 1 18rem", maxWidth: "100%" }}>TV title and season <select required value={selection} disabled={busy || loading || !!listError}
          style={{ maxWidth: "100%", width: "100%" }} onChange={event => { setSelection(event.target.value); reset(); }}>
          <option value="">Choose a title and season</option>
          {choices.map(choice => <option key={choice.key} value={choice.key}>{choice.label}</option>)}
        </select></label>
        <button type="submit" disabled={busy || loading || !!listError || !selected}>{busy ? "Checking season..." : "Audit entire season"}</button>
      </form>
    </section>
    {error && <p role="alert">{error}</p>}
    {error && command && <button className="secondary" disabled={busy} onClick={() => void evict(command)}>Retry same eviction operation</button>}
    {audit && <section className="panel" aria-label="Whole-season audit">
      <h2>{audit.title} / Season {audit.season.seasonNumber}</h2>
      <p>Verified allocation: {bytes(audit.verifiedReclaimableBytes)}. Eligible plan: {bytes(audit.planReclaimableBytes)}. Seed safety: {audit.seeding.state}.</p>
      <h3>{audit.eligible ? "Eligible whole-target eviction" : "Season removal blocked"}</h3>
      <ul aria-label="Season removal blockers">{[...audit.blockers, ...audit.executionBlockers].map(blocker =>
        <li key={blocker.code}>{blocker.message}</li>)}</ul>
      {audit.safetyEligible && !audit.removalSupported && <p>Observed safety evidence passes, but safe execution is not integrated for this scope. No removal can be requested.</p>}
      {audit.eligible && <button disabled={busy || result?.status === "queued"} onClick={() => {
        if (window.confirm(`Evict all ${audit.membership.episodeIds.length} selected episodes of ${audit.title}, season ${audit.season.seasonNumber}? Canonical copies and their view links will be removed. Original requests, torrent registrations and verified independent seed files remain. Explicitly requesting again restores demand.`))
          void evict({ ...audit.season, expectedRevision: audit.revision, operationId: crypto.randomUUID(), reason: "Administrator explicitly requested verified whole-season target eviction" });
      }}>Evict complete single-season target</button>}
      <details><summary>Watch, file and operation details</summary>
      <p>Target {audit.season.targetId}. Whole-season membership: {audit.membership.state}. Entire season requested: {audit.membership.wholeSeasonRequested ? "Yes" : "No"}. Desired presence: {audit.desiredPresence}.</p>
      <p>Mount: {audit.mount.state}. Watch coverage: {audit.watch.coverage}. Playback: {audit.watch.activePlayback}.</p>
      <p>Audit revision: <code>{audit.revision}</code>.</p>
      {command && <p>Operation: <code>{command.operationId}</code>.</p>}
      <h3>Exact episode history</h3>
      <ul>{audit.watch.episodes.map(episode => <li key={episode.episodeId}><code>{episode.episodeKey}</code>: {episode.state} / Plays: {episode.playCount ?? "Unknown"}</li>)}</ul>
      <h3>Owned canonical files</h3>
      <ul>{audit.files.map(file => <li key={file.id}><code>{file.relativePath}</code>: {file.presence} / {file.references}
        {file.lost ? " / Recorded content loss; never eligible" : ""} / {bytes(file.verifiedReclaimableBytes)}</li>)}</ul>
      </details>
    </section>}
    {result && <section className="panel" aria-label="TV eviction result">
      <h2>{result.status === "queued" ? "Durable eviction queued" : "Eviction blocked"}</h2>
      {result.jobId && <p>Queueing is not confirmation that the season has been removed. Inspect the operation before making another request.</p>}
      <ul>{result.blockers.map(blocker => <li key={blocker.code}>{blocker.message}</li>)}</ul>
    </section>}
  </>;
}
