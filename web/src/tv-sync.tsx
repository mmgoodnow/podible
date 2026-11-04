import { useState, type ReactNode } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { inferRouterOutputs } from "@trpc/server";
import type { AppRouter } from "../../src/trpc";
import { useTRPC } from "./rpc";
import { parseTvSeasonBackfill } from "./tv-sync-input";

type AuditCase = inferRouterOutputs<AppRouter>["tvShadow"]["get"]["case"];
type NativeRequest = inferRouterOutputs<AppRouter>["tv"]["get"];

function NativeRequestDetails({ item }: { item: NativeRequest }) {
  const trpc = useTRPC(), cache = useQueryClient();
  const [open, setOpen] = useState(false);
  const details = useQuery(trpc.tv.get.queryOptions({ requestId: item.requestId }, { enabled: open, refetchInterval: open ? 3000 : false }));
  const row = details.data ?? item;
  const retry = useMutation(trpc.tv.retry.mutationOptions({ onSuccess: async () => {
    await cache.invalidateQueries({ queryKey: trpc.tv.list.queryKey() });
    await cache.invalidateQueries({ queryKey: trpc.tv.get.queryKey({ requestId: item.requestId }) });
  } }));
  const busy = row.acquisitions.some((acquisition) => ["queued", "running"].includes(acquisition.status));
  return <details className="panel" open={open} onToggle={(event) => setOpen(event.currentTarget.open)}>
    <summary>#{item.requestId} {item.entry.title} · {item.availableEpisodes}/{item.requestedEpisodes} episodes available · {item.intentionallyAbsent ? "Intentionally absent" : item.withdrawn ? "Withdrawn" : item.events.at(-1)?.stage ?? "Requested"}</summary>
    {open && <>
      {details.error && <p role="alert">{details.error.message}</p>}
      <p>Native request #{row.requestId}; target #{row.targetId}{row.externalRequestId ? `; Seerr #${row.externalRequestId}` : ""}. <code>{row.entry.key}</code></p>
      <p><strong>{row.availableEpisodes} of {row.requestedEpisodes} requested episodes available</strong>. {row.futureEpisodes} future or not-yet-dated episodes.</p>
      {row.intentionallyAbsent && <p>This target was intentionally evicted. Re-requesting explicitly resumes acquisition; routine recovery remains paused.</p>}
      {row.withdrawn && <p>This request is withdrawn and cannot be retried.</p>}
      {!row.acquisitions.length && <p>No acquisition execution yet. Coverage, not the absence of errors, determines availability.</p>}
      {row.acquisitions.map((acquisition) => <div key={acquisition.id}>
        <p>Acquisition #{acquisition.id}: <strong>{acquisition.status}</strong> ({acquisition.reason}){acquisition.jobId ? `; job #${acquisition.jobId}` : ""}.</p>
        {acquisition.error && <p role="alert">{acquisition.error}</p>}
        {acquisition.decision != null && <details>
          <summary>Acquisition #{acquisition.id} selection and episode decisions</summary>
          <pre className="shadow-json">{JSON.stringify(acquisition.decision, null, 2)}</pre>
        </details>}
      </div>)}
      {!!row.events.length && <details>
        <summary>Acquisition events ({row.events.length})</summary>
        <ol className="timeline">{row.events.map((event, index) => <li key={`${event.createdAt}:${index}`}>
          <strong>{event.stage}</strong> <time dateTime={event.createdAt}>{new Date(event.createdAt).toLocaleString()}</time>
          <p>{event.message}</p>
        </li>)}</ol>
      </details>}
      <button disabled={row.withdrawn || busy || retry.isPending} onClick={() => retry.mutate({ requestId: row.requestId })}>
        {retry.isPending ? "Queuing..." : "Re-request seasons"}
      </button>
      {busy && <p role="status">An acquisition is already queued or running.</p>}
      {retry.isSuccess && <p role="status">Native recovery queued as job #{retry.data.jobId}. Availability is unchanged until publication succeeds.</p>}
      {retry.error && <p role="alert">{retry.error.message}</p>}
    </>}
  </details>;
}

function NativeTvAcquisitions() {
  const trpc = useTRPC(), cache = useQueryClient();
  const [ids, setIds] = useState("");
  const status = useQuery(trpc.tv.status.queryOptions({}, { refetchInterval: 5000 }));
  const requests = useQuery(trpc.tv.list.queryOptions({ limit: 25 }, { refetchInterval: 3000 }));
  const refresh = async () => {
    await cache.invalidateQueries({ queryKey: trpc.tv.status.queryKey() });
    await cache.invalidateQueries({ queryKey: trpc.tv.list.queryKey() });
  };
  const sync = useMutation(trpc.tv.sync.mutationOptions({ onSuccess: refresh }));
  const backfill = useMutation(trpc.tv.backfill.mutationOptions({ onSuccess: refresh }));
  const parsed = parseTvSeasonBackfill(ids);
  return <section className="panel" aria-label="Native TV acquisition">
    <h2>Independent TV acquisition</h2>
    <p>Explicitly approved Seerr seasons run through Podible's own release selection, download, episode verification and isolated Plex publication. All episodes in those seasons are requested, including future or not-yet-dated episodes. Sonarr remains separate. Existing requests are not backfilled automatically.</p>
    {status.isPending ? <p role="status">Loading native TV status...</p> : status.isError ? <p role="alert">{status.error.message}</p> : <>
      <p>Native connection: <strong>{status.data.configured ? "Configured" : "Incomplete"}</strong>. Isolated Plex section: {status.data.plexSectionId || "Not configured"}.</p>
      <p>{status.data.baselineId === null ? "Not activated. First sync establishes a no-history baseline." : `Automatic feed starts after Seerr request #${status.data.baselineId}.`}</p>
      <p>Last native sync: <strong>{status.data.lastSync?.status ?? "Not run"}</strong>{status.data.lastSync ? `; job #${status.data.lastSync.jobId}` : ""}. {status.data.cursor ? `Cursor: ${new Date(status.data.cursor).toLocaleString()}.` : ""}</p>
    </>}
    <button disabled={!status.data?.configured || sync.isPending} onClick={() => sync.mutate({})}>
      {sync.isPending ? "Queuing..." : "Sync new TV requests"}
    </button>
    {sync.isSuccess && <p role="status">Native request sync queued as job #{sync.data.jobId}; this is not an acquisition completion.</p>}
    {sync.error && <p role="alert">{sync.error.message}</p>}
    <details>
      <summary>Approved-season backfill</summary>
      <p>This starts real independent acquisition, not an audit. Select up to five Seerr TV requests. Each includes all episodes in its explicitly approved seasons; unapproved seasons are excluded and future episode intent is retained.</p>
      <form style={{ display: "flex", flexWrap: "wrap", alignItems: "end", gap: "1rem" }} onSubmit={(event) => {
        event.preventDefault();
        if (status.data?.configured && parsed.value && !backfill.isPending) backfill.mutate(parsed.value);
      }}>
        <label style={{ display: "grid", gap: "0.4rem", flex: "1 1 14rem", minWidth: 0 }}>
          Seerr TV request IDs
          <input value={ids} onChange={(event) => { setIds(event.target.value); backfill.reset(); }}
            placeholder="One to five IDs, separated by commas" autoComplete="off" aria-describedby="tv-approved-season-scope" />
        </label>
        <button type="submit" disabled={!status.data?.configured || !parsed.value || backfill.isPending}>
          {backfill.isPending ? "Queuing..." : "Acquire approved seasons"}
        </button>
      </form>
      <p id="tv-approved-season-scope">Enter one to five distinct Seerr request IDs. Approval is season-level only: no individual episode selection or episode cap.</p>
      {ids.trim() && parsed.error && <p role="alert">{parsed.error}</p>}
      {backfill.isSuccess && <p role="status">Explicit native backfill queued as job #{backfill.data.jobId}; follow actual episode coverage below.</p>}
      {backfill.error && <p role="alert">{backfill.error.message}</p>}
    </details>
    <h3>Native requests and episode coverage</h3>
    {requests.isPending ? <p role="status">Loading native requests...</p> : requests.isError ? <p role="alert">{requests.error.message}</p> : <>
      {!requests.data.items.length && <p>No independent TV requests yet. Comparison audit cases below are not native acquisitions.</p>}
      {requests.data.items.map((item) => <NativeRequestDetails item={item} key={item.requestId} />)}
    </>}
  </section>;
}
const verdict = (decision: NonNullable<AuditCase["result"]>["decision"]) => ({
  import: "Would import", reject: "Would reject", "needs-verification": "Evidence incomplete", unobservable: "Unobservable",
})[decision];

function AuditDetails({ item }: { item: AuditCase }) {
  const trpc = useTRPC(), cache = useQueryClient();
  const retry = useMutation(trpc.tvShadow.retry.mutationOptions({
    onSuccess: () => cache.invalidateQueries({ queryKey: trpc.tvShadow.list.queryKey() }),
  }));
  const wanted = item.input.entry.episodes.filter((episode) => item.input.episodeIds.includes(episode.id));
  return <div style={{ paddingTop: "0.75rem" }}>
    <p>{item.input.observation.releaseTitle}</p>
    {item.input.observation.sourceKey && <p>Source observation: <code>{item.input.observation.sourceKey}</code></p>}
    <p>Sonarr reported <strong>{item.input.observation.sonarr.outcome}</strong>. Independent review: <strong>{item.result ? verdict(item.result.decision) : item.status}</strong>.</p>
    <p>{item.result?.reason ?? "Evaluation has not produced a decision yet."}</p>
    <p>{item.input.files.length} candidate files; source: {item.input.source}. Captured {new Date(item.createdAt).toLocaleString()}.</p>
    {item.input.warnings.map((warning, index) => <p key={index}>{warning}</p>)}
    {item.error && <p role="alert">{item.error}</p>}
    {item.result?.files.map((file) => <div key={file.index}>
      <p><strong>{file.name}</strong> maps to episode IDs {file.episodeIds.join(", ")}: {file.quality.eligibility}.</p>
      <p>{file.reason}</p>
      {!!file.quality.reasons.length && <p>{file.quality.reasons.join("; ")}</p>}
    </div>)}
    {!!item.result?.unmappedEpisodeIds.length && <p>Unmapped requested episode IDs: {item.result.unmappedEpisodeIds.join(", ")}. Missing evidence is not a rejection.</p>}
    <details>
      <summary>Entry relationships and provider numbering</summary>
      <p><code>{item.input.entry.key}</code>{item.input.entry.anime ? " · Provider-classified anime" : ""}</p>
      {!item.input.entry.relations.length ? <p>No explicitly sourced relationships.</p> : <ul>
        {item.input.entry.relations.map((relation) => <li key={`${relation.key}:${relation.relation}`}>
          {relation.relation}: {relation.title} (<code>{relation.key}</code>). Source: {relation.source}.
        </li>)}
      </ul>}
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", textAlign: "left" }}>
          <caption style={{ textAlign: "left" }}>Requested episode identities and source-labelled mappings</caption>
          <thead><tr><th>Episode</th><th>Scheme</th><th>Season / episode / absolute</th></tr></thead>
          <tbody>{wanted.slice(0, 50).flatMap((episode) => episode.numbering.map((number) => <tr key={`${episode.id}:${number.scheme}`}>
            <td style={{ verticalAlign: "top" }}>{episode.title}<br /><code>{episode.key}</code> / internal {episode.id}</td>
            <td style={{ overflowWrap: "anywhere" }}>{number.scheme}<br /><small>{number.source}</small></td>
            <td>{number.season ?? "-"} / {number.episode ?? "-"} / {number.absolute ?? "-"}</td>
          </tr>))}</tbody>
        </table>
      </div>
      {wanted.length > 50 && <p>Showing 50 of {wanted.length} requested episode mappings; the full snapshot is below.</p>}
    </details>
    <details>
      <summary>Captured file maps and probe evidence</summary>
      <pre className="shadow-json">{JSON.stringify({ input: item.input, result: item.result }, null, 2)}</pre>
    </details>
    {!item.result && ["failed", "cancelled"].includes(item.status) && <button disabled={retry.isPending}
      onClick={() => retry.mutate({ caseId: item.id })}>{retry.isPending ? "Queuing..." : "Retry evaluation"}</button>}
    {retry.error && <p role="alert">{retry.error.message}</p>}
    {retry.isSuccess && <p role="status">Evaluation retry queued.</p>}
  </div>;
}

/** Native request coverage and read-only comparisons remain separate surfaces. */
export function TvSync({ children }: { children?: ReactNode } = {}) {
  const trpc = useTRPC(), cache = useQueryClient();
  const [cursor, setCursor] = useState<number>();
  const [includeHistory, setIncludeHistory] = useState(false);
  const status = useQuery(trpc.tvShadow.status.queryOptions({}, { refetchInterval: 5000 }));
  const cases = useQuery(trpc.tvShadow.list.queryOptions({ cursor, limit: 25, includeHistory }, { refetchInterval: 5000 }));
  const sync = useMutation(trpc.tvShadow.sync.mutationOptions({ onSuccess: async () => {
    setCursor(undefined);
    await cache.invalidateQueries({ queryKey: trpc.tvShadow.status.queryKey() });
    await cache.invalidateQueries({ queryKey: trpc.tvShadow.list.queryKey() });
  } }));
  return <>
    <h1>TV sync</h1>
    {children}
    <NativeTvAcquisitions />
    <section className="panel" aria-label="TV observation connection">
      <h2>Sonarr comparison audit</h2>
      <p>Read-only file and episode checks. This does not download, publish, or change Sonarr, and does not gate independent acquisition.</p>
      {status.isPending ? <p role="status">Loading connection state...</p> : status.isError ? <p role="alert">{status.error.message}</p> : <>
        <p>Sonarr connection: <strong>{status.data.sonarrConfigured ? "Configured" : "Missing"}</strong>. Agent: {status.data.agentConfigured ? "Configured" : "Missing"}. Path mappings: {status.data.pathMappingCount}.</p>
        <p>Last observation: <strong>{status.data.lastSync?.status ?? "Not run"}</strong>{status.data.lastSync ? ` (${new Date(status.data.lastSync.createdAt).toLocaleString()})` : ""}.</p>
        {status.data.lastSync?.error && <p role="alert">{status.data.lastSync.error}</p>}
        {!status.data.pathMappingCount && <p>No mapped roots: source files will be unobservable until server path mappings are configured.</p>}
        <p>{status.data.lookbackDays}-day history, up to {status.data.maxCasesPerSync} new cases per pass; polls every {status.data.pollIntervalMinutes} minutes.</p>
        <button disabled={!status.data.sonarrConfigured || sync.isPending} onClick={() => sync.mutate({})}>
          {sync.isPending ? "Queuing..." : "Observe Sonarr now"}
        </button>
      </>}
      {sync.isSuccess && <p role="status">Read-only observation queued.</p>}
      {sync.error && <p role="alert">{sync.error.message}</p>}
    </section>
    <section aria-label="TV comparison cases">
      <h2>Recent audit cases</h2>
      <label><input type="checkbox" checked={includeHistory} onChange={(event) => {
        setIncludeHistory(event.target.checked); setCursor(undefined);
      }} /> Show superseded snapshots</label>
      {cases.isPending ? <p role="status">Loading audit cases...</p> : cases.isError ? <p role="alert">{cases.error.message}</p> : <>
        {!cases.data.items.length && <p>No cases captured yet.</p>}
        {cases.data.items.map((item) => <details className="panel" key={item.id}>
          <summary>#{item.id} {item.input.entry.title} · {item.result ? verdict(item.result.decision) : item.status} · Sonarr {item.input.observation.sonarr.outcome}</summary>
          <AuditDetails item={item} />
        </details>)}
        <div className="actions">
          {cases.data.nextCursor && <button onClick={() => setCursor(cases.data.nextCursor!)}>Older cases</button>}
          {cursor && <button onClick={() => setCursor(undefined)}>Newest cases</button>}
        </div>
      </>}
    </section>
  </>;
}
