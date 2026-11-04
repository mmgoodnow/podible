import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { StewardshipPanelApi } from "../../src/media/torrent-stewardship-types";
import { Failure } from "./books";
import { Link } from "./router";
import { stewardshipIssue, stewardshipView, type StewardshipItem } from "./stewardship-view";

const queryRoot = ["torrent-stewardship"] as const;
function duration(seconds: number) {
  const days = Math.floor(seconds / 86_400), hours = Math.floor((seconds % 86_400) / 3600);
  return `${days}d ${hours}h`;
}

function TorrentIssue({ item }: { item: StewardshipItem }) {
  const issue = stewardshipIssue(item)!, observation = item.observation;
  return <article className="panel" aria-label={item.titles[0]?.title ?? "Unavailable title"}>
    <h3 style={{ marginTop: 0 }}>{item.titles.length ? item.titles.map((title, index) => <span key={title.href}>
      {index > 0 && " / "}<Link href={title.href}>{title.title}</Link>
    </span>) : "Unavailable title"}</h3>
    {item.titlesTruncated && <p className="muted">Additional titles share this binding.</p>}
    <p><strong>{issue.title}</strong></p><p>{issue.cause}</p>
    <p><Link className="button secondary" href={issue.href}>{issue.action}</Link></p>
    <p>{observation?.actualSeedSeconds == null ? "Unknown seed time; protected" : `${duration(observation.actualSeedSeconds)} actual seeding; seed obligations retained`}</p>
    <details><summary>Technical details</summary>
      <p>{item.technicalLabel || item.label} · {item.media}</p><p>Binding <code>{item.key}</code></p>
      {observation && <p>Saved <time dateTime={observation.observedAt}>{new Date(observation.observedAt).toLocaleString()}</time>. Client state: {observation.state.replaceAll("-", " ")}.</p>}
      <p>These observations do not report media recovery progress. Open the title or Activity for its current request status.</p>
    </details>
  </article>;
}

/** Read-only status combines both producer inventories; collection provenance stays separate internally. */
export function StewardshipPage({ api }: { api: StewardshipPanelApi }) {
  const cache = useQueryClient();
  const [offset, setOffset] = useState(0);
  const [lastOperationId, setLastOperationId] = useState<string | null>(null);
  const status = useQuery({ queryKey: [...queryRoot, "all", offset], queryFn: () => api.status({ source: "all", offset, limit: 25 }), refetchInterval: 30_000 });
  const collect = useMutation({ mutationFn: (operationId: string) => api.collect({ operationId }),
    onSuccess: () => { void cache.invalidateQueries({ queryKey: queryRoot }); } });
  const { problems, unknown, healthyCount } = stewardshipView(status.data?.items ?? []);
  const grid = { display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(min(100%,22rem),1fr))", gap: "1rem" };
  return <>
    <div className="library-toolbar"><h1>Torrent health</h1>
      <button className="secondary" disabled={status.isFetching} onClick={() => void status.refetch()}>Refresh saved observations</button>
      <button id="stewardship-collection" disabled={collect.isPending} onClick={() => {
        const operationId = crypto.randomUUID(); setLastOperationId(operationId); collect.mutate(operationId);
      }}>{collect.isPending ? "Collecting..." : "Collect next batch"}</button>
    </div>
    <p className="muted">Saved observations only. This panel sends no torrent control commands.</p>
    {status.isError ? <Failure error={status.error} retry={() => void status.refetch()} /> : !status.data ? <p role="status">Reading saved observations...</p> : <>
      {status.data.unavailableReason && <p role="status" className="notice">{status.data.unavailableReason}</p>}
      {!status.data.items.length ? <p>No matching private Transmission bindings on this page.</p> : <>
        <p>{healthyCount} healthy observations on this page.</p>
        {problems.length > 0 ? <section aria-label="Torrent problems"><h2>Needs attention ({problems.length})</h2>
          <div style={grid}>{problems.map(item => <TorrentIssue key={item.key} item={item} />)}</div>
        </section> : <p>No confirmed torrent problems on this page.</p>}
        {unknown.length > 0 && <details className="media-section"><summary>Could not verify ({unknown.length})</summary>
          <div style={grid}>{unknown.map(item => <TorrentIssue key={item.key} item={item} />)}</div>
        </details>}
      </>}
      {status.data.nextOffset !== null && <><p>More bindings have not been shown.</p><button className="secondary" disabled={status.isFetching} onClick={() => setOffset(status.data!.nextOffset!)}>Next page</button></>}
      {offset > 0 && <button className="secondary" disabled={status.isFetching} onClick={() => setOffset(0)}>Back to first page</button>}
      <details className="media-section"><summary>Collection details</summary>
        <p>Read {status.data.scannedCount} raw binding rows from offset {offset}, out of {status.data.sourceRows}. {status.data.excludedCount} foreign or unsupported bindings excluded from this page.</p>
        <p>Collection rotates through at most 50 records per batch; it does not automatically check every page.</p>
        {status.data.lastCollection && <p>Last collection: <time dateTime={status.data.lastCollection.collectedAt}>{new Date(status.data.lastCollection.collectedAt).toLocaleString()}</time>
          {status.data.lastCollection.unavailableReason && ` · ${status.data.lastCollection.unavailableReason}`}</p>}
      </details>
    </>}
    <details className="media-section"><summary>Seed protection</summary>
      <p>At least 30 days of actual daemon-reported seed time is required, not time since download. Meeting the minimum seed time does not prove every tracker obligation is satisfied.</p>
      <p>Unknown seed time remains protected. This health panel never resumes, pauses, reannounces, removes torrents, or deletes data. Retirement preview is separate.</p>
    </details>
    {collect.isError && <><Failure error={collect.error} />{lastOperationId && <button className="secondary" onClick={() => collect.mutate(lastOperationId)}>Retry same collection</button>}</>}
    {collect.data && <p role="status">Collected {collect.data.observedCount} observation(s).
      {collect.data.unavailableReason ? ` ${collect.data.unavailableReason}` : " No torrent controls were sent."}</p>}
  </>;
}
