import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { StewardshipPanelApi, StewardshipSource } from "../../src/media/torrent-stewardship-types";
import { Failure } from "./books";

const queryRoot = ["torrent-stewardship"] as const;
const trackerLabels = { healthy: "Announce succeeded", "confirmed-error": "Confirmed tracker error",
  "confirmed-unregistered": "Explicitly unregistered", unknown: "Unknown / no current announce" };
function duration(seconds: number) {
  const days = Math.floor(seconds / 86_400), hours = Math.floor((seconds % 86_400) / 3600);
  return `${days}d ${hours}h`;
}

/** Parent composition supplies the existing typed tRPC client; no extra provider or settings. */
export function StewardshipPage({ api }: { api: StewardshipPanelApi }) {
  const cache = useQueryClient();
  const [source, setSource] = useState<StewardshipSource>("books");
  const [offset, setOffset] = useState(0);
  const [lastOperationId, setLastOperationId] = useState<string | null>(null);
  const status = useQuery({ queryKey: [...queryRoot, source, offset], queryFn: () => api.status({ source, offset, limit: 25 }), refetchInterval: 30_000 });
  const collect = useMutation({ mutationFn: (operationId: string) => api.collect({ operationId }),
    onSuccess: () => { void cache.invalidateQueries({ queryKey: queryRoot }); } });
  return <>
    <div className="library-toolbar"><h1>Torrent Stewardship</h1>
      <button className="secondary" disabled={status.isFetching} onClick={() => void status.refetch()}>Refresh saved observations</button>
      <button disabled={collect.isPending} onClick={() => {
        const operationId = crypto.randomUUID(); setLastOperationId(operationId); collect.mutate(operationId);
      }}>{collect.isPending ? "Collecting..." : "Collect next batch"}</button>
    </div>
    <p>Health of Podible-owned records in its private Transmission daemon. Seed protection requires at least 30 days of actual daemon-reported seed time, not time since download.</p>
    <p className="notice">Observation only. This panel never resumes, pauses, reannounces, removes torrents, or deletes data. Meeting the minimum seed time does not prove every tracker obligation is satisfied.</p>
    <div className="media-filters" role="group" aria-label="Stewardship binding source">
      {(["books", "screen"] as const).map(value => <button key={value} className="secondary" aria-pressed={source === value}
        onClick={() => { setSource(value); setOffset(0); }}>{value === "books" ? "Books" : "Movies & TV"}</button>)}
    </div>
    {status.isError ? <Failure error={status.error} retry={() => void status.refetch()} /> : !status.data ? <p role="status">Reading saved observations...</p> : <>
      {status.data.unavailableReason && <p role="status" className="notice">{status.data.unavailableReason}</p>}
      <p className="muted">Viewing {status.data.scannedCount} binding rows from offset {offset}. {status.data.excludedCount} foreign or unsupported binding(s) excluded. Collection rotates through at most 50 records per batch.</p>
      {status.data.lastCollection && <p>Last collection: <time dateTime={status.data.lastCollection.collectedAt}>{new Date(status.data.lastCollection.collectedAt).toLocaleString()}</time>
        {status.data.lastCollection.unavailableReason && ` · ${status.data.lastCollection.unavailableReason}`}</p>}
      {!status.data.items.length ? <p>No matching private Transmission bindings on this page.</p> : <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", textAlign: "left", borderSpacing: "0 1rem" }}>
          <caption>Durable torrent observations. Unknown is not a confirmed failure.</caption>
          <thead><tr><th scope="col">Owned Binding</th><th scope="col">State</th><th scope="col">Tracker Evidence</th><th scope="col">Seed Protection</th></tr></thead>
          <tbody>{status.data.items.map(item => {
            const observation = item.observation;
            return <tr key={item.key}>
              <th scope="row">{item.label}<small style={{ display: "block" }}>{item.media}</small></th>
              <td>{observation ? observation.state.replaceAll("-", " ") : "Not collected"}
                {observation && observation.daemonError !== "none" && observation.daemonError !== "unknown" && <small style={{ display: "block" }}>Daemon: {observation.daemonError.replaceAll("-", " ")}</small>}
                {observation?.ownership !== undefined && observation.ownership !== "verified-private-binding" && <small style={{ display: "block" }}>{observation.ownership.replaceAll("-", " ")}</small>}
                {observation && <small style={{ display: "block" }}><time dateTime={observation.observedAt}>{new Date(observation.observedAt).toLocaleString()}</time></small>}
                {item.stale && <small style={{ display: "block" }}>No current observation</small>}</td>
              <td>{observation ? trackerLabels[observation.trackerState] : "Unknown"}
                {observation && <small style={{ display: "block" }}>{observation.reason}</small>}
                {!!observation?.consistentUnregisteredAnnouncements && <small style={{ display: "block" }}>{observation.consistentUnregisteredAnnouncements} distinct consistent announcement(s){observation.repeatedUnregistered ? "; repeated evidence recorded, retirement disabled" : ""}</small>}</td>
              <td>{observation?.actualSeedSeconds === null || !observation ? "Unknown seed time; protected" : <>
                {duration(observation.actualSeedSeconds)} actual seeding
                <small style={{ display: "block" }}>{observation.seedProtection === "minimum-met" ? "30-day minimum met; retirement still disabled" : `${duration(observation.remainingSeedSeconds!)} minimum remaining`}</small>
              </>}{observation?.seedCounterRegressed && <small style={{ display: "block" }}>Daemon counter decreased; repeated-error evidence reset.</small>}</td>
            </tr>;
          })}</tbody>
        </table>
      </div>}
      {status.data.nextOffset !== null && <button className="secondary" onClick={() => setOffset(status.data!.nextOffset!)}>Next page</button>}
      {offset > 0 && <button className="secondary" onClick={() => setOffset(0)}>Back to first page</button>}
    </>}
    {collect.isError && <><Failure error={collect.error} />{lastOperationId && <button className="secondary" onClick={() => collect.mutate(lastOperationId)}>Retry same collection</button>}</>}
    {collect.data && <p role="status">Collected {collect.data.observedCount} observation(s) from {collect.data.source === "books" ? "books" : "movies & TV"}, offset {collect.data.offset}.
      {collect.data.unavailableReason ? ` ${collect.data.unavailableReason}` : " No torrent controls were sent."}</p>}
  </>;
}
