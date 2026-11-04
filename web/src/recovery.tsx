import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import type { inferRouterInputs } from "@trpc/server";
import type { AppRouter } from "../../src/trpc";
import { useTRPC } from "./rpc";
import { Failure } from "./books";

type Command = inferRouterInputs<AppRouter>["recovery"]["recover"];
export function RecoveryPage() {
  const trpc = useTRPC();
  const [namespace, setNamespace] = useState<"movie-target" | "tv-target">("movie-target");
  const [afterTargetId, setAfterTargetId] = useState(0);
  const [selected, setSelected] = useState<string[]>([]);
  const [lastCommand, setLastCommand] = useState<Command | null>(null);
  const inventory = useQuery(trpc.recovery.inventory.queryOptions({ namespace, afterTargetId, limit: 20 }));
  const recover = useMutation(trpc.recovery.recover.mutationOptions({ onSuccess: () => {
    setSelected([]);
    void inventory.refetch();
  } }));
  const actionable = inventory.data?.resources.filter(r => r.action === "repair-view" || r.action === "queue-acquisition") ?? [];
  function submit() {
    const targets: Command["targets"] = actionable.filter(r => selected.includes(r.key.id)).map(r => ({
      key: r.key, expectedRevision: r.revision, action: r.action as "repair-view" | "queue-acquisition",
    }));
    if (!targets.length || !window.confirm(`Recover ${targets.length} selected target(s)? Missing links will be repaired; missing content may queue acquisition. No files or torrent records will be deleted.`)) return;
    const command = { operationId: crypto.randomUUID(), targets };
    setLastCommand(command);
    recover.mutate(command);
  }
  return <>
    <div className="library-toolbar"><h1>Recovery</h1>
      <button className="secondary" disabled={inventory.isFetching || recover.isPending} onClick={() => { setSelected([]); void inventory.refetch(); }}>Refresh evidence</button>
    </div>
    <p>Restore missing presentation links or reacquire wanted media without rebuilding requests. Missing or replaced storage mounts block recovery rather than trigger a download flood.</p>
    <div className="media-filters" role="group" aria-label="Recovery media type">
      {(["movie-target", "tv-target"] as const).map(n => <button key={n} className="secondary" aria-pressed={namespace === n}
        disabled={recover.isPending} onClick={() => { setNamespace(n); setAfterTargetId(0); setSelected([]); }}>
        {n === "movie-target" ? "Movies" : "TV"}
      </button>)}
    </div>
    <p className="muted">Preview is read-only. Recovery preserves intentional absence, existing request IDs and torrent seed data. At most 20 targets are considered per batch.</p>
    {inventory.isError ? <Failure error={inventory.error} retry={() => void inventory.refetch()} /> : inventory.isPending ? <p role="status">Inspecting owned media...</p> : <>
      <div className="recovery-actions">
        <button className="secondary" disabled={recover.isPending || !actionable.length} onClick={() => setSelected(actionable.map(r => r.key.id))}>Select recoverable</button>
        <button disabled={recover.isPending || inventory.isFetching || !selected.length} onClick={submit}>{recover.isPending ? "Recovering..." : `Recover selected (${selected.length})`}</button>
      </div>
      {!inventory.data.resources.length && <p>No tracked {namespace === "movie-target" ? "movie" : "TV"} targets.</p>}
      {inventory.data.resources.map(r => <section className="recovery-resource" key={`${r.key.namespace}:${r.key.id}`}>
        <div className="recovery-title"><label>
          <input type="checkbox" checked={selected.includes(r.key.id)} disabled={recover.isPending || r.action === "blocked" || r.action === "none"}
            onChange={e => setSelected(previous => e.target.checked ? [...previous, r.key.id] : previous.filter(id => id !== r.key.id))} />
          <strong>{r.title}</strong>
        </label><span>{r.action === "repair-view" ? "Repair links" : r.action === "queue-acquisition" ? "Reacquire" : r.action === "none" ? "No action" : "Blocked"}</span></div>
        <p>Wanted: {r.desiredPresence} · Mount: {r.mount.state} · Content: {r.canonical.state} · View: {r.view.state}</p>
        {r.activeAcquisitions.length > 0 && <p>{r.activeAcquisitions.length} active acquisition(s); recovery will not duplicate them.</p>}
        <ul>{r.reasons.map(reason => <li key={reason.code}>{reason.message}</li>)}</ul>
      </section>)}
      {inventory.data.nextAfterTargetId !== null && <button className="secondary" disabled={recover.isPending} onClick={() => { setAfterTargetId(inventory.data!.nextAfterTargetId!); setSelected([]); }}>Next 20 targets</button>}
      {afterTargetId > 0 && <button className="secondary" disabled={recover.isPending} onClick={() => { setAfterTargetId(0); setSelected([]); }}>Back to first page</button>}
    </>}
    {recover.isError && <><Failure error={recover.error} />{lastCommand && <button className="secondary" onClick={() => recover.mutate(lastCommand)}>Retry same recovery</button>}</>}
    {recover.data && <section className="media-section" aria-label="Recovery results"><h2>Results</h2>
      {recover.data.results.map(r => <div key={`${r.key.namespace}:${r.key.id}`}><strong>{r.key.namespace} {r.key.id}: {r.status}</strong>
        {r.jobIds.length > 0 && <p>Queued jobs: {r.jobIds.join(", ")}. Queueing is not confirmation that content has been restored.</p>}
        <ul>{r.reasons.map(reason => <li key={reason.code}>{reason.message}</li>)}</ul>
      </div>)}
    </section>}
  </>;
}
