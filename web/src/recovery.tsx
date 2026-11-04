import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import type { inferRouterInputs } from "@trpc/server";
import type { AppRouter } from "../../src/trpc";
import { useTRPC } from "./rpc";
import { Failure } from "./books";
import { recoveryResourceKey, recoveryView } from "./recovery-view";

type Command = inferRouterInputs<AppRouter>["recovery"]["recover"];
export function RecoveryPage() {
  const trpc = useTRPC();
  const [namespace, setNamespace] = useState<"movie-target" | "tv-target">(() =>
    new URLSearchParams(location.search).get("media") === "tv" ? "tv-target" : "movie-target");
  const [afterTargetId, setAfterTargetId] = useState(0);
  const [selected, setSelected] = useState<string[]>([]);
  const [lastCommand, setLastCommand] = useState<Command | null>(null);
  const [resultTitles, setResultTitles] = useState<Record<string, string>>({});
  const inventory = useQuery(trpc.recovery.inventory.queryOptions({ namespace, afterTargetId, limit: 20 }));
  const recover = useMutation(trpc.recovery.recover.mutationOptions({ onSuccess: () => {
    setSelected([]);
    void inventory.refetch();
  } }));
  const { missing, unchecked, checkedCount, actionable } = recoveryView(inventory.data?.resources ?? []);
  const selectedResources = actionable.filter(resource => selected.includes(resource.key.id));
  function submit() {
    const targets: Command["targets"] = selectedResources.map(r => ({
      key: r.key, expectedRevision: r.revision, action: r.action as "repair-view" | "queue-acquisition",
    }));
    if (!targets.length || !window.confirm(`Recover ${targets.length} selected target(s)? Missing links will be repaired; missing content may queue acquisition. No files or torrent records will be deleted.`)) return;
    const command = { operationId: crypto.randomUUID(), targets };
    setResultTitles(Object.fromEntries(selectedResources.map(resource => [recoveryResourceKey(resource), resource.title])));
    setLastCommand(command);
    recover.mutate(command);
  }
  return <>
    <div className="library-toolbar"><h1>Restore missing media</h1>
      <button className="secondary" disabled={inventory.isFetching || recover.isPending} onClick={() => { setSelected([]); void inventory.refetch(); }}>Check again</button>
    </div>
    <div className="media-filters" role="group" aria-label="Recovery media type">
      {(["movie-target", "tv-target"] as const).map(n => <button key={n} className="secondary" aria-pressed={namespace === n}
        disabled={recover.isPending} onClick={() => { setNamespace(n); setAfterTargetId(0); setSelected([]); }}>
        {n === "movie-target" ? "Movies" : "TV"}
      </button>)}
    </div>
    {inventory.isError ? <Failure error={inventory.error} retry={() => void inventory.refetch()} /> : inventory.isPending ? <p role="status">Inspecting owned media...</p> : <>
      {actionable.length > 0 && <div className="recovery-actions">
        <button className="secondary" disabled={recover.isPending || !actionable.length} onClick={() => setSelected(actionable.map(r => r.key.id))}>Select recoverable</button>
        <button disabled={recover.isPending || inventory.isFetching || !selectedResources.length} onClick={submit}>{recover.isPending ? "Recovering..." : `Recover selected (${selectedResources.length})`}</button>
      </div>}
      {!missing.length && <p>{checkedCount === 0 ? "No titles could be checked." : `No missing files or broken links found among these ${checkedCount} checked titles.${unchecked.length > 0 ? " Not all titles could be checked." : ""}`}</p>}
      {missing.map(r => <section className="recovery-resource" key={recoveryResourceKey(r)}>
        <div className="recovery-title"><label>
          <input type="checkbox" checked={selected.includes(r.key.id)} disabled={recover.isPending || r.action === "blocked" || r.action === "none"}
            onChange={e => setSelected(previous => e.target.checked ? [...previous, r.key.id] : previous.filter(id => id !== r.key.id))} />
          <strong>{r.title}</strong>
        </label><span>{r.action === "repair-view" ? "Repair links" : r.action === "queue-acquisition" ? "Retrieve missing files" : r.action === "none" ? "No repair needed" : "Cannot repair yet"}</span></div>
        <p>{r.action === "repair-view" ? "Your media files are present. Repairing will restore their missing library links."
          : r.action === "queue-acquisition" ? "Recovery will queue retrieval of missing media while keeping existing requests and surviving files."
          : r.action === "blocked" ? r.reasons[0]?.message ?? "The required safety checks have not passed."
          : r.canonical.state === "present" && r.view.state === "present" ? "Media files and library links are present." : "No repair is currently available."}</p>
        {r.activeAcquisitions.length > 0 && <p>{r.activeAcquisitions.length} active acquisition(s); recovery will not duplicate them.</p>}
        <details><summary>Inspection details</summary><p>Wanted: {r.desiredPresence} · Mount: {r.mount.state} · Content: {r.canonical.state} · Library links: {r.view.state}</p>
          <ul>{r.reasons.map(reason => <li key={reason.code}>{reason.message}</li>)}</ul></details>
      </section>)}
      {unchecked.length > 0 && <details className="media-section"><summary>Could not check ({unchecked.length})</summary>
        <p>We couldn't verify these titles. They may still be available.</p>
        {unchecked.map(resource => <div key={recoveryResourceKey(resource)}><h3>{resource.title}</h3>
          <p>{resource.mount.state === "unavailable" ? resource.mount.reason : resource.reasons[0]?.message ?? "Owned paths could not be verified."}</p>
        </div>)}
      </details>}
      {inventory.data.nextAfterTargetId !== null && <><p>More titles have not been checked.</p><button className="secondary" disabled={recover.isPending || inventory.isFetching} onClick={() => { setAfterTargetId(inventory.data!.nextAfterTargetId!); setSelected([]); }}>Check next 20</button></>}
      {afterTargetId > 0 && <button className="secondary" disabled={recover.isPending} onClick={() => { setAfterTargetId(0); setSelected([]); }}>Back to first page</button>}
    </>}
    {recover.isError && <><Failure error={recover.error} />{lastCommand && <button className="secondary" onClick={() => recover.mutate(lastCommand)}>Retry same recovery</button>}</>}
    {recover.data && <section className="media-section" aria-label="Recovery results"><h2>Results</h2>
      {recover.data.results.map(r => <div key={recoveryResourceKey(r)}><strong>{resultTitles[recoveryResourceKey(r)] ?? inventory.data?.resources.find(resource => recoveryResourceKey(resource) === recoveryResourceKey(r))?.title ?? "Selected title"}: {{ repaired: "Links repaired", queued: "Recovery queued", unchanged: "No change needed", blocked: "Recovery blocked" }[r.status]}</strong>
        {r.jobIds.length > 0 && <p>Recovery is queued. Queueing is not confirmation that content has been restored.</p>}
        <ul>{r.reasons.map(reason => <li key={reason.code}>{reason.message}</li>)}</ul>
      </div>)}
    </section>}
  </>;
}
