import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTRPC } from "./rpc";

export function CrossSeedBackfill() {
  const trpc = useTRPC(), cache = useQueryClient();
  const latest = useQuery(trpc.crossSeed.backfillLatest.queryOptions({}, { refetchInterval: query => {
    const status = query.state.data?.backfill?.status;
    return status && ["queued", "running", "waiting"].includes(status) ? 5000 : 60_000;
  } }));
  const refresh = () => cache.invalidateQueries({ queryKey: trpc.crossSeed.backfillLatest.pathKey() });
  const start = useMutation(trpc.crossSeed.startBackfill.mutationOptions({ onSuccess: refresh }));
  const cancel = useMutation(trpc.crossSeed.cancelBackfill.mutationOptions({ onSuccess: refresh }));
  const retry = useMutation(trpc.jobs.retry.mutationOptions({ onSuccess: refresh }));
  const state = latest.data?.backfill;
  const busy = state && ["queued", "running", "waiting"].includes(state.status);
  const pending = start.isPending || cancel.isPending || retry.isPending;
  const error = start.error ?? cancel.error ?? retry.error;
  const label = !state ? "Search the existing library" : state.status === "queued" ? "Preparing library search"
    : state.status === "waiting" ? "Waiting for indexers to recover"
    : state.status === "running" ? "Searching the library"
    : state.status === "cancelled" ? "Library search stopped"
    : state.status === "failed" ? "Some library searches could not finish" : "Library search complete";
  return <section className="transfer-backfill" aria-label="Find more seeds">
    <div><h4>Find more seeds</h4><p>Search existing books, movies and TV for matching torrents.</p></div>
    {latest.isError ? <p role="alert">Could not load library search progress. <button onClick={() => latest.refetch()}>Try again</button></p>
      : <>
        <p role="status"><strong>{label}</strong>{state && state.total > 0 && <span> · {state.scanned.toLocaleString()} of {state.total.toLocaleString()} items checked · {state.queued.toLocaleString()} candidates queued for verification</span>}</p>
        {state && busy && state.total > 0 && <progress aria-label="Library cross-seed search" max={state.total} value={state.scanned} />}
        {state?.errors ? <p className="transfer-backfill-error">{state.errors.toLocaleString()} indexer searches need another attempt. Other results are saved.</p> : null}
        <div className="actions">
          {busy ? <button disabled={pending} onClick={() => cancel.mutate({ requestId: state.requestId })}>Stop search</button>
            : <>
              {state?.status === "failed" && <button disabled={pending} onClick={() => retry.mutate({ jobId: state.jobId })}>Retry unfinished searches</button>}
              <button disabled={pending || latest.isPending} onClick={() => start.mutate({ requestId: crypto.randomUUID() })}>
                {start.isPending ? "Starting search..." : state ? "Search library again" : "Search library"}
              </button>
            </>}
        </div>
        {state && <p className="transfer-footnote">Candidates count as seeds only after verification. Queued checks continue if you stop searching.</p>}
      </>}
    {error && <p role="alert">Could not confirm the request. Refresh progress before trying again.</p>}
  </section>;
}
