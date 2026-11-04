import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useTRPC } from "./rpc";
export function MovieDualWrite() {
  const trpc = useTRPC(),
    cache = useQueryClient();
  const [ids, setIds] = useState("");
  const status = useQuery(
    trpc.movieDualWrite.status.queryOptions({}, { refetchInterval: 5000 }),
  );
  const requests = useQuery(
    trpc.movieDualWrite.list.queryOptions(
      { limit: 50 },
      { refetchInterval: 3000 },
    ),
  );
  const sync = useMutation(
    trpc.movieDualWrite.sync.mutationOptions({
      onSuccess: () => cache.invalidateQueries(),
    }),
  );
  const backfill = useMutation(
    trpc.movieDualWrite.backfill.mutationOptions({
      onSuccess: () => cache.invalidateQueries(),
    }),
  );
  const parsed = ids
      .split(/[,\s]+/)
      .filter(Boolean)
      .map(Number),
    valid =
      parsed.length > 0 &&
      parsed.length <= 5 &&
      parsed.every((n) => Number.isInteger(n) && n > 0);
  return (
    <section className="panel">
      <h2>Seerr dual-write</h2>
      <p>
        New approved Seerr requests run through Podible’s own search, agent
        selection, Transmission download, import and isolated Plex library.
        Radarr continues separately. Existing requests are not backfilled
        automatically.
      </p>
      {status.data && (
        <>
          <p>
            Seerr: {status.data.url || "Not configured"}. Feed starts after
            request #{status.data.baselineId ?? "not activated"}. Plex section:{" "}
            {status.data.plexSectionId || "Not configured"}.
          </p>
          <p>
            Last sync: {status.data.lastSync?.status ?? "Not run"}.{" "}
            {status.data.lastSync?.error}
          </p>
        </>
      )}
      {status.error && <p role="alert">{status.error.message}</p>}
      <button
        disabled={!status.data?.keyConfigured || sync.isPending}
        onClick={() => sync.mutate({})}
      >
        Sync new requests
      </button>
      <details>
        <summary>Backfill specific approved requests</summary>
        <form
          className="movie-search"
          onSubmit={(e) => {
            e.preventDefault();
            if (valid) backfill.mutate({ backfillRequestIds: parsed });
          }}
        >
          <label className="search">
            Seerr request IDs
            <input
              value={ids}
              onChange={(e) => setIds(e.target.value)}
              placeholder="731, 730, 729"
            />
          </label>
          <button disabled={!valid || backfill.isPending}>
            Acquire independently
          </button>
        </form>
        <p>
          At most five explicit IDs per submission. This starts real downloads,
          not just an audit.
        </p>
      </details>
      {(sync.error || backfill.error) && (
        <p role="alert">{sync.error?.message ?? backfill.error?.message}</p>
      )}
      {(sync.isSuccess || backfill.isSuccess) && (
        <p role="status">Feed work queued.</p>
      )}
      {requests.error && <p role="alert">{requests.error.message}</p>}
      {requests.data?.items.map((item) => (
        <details key={item.externalRequestId} className="panel">
          <summary>
            Seerr #{item.externalRequestId} · {item.request.movie.title} ·{" "}
            {item.request.state} · {item.request.stage}
          </summary>
          <p>
            Requested by {item.requester}. Podible request #{item.request.id}.
          </p>
          <p>{item.request.decisionNote}</p>
          <p>{item.request.job?.error}</p>
          <ol className="timeline">
            {item.request.events.map((event) => (
              <li key={event.id}>
                <strong>{event.stage}</strong>
                <p>{event.message}</p>
              </li>
            ))}
          </ol>
        </details>
      ))}
      {!requests.isPending && !requests.data?.items.length && (
        <p>No independent acquisitions yet.</p>
      )}
    </section>
  );
}
