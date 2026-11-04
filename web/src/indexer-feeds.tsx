import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useTRPC } from "./rpc";

export function IndexerFeeds() {
  const trpc = useTRPC(),
    cache = useQueryClient();
  const status = useQuery(
    trpc.indexerFeeds.status.queryOptions({}, { refetchInterval: 15_000 }),
  );
  const sync = useMutation(
    trpc.indexerFeeds.sync.mutationOptions({
      onSuccess: () =>
        cache.invalidateQueries({
          queryKey: trpc.indexerFeeds.status.queryKey(),
        }),
    }),
  );
  return (
    <section className="panel">
      <h2>Indexer feeds</h2>
      <p>
        New releases are checked against missing book formats and wanted movies,
        including movie upgrade policies. No scheduled per-title searches.
      </p>
      {status.data && (
        <>
          <p>
            Poll interval: {status.data.intervalMinutes} minutes. Last sync:{" "}
            {status.data.lastSync?.status ?? "Not run"}.
          </p>
          <p>
            Movie release matching:{" "}
            {status.data.lastMatch?.status ?? "No new releases"}.
          </p>
          <p>
            Book release matching:{" "}
            {status.data.lastBookMatch?.status ?? "No new releases"}.
          </p>
          {status.data.lastSync?.error && (
            <p role="alert">{status.data.lastSync.error}</p>
          )}
          {status.data.lastMatch?.error && (
            <p role="alert">{status.data.lastMatch.error}</p>
          )}
          {status.data.lastBookMatch?.error && (
            <p role="alert">{status.data.lastBookMatch.error}</p>
          )}
          {!status.data.configured && (
            <p>
              Configure Prowlarr or Torznab sources in Settings to receive
              indexer feeds.
            </p>
          )}
          <button
            disabled={!status.data.configured || sync.isPending}
            onClick={() => sync.mutate({})}
          >
            Poll feeds now
          </button>
          {status.data.feeds.map((feed) => (
            <details className="panel" key={feed.id}>
              <summary>
                {feed.provider} · {feed.mediaType} ·{" "}
                {feed.error
                  ? "Unavailable"
                  : feed.lastSuccessAt
                    ? "Receiving"
                    : "Not polled"}
                {feed.gapSince ? " · Coverage gap" : ""}
              </summary>
              <p>
                Last successful poll:{" "}
                {feed.lastSuccessAt
                  ? new Date(feed.lastSuccessAt).toLocaleString()
                  : "None"}
                .
              </p>
              {feed.error && <p role="alert">{feed.error}</p>}
              {feed.retryAt && (
                <p>Retry after {new Date(feed.retryAt).toLocaleString()}.</p>
              )}
              {feed.gapNote && <p className="notice">{feed.gapNote}</p>}
            </details>
          ))}
        </>
      )}
      {sync.isSuccess && <p role="status">Feed polling queued.</p>}
      {(status.error || sync.error) && (
        <p role="alert">{status.error?.message ?? sync.error?.message}</p>
      )}
    </section>
  );
}
