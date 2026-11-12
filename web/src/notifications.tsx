import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTRPC } from "./rpc";

export function NotifyMe({ kind, tmdbId }: { kind: "movie" | "tv"; tmdbId: number }) {
  const trpc = useTRPC(), cache = useQueryClient();
  const title = { kind, tmdbId };
  const status = useQuery(trpc.notifications.watchStatus.queryOptions(title));
  const setWatch = useMutation(trpc.notifications.setWatch.mutationOptions({
    onSuccess: data => cache.setQueryData(trpc.notifications.watchStatus.queryKey(title), data),
  }));
  if (!status.data || status.data.status === "unavailable") {
    return setWatch.isSuccess && setWatch.variables.enabled && setWatch.data.status === "unavailable"
      ? <p role="status">This title is no longer waiting for Plex availability.</p> : null;
  }
  const watching = status.data.status === "watching";
  return <div className="media-section">
    <button type="button" className="secondary" disabled={setWatch.isPending}
      onClick={() => setWatch.mutate({ ...title, enabled: !watching })}>
      {watching ? "Stop watching" : "Notify me"}
    </button>
    <p role={watching ? "status" : undefined}>{watching ? "Subscribed to email alerts for this title."
      : kind === "movie" ? "Get an email when this movie is ready in Plex."
        : "Get an email as currently requested episodes become ready in Plex."} Email delivery must be enabled for your account.</p>
    {setWatch.isError && <p role="alert">Could not update this watch. Try again.</p>}
  </div>;
}
