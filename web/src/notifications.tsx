import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useAcquisitionEvents } from "./acquisition-events";
import { Link } from "./router";
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
    <p role={watching ? "status" : undefined}>{watching ? "Watching. Updates will appear in Notifications."
      : kind === "movie" ? "Get an in-app notification when this movie is ready in Plex."
        : "Get in-app notifications as currently requested episodes become ready in Plex."}</p>
    {setWatch.isError && <p role="alert">Could not update this watch. Try again.</p>}
  </div>;
}

export function NotificationsInbox() {
  const trpc = useTRPC(), cache = useQueryClient();
  const [pages, setPages] = useState<string[]>([]);
  const before = pages.at(-1);
  const inbox = useQuery(trpc.notifications.list.queryOptions({ before }));
  const next = inbox.data?.next;
  const invalidate = () => { void cache.invalidateQueries({ queryKey: trpc.notifications.list.pathKey() }); };
  useAcquisitionEvents(true, invalidate);
  const read = useMutation(trpc.notifications.markRead.mutationOptions({ onSuccess: invalidate }));
  return <section aria-labelledby="notifications-heading">
    <div className="library-toolbar"><h1 id="notifications-heading">Notifications</h1></div>
    <p>Updates for your movie and TV requests, and for titles you watch. Only you can see this inbox.</p>
    {inbox.isPending && <p role="status">Loading notifications...</p>}
    {inbox.isError && <p role="alert">Could not load notifications. <button onClick={() => void inbox.refetch()}>Try again</button></p>}
    {inbox.data?.items.length === 0 && <p>No notifications yet.</p>}
    {inbox.data?.items.map(item => <article className="panel" key={item.id}>
      <h2><Link href={item.href}>{item.title}</Link></h2>
      <p>{item.episode ? `${item.episode} is ready in Plex.` : "Ready in Plex."}</p>
      <div className="actions">
        <time dateTime={item.createdAt}>{new Date(item.createdAt).toLocaleString()}</time>
        {item.readAt ? <span>Read</span> : <button className="secondary" disabled={read.isPending}
          onClick={() => read.mutate({ id: item.id })}>Mark read</button>}
      </div>
    </article>)}
    {read.isError && <p role="alert">Could not mark this notification read. Try again.</p>}
    <div className="actions">
      {before && <button className="secondary" onClick={() => setPages(p => p.slice(0, -1))}>Newer</button>}
      {next && <button className="secondary" onClick={() => setPages(p => [...p, next])}>Older</button>}
    </div>
  </section>;
}
