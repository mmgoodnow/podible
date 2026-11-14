import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useTRPC } from "./rpc";
import "./release-feed.css";

const mediaLabels = { movie: "Movie", tv: "TV", audio: "Audiobook", ebook: "Ebook" } as const;
type Media = keyof typeof mediaLabels;
const date = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });

export function ReleaseFeedPage() {
  const trpc = useTRPC();
  const [media, setMedia] = useState<Media>();
  const [pages, setPages] = useState<number[]>([]);
  const feed = useQuery(trpc.indexerFeeds.releases.queryOptions({ media, cursor: pages.at(-1) }));
  const nextCursor = feed.data?.nextCursor;

  return <section className="release-feed" aria-labelledby="release-feed-title">
    <div className="release-feed-heading">
      <div><h1 id="release-feed-title">Release feed</h1><p>What came in from indexers and which releases were considered for acquisition.</p></div>
      <button className="secondary" disabled={feed.isFetching} onClick={() => {
        if (pages.length) setPages([]);
        else void feed.refetch();
      }}>{pages.length ? "Back to latest" : "Refresh"}</button>
    </div>
    <div className="release-feed-filters" role="group" aria-label="Filter releases by media type">
      <button className={media === undefined ? undefined : "secondary"} aria-pressed={media === undefined} onClick={() => { setMedia(undefined); setPages([]); }}>All</button>
      {(Object.keys(mediaLabels) as Media[]).map(kind => <button key={kind} className={media === kind ? undefined : "secondary"} aria-pressed={media === kind} onClick={() => { setMedia(kind); setPages([]); }}>{mediaLabels[kind]}</button>)}
    </div>
    {feed.isPending ? <p role="status">Loading releases...</p> : feed.isError ? <p role="alert">Could not load the release feed. Try refreshing.</p> : !feed.data.items.length ? <p>No releases recorded here yet.</p> : <ul className="release-feed-list" aria-label="Incoming releases">
      {feed.data.items.map(release => <li key={release.id}>
        <div className="release-feed-title"><strong>{release.title || "Untitled release"}</strong><small>{release.provider || "Unknown indexer"} · {mediaLabels[release.media]}</small></div>
        <div className="release-feed-route">
          {release.acquisitionRoutes.length > 0 ? release.acquisitionRoutes.map(route => <span key={route.targetKind} className="release-feed-match">{mediaLabels[route.targetKind]} · {route.targetCount} {route.targetCount === 1 ? "target" : "targets"}</span>) : <span>No acquisition recorded</span>}
        </div>
        <time dateTime={release.receivedAt}>{date.format(new Date(release.receivedAt))}</time>
      </li>)}
    </ul>}
    <div className="release-feed-pagination">
      {pages.length > 0 && <button className="secondary" onClick={() => setPages(pages.slice(0, -1))}>Newer releases</button>}
      {nextCursor != null && <button className="secondary" onClick={() => setPages([...pages, nextCursor])}>Older releases</button>}
    </div>
  </section>;
}
