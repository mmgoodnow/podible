import { useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "./router";
import { useTRPC } from "./rpc";
import "./release-feed.css";

const mediaLabels = { movie: "Movie", tv: "TV", audio: "Audiobook", ebook: "Ebook" } as const;
type Media = keyof typeof mediaLabels;
const date = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
const number = new Intl.NumberFormat();
const ordinalRule = new Intl.PluralRules("en", { type: "ordinal" });
const ordinal = (n: number) => `${n}${({ one: "st", two: "nd", few: "rd", other: "th" } as const)[ordinalRule.select(n) as "one" | "two" | "few" | "other"]}`;
const purposeLabels = { initial: "Initial download", upgrade: "Upgrade", recovery: "Recovery", "cross-seed": "Cross-seed" } as const;

function picked(action: { status: string }) {
  return action.status === "picked" || action.status === "cross-seeded";
}

function ReleaseGroup({ title, count, children }: { title: string; count: number; children: ReactNode }) {
  if (!count) return null;
  return <details className="release-feed-group">
    <summary>{title} <span>{number.format(count)}</span></summary>
    {children}
  </details>;
}

export function ReleaseFeedPage() {
  const trpc = useTRPC();
  const [media, setMedia] = useState<Media>();
  const [pickedOnly, setPickedOnly] = useState(false);
  const [pages, setPages] = useState<number[]>([]);
  const feed = useQuery(trpc.indexerFeeds.releases.queryOptions({ media, pickedOnly, cursor: pages.at(-1) }));
  const nextCursor = feed.data?.nextCursor;
  type Release = NonNullable<typeof feed.data>["items"][number];
  const items = feed.data?.items ?? [];
  const picks = items.filter(release => release.actions.some(picked));
  const candidates = items.filter(release => !release.actions.some(picked) && release.actions.some(action => action.status === "candidate"));
  const skipped = items.filter(release => release.actions.every(action => action.status === "not-seeded"));

  const list = (releases: Release[], label: string) => <ul className="release-feed-list" aria-label={label}>
    {releases.map(release => <li key={release.id}>
      <div className="release-feed-title"><strong>{release.title || "Untitled release"}</strong><small>{release.provider || "Unknown indexer"} · {mediaLabels[release.media]}</small></div>
      <time dateTime={release.receivedAt}>{date.format(new Date(release.receivedAt))}</time>
      {release.actions.length > 0 && <div className="release-feed-route">
        {release.actions.map((action, i) => {
          const rank = action.seedOrdinal;
          const status = action.status === "candidate" ? "Candidate" : action.status === "picked" ? "Picked"
            : action.status === "not-seeded" ? "Not seeded"
              : rank === null ? "Cross-seeded" : rank.min === rank.max
                ? `${ordinal(rank.min)} cross-seed` : `${ordinal(rank.min)} to ${ordinal(rank.max)} cross-seed across files`;
          return <div className="release-feed-action" key={i}>
            <span className={picked(action) ? "release-feed-match is-picked" : "release-feed-match"}>{status}</span>
            {action.status !== "cross-seeded" && <span>{purposeLabels[action.purpose]}</span>}
            {action.href ? <Link href={action.href}>{action.title || "Open title"}</Link>
              : <span>{action.title || "Title unavailable"}</span>}
          </div>;
        })}
      </div>}
    </li>)}
  </ul>;

  return <section className="release-feed" aria-labelledby="release-feed-title">
    <div className="release-feed-heading">
      <div><h1 id="release-feed-title">Release feed</h1><p>See what arrived, what Podible picked, and which titles it was for.</p></div>
      <button className="secondary" disabled={feed.isFetching} onClick={() => {
        if (pages.length) setPages([]);
        else void feed.refetch();
      }}>{pages.length ? "Back to latest" : "Refresh"}</button>
    </div>
    <div className="release-feed-filters" role="group" aria-label="Filter releases by media type">
      <button className={media === undefined ? undefined : "secondary"} aria-pressed={media === undefined} onClick={() => { setMedia(undefined); setPages([]); }}>All</button>
      {(Object.keys(mediaLabels) as Media[]).map(kind => <button key={kind} className={media === kind ? undefined : "secondary"} aria-pressed={media === kind} onClick={() => { setMedia(kind); setPages([]); }}>{mediaLabels[kind]}</button>)}
      <label className="release-feed-picked-filter"><input type="checkbox" checked={pickedOnly} onChange={event => { setPickedOnly(event.target.checked); setPages([]); }} /> Picked only</label>
    </div>
    {feed.isPending ? <p role="status">Loading releases...</p> : feed.isError ? <p role="alert">Could not load the release feed. Try refreshing.</p>
      : !items.length ? <p>{pickedOnly ? "No picks recorded here yet." : "No releases recorded here yet."}</p>
        : <>
          <h2 className="release-feed-picked-heading">Picked <small>{picks.length}</small></h2>
          {picks.length ? list(picks, "Picked releases") : <p className="release-feed-empty">No picks on this page.</p>}
          <ReleaseGroup title="Candidates" count={candidates.length} key={`candidates-${pages.at(-1)}-${media}`}>{list(candidates, "Release candidates")}</ReleaseGroup>
          <ReleaseGroup title="No pick recorded" count={skipped.length} key={`skipped-${pages.at(-1)}-${media}`}>{list(skipped, "Releases with no pick recorded")}</ReleaseGroup>
        </>}
    <div className="release-feed-pagination">
      {pages.length > 0 && <button className="secondary" onClick={() => setPages(pages.slice(0, -1))}>Newer releases</button>}
      {nextCursor != null && <button className="secondary" onClick={() => setPages([...pages, nextCursor])}>Older releases</button>}
    </div>
  </section>;
}
