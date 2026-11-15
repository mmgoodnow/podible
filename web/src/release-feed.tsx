import { useId, useState } from "react";
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
const downloadLabels = {
  initial: { candidate: "Download option", picked: "First download", purpose: "get a first copy" },
  upgrade: { candidate: "Possible upgrade", picked: "Upgrade chosen", purpose: "get a better copy" },
  recovery: { candidate: "Possible replacement", picked: "Replacement chosen", purpose: "fill a gap in your library" },
} as const;

function picked(action: { status: string }) {
  return action.status === "picked" || action.status === "cross-seeded";
}

function describeAction(action: {
  status: string; purpose: keyof typeof downloadLabels | "cross-seed"; seedOrdinal: { min: number; max: number } | null;
}) {
  if (action.purpose !== "cross-seed") {
    const { candidate, picked: chosen, purpose } = downloadLabels[action.purpose];
    return picked(action)
      ? { label: chosen, hint: `Podible chose this release to ${purpose}. It may not be ready yet.` }
      : { label: candidate, hint: `Podible considered this release to ${purpose}, but it has not been chosen.` };
  }
  if (action.status === "not-seeded")
    return { label: "Reuse skipped", hint: "Podible considered reusing files you already have, but this release didn't count as a successful cross-seed." };
  if (action.status !== "cross-seeded")
    return { label: "Possible reuse", hint: "Podible considered sharing this release using files you already have. It hasn't recorded a successful cross-seed." };
  const rank = action.seedOrdinal;
  return {
    label: rank === null ? "Cross-seeded" : rank.min === rank.max ? `${ordinal(rank.min)} cross-seed` : `${ordinal(rank.min)} to ${ordinal(rank.max)} cross-seed`,
    hint: "Podible shared this release using files already on disk (cross-seeding)." + (rank === null ? ""
      : rank.min === rank.max ? " The number counts how many times these files have been used to share a release."
        : " The range shows that some files have been used to share more releases than others."),
  };
}

function StatusBadge({ label, hint, emphasized }: { label: string; hint: string; emphasized: boolean }) {
  const id = useId();
  const [expanded, setExpanded] = useState(false);
  const [hovered, setHovered] = useState(false);
  const visible = expanded || hovered;
  const dismiss = () => { setExpanded(false); setHovered(false); };
  return <span onPointerEnter={event => { if (event.pointerType === "mouse") setHovered(true); }}
    onPointerLeave={() => setHovered(false)}>
    <button type="button" className={emphasized ? "release-feed-match is-picked" : "release-feed-match"}
      aria-expanded={visible} aria-controls={id} aria-describedby={id}
      onClick={event => { event.currentTarget.focus({ preventScroll: true }); setExpanded(!expanded); setHovered(false); }}
      onBlur={dismiss} onKeyDown={event => { if (event.key === "Escape") dismiss(); }}>
      {label}
    </button>
    <span id={id} className="release-feed-hint" hidden={!visible}>{hint}</span>
  </span>;
}

function actionsByTitle<T extends { href: string | null; targetKind: Media; targetId: number }>(actions: T[]) {
  const groups = new Map<string, T[]>();
  for (const action of actions) {
    const key = action.href ?? `${action.targetKind}:${action.targetId}`;
    const group = groups.get(key);
    if (group) group.push(action);
    else groups.set(key, [action]);
  }
  return groups;
}

function noPickRecorded(release: { actions: { status: string }[] }) {
  return release.actions.every(action => action.status === "not-seeded");
}

function groupConsecutiveSkips<T extends Parameters<typeof noPickRecorded>[0]>(items: T[]): T[][] {
  const runs: T[][] = [];
  for (const release of items) {
    const last = runs.at(-1);
    if (last && noPickRecorded(last[0]!) && noPickRecorded(release)) last.push(release);
    else runs.push([release]);
  }
  return runs;
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

  const row = (release: Release) => <li className="release-feed-row" key={release.id}>
      <div className="release-feed-title"><strong>{release.title || "Untitled release"}</strong><small>{release.provider || "Unknown indexer"} · {mediaLabels[release.media]}</small></div>
      <time dateTime={release.receivedAt}>{date.format(new Date(release.receivedAt))}</time>
      {release.actions.length > 0 && <div className="release-feed-route">
        {[...actionsByTitle(release.actions)].map(([key, actions]) => {
          const { href, title } = actions[0]!;
          return <div className="release-feed-action" key={key}>
            {actions.map((action, i) => <StatusBadge key={i} {...describeAction(action)} emphasized={picked(action)} />)}
            {href ? <Link href={href}>{title || "Open title"}</Link> : <span>{title || "Title unavailable"}</span>}
          </div>;
        })}
      </div>}
    </li>;

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
        : <ul className="release-feed-list" aria-label="Incoming releases">
          {groupConsecutiveSkips(items).map(run => {
            const latest = run[0]!;
            if (run.length === 1) return row(latest);
            const times = run.map(release => Date.parse(release.receivedAt));
            return <li className="release-feed-group" key={`skips:${latest.id}`}>
              <details>
                <summary>
                  {`${number.format(run.length)} other releases`}
                  <span>{date.formatRange(Math.min(...times), Math.max(...times))}</span>
                </summary>
                <ul className="release-feed-list" aria-label="Releases with no recorded pick">{run.map(row)}</ul>
              </details>
            </li>;
          })}
        </ul>}
    <div className="release-feed-pagination">
      {pages.length > 0 && <button className="secondary" onClick={() => setPages(pages.slice(0, -1))}>Newer releases</button>}
      {nextCursor != null && <button className="secondary" onClick={() => setPages([...pages, nextCursor])}>Older releases</button>}
    </div>
  </section>;
}
