import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import type { LibraryInventory } from "../../src/media/library-inventory";
import { useTRPC } from "./rpc";
import { metricBytes } from "./transfer-metrics-model";

const quantity = (count: number, noun: string) => `${count.toLocaleString()} ${noun}${count === 1 ? "" : "s"}`;

export function LibraryInventoryStats() {
  const trpc = useTRPC();
  const query = useQuery(trpc.transferMetrics.library.queryOptions({}, { staleTime: 60_000 }));
  const data = query.data;
  const row = (key: keyof LibraryInventory["categories"], label: string, nested = false) => {
    const category = data!.categories[key];
    return <tr key={key} className={nested ? "inventory-subrow" : ""}><th scope="row">{label}</th>
      <td>{category.count.toLocaleString()}{category.unknownCount > 0 && <small> + {category.unknownCount.toLocaleString()} unchecked</small>}</td>
      <td>{category.onDiskBytes === null ? <span title={`Known allocation: ${metricBytes(category.knownOnDiskBytes)}`}>Incomplete</span> : metricBytes(category.onDiskBytes)}</td>
      <td>{metricBytes(category.logicalBytes)}</td></tr>;
  };
  return <section className="transfer-library" aria-label="Library inventory">
    <div className="transfer-heading"><h3>Measured library files</h3>{data && <span className="transfer-library-total">{data.totals.onDiskBytes === null ? "Storage measurement incomplete" : `${metricBytes(data.totals.onDiskBytes)} on disk`}</span>}</div>
    {query.isPending ? <p role="status">Measuring library...</p> : query.isError ? <p role="alert">Library totals unavailable. <button className="secondary" onClick={() => query.refetch()}>Try again</button></p> : data && <>
      <div className="transfer-table-scroll"><table><thead><tr><th>Media</th><th>Available</th><th>On disk</th><th>File size</th></tr></thead><tbody>
        {row("books", "Books")}{row("ebooks", "eBooks", true)}{row("audiobooks", "Audiobooks", true)}
        {row("movies", "Movies")}{row("tvShows", "TV shows")}{row("seasons", "Seasons", true)}{row("episodes", "Episodes", true)}
      </tbody></table></div>
      <p className="transfer-footnote">Available managed media. Subtotals overlap; shared files count once. On-disk allocation is not reclaimable space.</p>
      {!data.complete && <p className="transfer-library-warning">Some published files are missing or could not be measured. Those titles are not counted as available.</p>}
    </>}
  </section>;
}

export function LibraryOverview() {
  const trpc = useTRPC();
  const [measuring, setMeasuring] = useState(false);
  const query = useQuery(trpc.transferMetrics.published.queryOptions({}, { staleTime: 30_000, refetchInterval: 60_000 }));
  const data = query.data;
  return <section id="activity-library" className="activity-section" aria-label="Published library">
    <div className="transfer-heading"><h2>In your library</h2><span className="transfer-live">Published records</span></div>
    {data ? <div className="transfer-overview library-overview">
      <article><h3>Books</h3><strong>{data.books.toLocaleString()}</strong><small>{quantity(data.ebooks, "ebook")} · {quantity(data.audiobooks, "audiobook")}</small></article>
      <article><h3>Movies</h3><strong>{data.movies.toLocaleString()}</strong></article>
      <article><h3>TV shows</h3><strong>{data.shows.toLocaleString()}</strong><small>{quantity(data.seasons, "season")}</small></article>
      <article><h3>Episodes</h3><strong>{data.episodes.toLocaleString()}</strong></article>
    </div> : query.isError ? <p role="alert">Published counts unavailable. <button className="secondary" onClick={() => query.refetch()}>Try again</button></p>
      : <p role="status">Loading published counts...</p>}
    {query.isRefetchError && <p className="muted">Could not refresh published counts. Showing the last response.</p>}
    <details className="activity-disclosure" onToggle={event => setMeasuring(event.currentTarget.open)}>
      <summary>Disk usage by media type</summary>
      {measuring && <LibraryInventoryStats />}
    </details>
  </section>;
}
