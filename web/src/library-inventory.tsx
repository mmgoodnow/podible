import { useQuery } from "@tanstack/react-query";
import type { LibraryInventory } from "../../src/media/library-inventory";
import { useTRPC } from "./rpc";
import { metricBytes } from "./transfer-metrics-model";

export function LibraryInventoryStats() {
  const trpc = useTRPC();
  const query = useQuery(trpc.transferMetrics.library.queryOptions({}, { staleTime: 60_000, refetchInterval: 300_000 }));
  const data = query.data;
  const row = (key: keyof LibraryInventory["categories"], label: string, nested = false) => {
    const category = data!.categories[key];
    return <tr key={key} className={nested ? "inventory-subrow" : ""}><th scope="row">{label}</th>
      <td>{category.count.toLocaleString()}{category.unknownCount > 0 && <small> + {category.unknownCount.toLocaleString()} unchecked</small>}</td>
      <td>{category.onDiskBytes === null ? <span title={`Known allocation: ${metricBytes(category.knownOnDiskBytes)}`}>Incomplete</span> : metricBytes(category.onDiskBytes)}</td>
      <td>{metricBytes(category.logicalBytes)}</td></tr>;
  };
  return <section className="transfer-library" aria-label="Library inventory">
    <div className="transfer-heading"><h3>In your library</h3>{data && <span className="transfer-library-total">{data.totals.onDiskBytes === null ? "Storage measurement incomplete" : `${metricBytes(data.totals.onDiskBytes)} on disk`}</span>}</div>
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
