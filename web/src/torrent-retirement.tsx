import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useTRPC } from "./rpc";
import { Failure } from "./books";

export function TorrentRetirementStatus() {
  const trpc = useTRPC();
  const [source, setSource] = useState<"books" | "screen">("books");
  const [offset, setOffset] = useState(0);
  const preview = useQuery(trpc.torrentRetirement.preview.queryOptions({ source, offset, limit: 20 }, { refetchInterval: 30_000 }));
  return <section className="media-section" aria-label="Automatic orphan retirement">
    <h2>Automatic orphan record retirement</h2>
    <p>Only complete, unused private torrent records qualify: every tracker, including backups, must report unregistration across three advancing announcements at least 15 minutes apart. No minimum seed duration applies to this record-only retirement. Library-bound or actively used records remain protected.</p>
    <p>No seed files or canonical media are deleted. Ambiguous tracker failures, timeouts and missing evidence do not authorize retirement.</p>
    <div className="media-filters" role="group" aria-label="Retirement binding source">
      {(["books", "screen"] as const).map(value => <button key={value} className="secondary" aria-pressed={source === value}
        onClick={() => { setSource(value); setOffset(0); }}>{value === "books" ? "Books" : "Movies & TV"}</button>)}
    </div>
    {preview.isError ? <Failure error={preview.error} retry={() => void preview.refetch()} /> : !preview.data ? <p role="status">Reading retirement evidence...</p> : <>
      {!preview.data.automaticConfigured && <p className="notice">Private daemon identity or retirement admission is unavailable. No automatic record removal is enabled.</p>}
      {!preview.data.items.length && <p>No matching private bindings on this page.</p>}
      {preview.data.items.map(item => <details className="panel" key={item.bindingKey}>
        <summary>{item.label} / {item.eligible ? "Eligible for background record retirement" : "Protected"}</summary>
        <ul>{item.blockers.map(reason => <li key={reason}>{reason}</li>)}</ul>
      </details>)}
      {preview.data.nextOffset !== null && <button className="secondary" onClick={() => setOffset(preview.data!.nextOffset!)}>Next retirement page</button>}
      {offset > 0 && <button className="secondary" onClick={() => setOffset(0)}>First retirement page</button>}
    </>}
  </section>;
}
