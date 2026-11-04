import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useTRPC } from "./rpc";
import { Failure } from "./books";
import { Link } from "./router";
import { retirementView, type RetirementPreviewItem } from "./torrent-retirement-view";

function RetirementItem({ item }: { item: RetirementPreviewItem }) {
  return <article className="panel" aria-label={item.titles[0]?.title ?? "Unavailable title"}>
    <h3 style={{ marginTop: 0 }}>{item.titles.length ? item.titles.map((title, index) => <span key={title.href}>{index > 0 && " / "}<Link href={title.href}>{title.title}</Link></span>) : "Unavailable title"}</h3>
    <p><strong>{item.eligible ? "Candidate for background verification" : item.disposition === "pending" ? "Retirement already in progress" : "Could not verify retirement"}</strong></p>
    <p>{item.explanation || "Required evidence is incomplete. No retirement is authorized by this preview."}</p>
    {(item.eligible || item.disposition === "pending") && <p><Link href="/ops">Review Activity</Link>. This preview does not start or confirm removal.</p>}
    <details><summary>Technical details</summary><p>{item.technicalLabel || item.label} · {item.media}</p><p>Binding <code>{item.bindingKey}</code></p>
      <ul>{item.blockers.map(reason => <li key={reason}>{reason}</li>)}</ul>
    </details>
  </article>;
}

export function TorrentRetirementStatus() {
  const trpc = useTRPC();
  const [offset, setOffset] = useState(0);
  const preview = useQuery(trpc.torrentRetirement.preview.queryOptions({ source: "all", offset, limit: 20 }, { refetchInterval: 30_000 }));
  const { attention, unknown, protectedCount, completeCount } = retirementView(preview.data?.items ?? []);
  return <section className="media-section" aria-label="Automatic orphan retirement">
    <h2>Automatic cleanup</h2>
    <p className="muted">Read-only preview. No seed files or canonical media are deleted.</p>
    {preview.isError ? <Failure error={preview.error} retry={() => void preview.refetch()} /> : !preview.data ? <p role="status">Reading retirement evidence...</p> : <>
      {!preview.data.automaticConfigured && <p className="notice">Private daemon identity or retirement admission is unavailable. No automatic record removal is enabled.</p>}
      {!preview.data.items.length && <p>No matching private bindings on this page.</p>}
      {attention.map(item => <RetirementItem key={item.bindingKey} item={item} />)}
      {protectedCount > 0 && <p className="muted">Protected on this page ({protectedCount}). No action needed.</p>}
      {completeCount > 0 && <p>{completeCount} retirements already confirmed on this page.</p>}
      {unknown.length > 0 && <details className="media-section"><summary>Could not verify retirement ({unknown.length})</summary>
        {unknown.map(item => <RetirementItem key={item.bindingKey} item={item} />)}
      </details>}
      <details className="media-section"><summary>Preview scope and requirements</summary>
        <p>This page scanned {preview.data.scannedCount} of {preview.data.sourceRows} raw bindings; {preview.data.excludedCount} foreign or unsupported bindings were excluded. Counts apply to this page only.</p>
        <p>Only complete, unused private torrent records qualify: every tracker, including backups, must report unregistration across three advancing announcements at least 15 minutes apart. Library-bound or actively used records remain protected.</p>
        <p>No minimum seed duration applies to this record-only retirement. Ambiguous tracker failures, timeouts and missing evidence do not authorize retirement.</p>
      </details>
      {preview.data.nextOffset !== null && <><p>More bindings have not been shown.</p><button className="secondary" disabled={preview.isFetching} onClick={() => setOffset(preview.data!.nextOffset!)}>Next retirement page</button></>}
      {offset > 0 && <button className="secondary" onClick={() => setOffset(0)}>First retirement page</button>}
    </>}
  </section>;
}
