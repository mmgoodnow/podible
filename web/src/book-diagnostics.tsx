import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useTRPC } from "./rpc";
import { bytes, Failure } from "./books";

export function BookDiagnostics({ bookId, selectedManifestationId }: {
  bookId: number;
  selectedManifestationId?: number;
}) {
  const trpc = useTRPC();
  const [open, setOpen] = useState(false);
  const diagnostics = useQuery(trpc.book.diagnostics.queryOptions(
    { bookId }, { enabled: open, refetchInterval: open ? 10_000 : false },
  ));
  return (
    <details className="panel manifestation-diagnostics" onToggle={(event) => setOpen(event.currentTarget.open)}>
      <summary>Manifestation diagnostics</summary>
      <p className="muted">Editions, ordered containers, source releases, imported files and chapter curation. Administrator only.</p>
      {open && diagnostics.isPending && <p role="status">Loading diagnostics...</p>}
      {diagnostics.error && <Failure error={diagnostics.error} retry={() => void diagnostics.refetch()} />}
      {diagnostics.data?.manifestations.map((edition) => (
        <details className="manifestation-tree" key={edition.id} data-testid={`manifestation-${edition.id}`}>
          <summary>
            <strong>{edition.label || `Manifestation ${edition.id}`}</strong> #{edition.id}
            {` · ${edition.kind} · ${bytes(edition.sizeBytes)} · score ${edition.preferredScore}`}
            {edition.id === selectedManifestationId ? " · selected" : ""}
          </summary>
          <p className="muted">
            {edition.editionNote}
            {edition.language ? ` · ${edition.language}` : ""}
            {edition.durationMs !== null ? ` · ${Math.round(edition.durationMs / 60_000)} minutes` : ""}
          </p>
          {edition.selectionNote && <p>Selection note: {edition.selectionNote}</p>}
          {edition.analysis ? (
            <section aria-label={`Chapter curation for manifestation ${edition.id}`}>
              <p>
                <strong>Chapter curation: {edition.analysis.curationStatus ?? edition.analysis.status}</strong>
                {` · ${edition.analysis.resolvedBoundaries}/${edition.analysis.totalBoundaries} boundaries · ${edition.analysis.hasChapters ? "chapters stored" : "no stored chapters"}`}
              </p>
              <p className="muted">{edition.analysis.source} · {edition.analysis.algorithmVersion}</p>
              {edition.analysis.budget && <p>Budget: {edition.analysis.budget}</p>}
              {edition.analysis.error && <p role="alert">{edition.analysis.error}</p>}
              {edition.analysis.unresolvedCount > 0 && (
                <details>
                  <summary>Unresolved nodes: {edition.analysis.unresolvedCount}</summary>
                  <ul>{edition.analysis.unresolvedNodes.map((node, index) => (
                    <li key={index}>
                      <strong>{node.title}</strong> · {node.failureKind}
                      {node.expectedStartTime !== null ? ` · expected ${Math.round(node.expectedStartTime)}s` : ""}
                      {node.errors.map((error, i) => <p key={i}>{error}</p>)}
                    </li>
                  ))}</ul>
                </details>
              )}
              {edition.analysis.eventLogPath && <p>Events: <code>{edition.analysis.eventLogPath}</code></p>}
              {edition.analysis.traceDir && <p>Traces: <code>{edition.analysis.traceDir}</code></p>}
            </section>
          ) : <p className="muted">Chapter curation: not run.</p>}
          {edition.containers.length ? <ul>{edition.containers.map((container) => (
            <li key={container.id}>
              <strong>Asset {container.id}</strong> · {container.kind} · {container.mimeType} · seq {container.sequence}
              {container.release && <p>Release {container.release.id}: {container.release.title} · {container.release.provider} · {container.release.status}</p>}
              {container.importNote && <p>Import note: {container.importNote}</p>}
              {container.files.length ? <ul>{container.files.map((file) => (
                <li key={file.id}>
                  <code>{file.path}</code>
                  <p className="muted">{file.sizeBytes} bytes · {Math.round(file.durationMs / 60_000)} minutes</p>
                  {file.sourcePath && file.sourcePath !== file.path && <p>Source: <code>{file.sourcePath}</code></p>}
                </li>
              ))}</ul> : <p className="muted">No files.</p>}
            </li>
          ))}</ul> : <p className="muted">No containers.</p>}
        </details>
      ))}
      {diagnostics.data?.manifestations.length === 0 && <p>No manifestations.</p>}
    </details>
  );
}
