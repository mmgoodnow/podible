import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useTRPC } from "./rpc";

export function LibraryMigrationPreview() {
  const trpc = useTRPC();
  const [source, setSource] = useState<"radarr" | "sonarr">("radarr");
  const [offset, setOffset] = useState(0);
  const [opened, setOpened] = useState(false);
  const preview = useQuery(trpc.adminWeb.migrationPreview.queryOptions({ source, offset }, {
    enabled: opened, retry: false, refetchOnWindowFocus: false, staleTime: Infinity,
  }));
  return <section className="panel" aria-label="Existing library preview">
    <h2>Bring your library over</h2>
    <p>Review what Radarr or Sonarr manages. This preview changes nothing and does not start downloads.</p>
    <div className="actions">
      <label>Source <select value={source} onChange={event => {
        setOpened(false); setOffset(0); setSource(event.target.value as typeof source);
      }}><option value="radarr">Radarr</option><option value="sonarr">Sonarr</option></select></label>
      <button disabled={preview.isFetching} onClick={() => {
        if (opened) void preview.refetch(); else setOpened(true);
      }}>{preview.isFetching ? "Reading library..." : "Preview library"}</button>
    </div>
    {opened && preview.isError && <p role="alert">{preview.error.message}</p>}
    {opened && preview.data && <>
      <p>{preview.data.total.toLocaleString()} titles · {preview.data.monitored.toLocaleString()} monitored · {preview.data.withFiles.toLocaleString()} with files reported</p>
      <p className="muted">Files are reported by {source === "radarr" ? "Radarr" : "Sonarr"}, not independently verified in Plex. Nothing here has been adopted into Podible.</p>
      {preview.data.withoutIdentity > 0 && <p>{preview.data.withoutIdentity} titles need a catalog identity before they can be adopted.</p>}
      {preview.data.total === 0 ? <p>No titles found.</p> : <ul>{preview.data.items.map(item => <li key={item.sourceId}>
        <strong>{item.title}{item.year ? ` (${item.year})` : ""}</strong> · {item.monitored === null ? "Monitoring unknown" : item.monitored ? "Monitored" : "Not monitored"} · {
          item.filesReported === null ? "File status unknown" : item.filesReported ? item.episodeFiles !== null ? `${item.episodeFiles} episode files reported` : "File reported" : "No files reported"
        }{item.catalogId === null ? " · Catalog identity missing" : ""}
      </li>)}</ul>}
      <div className="actions">
        {offset > 0 && <button className="secondary" disabled={preview.isFetching} onClick={() => setOffset(Math.max(0, offset - 50))}>Previous titles</button>}
        {preview.data.nextOffset !== null && <button className="secondary" disabled={preview.isFetching} onClick={() => setOffset(preview.data!.nextOffset!)}>More titles</button>}
      </div>
    </>}
  </section>;
}
