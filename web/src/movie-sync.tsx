import { useState } from "react";
import { MovieDualWrite } from "./movie-dual-write";
import { MovieShadowAudit } from "./movie-shadow";
import { IndexerFeeds } from "./indexer-feeds";

export function MovieSync() {
  const [auditOpen, setAuditOpen] = useState(
    () => location.hash === "#radarr-audit",
  );
  return (
    <>
      <h1>Movie sync</h1>
      <MovieDualWrite />
      <IndexerFeeds />
      <details
        id="radarr-audit"
        className="panel"
        open={auditOpen}
        onToggle={(event) => setAuditOpen(event.currentTarget.open)}
      >
        <summary>Radarr comparison audit</summary>
        <p>
          Read-only comparisons against Radarr's existing files. These do not
          control or block independent acquisition.
        </p>
        {auditOpen && <MovieShadowAudit />}
      </details>
    </>
  );
}
