# Movie shadow mode

Radarr remains authoritative. Podible polls its completed/blocked queue entries and recent import history, maps their paths into explicit allowed roots, probes the files and runs the real movie import verifier. No torrent calls, downloads, imports, content adoption, symlinks or Plex updates are available to this evaluator. Agent input excludes Radarr's outcome. This is an import audit, not an A/B test of release selection or automatic recovery.

## Settings

Use the existing administrator settings JSON at `/admin/settings`. Merge this into the current `movies` object; do not replace unrelated settings or existing secrets:

```json
{
  "shadow": {
    "enabled": true,
    "radarrUrl": "http://radarr:7878/radarr",
    "radarrApiKey": "<existing Radarr API key>",
    "pollIntervalMinutes": 5,
    "lookbackDays": 7,
    "maxCasesPerSync": 5,
    "pathMappings": [
      { "remoteRoot": "/media/MediaStorage/Raw/Movies", "localRoot": "/media/MediaStorage/Raw/Movies" },
      { "remoteRoot": "/media/MediaStorage/Media/Movies", "localRoot": "/media/MediaStorage/Media/Movies" }
    ]
  }
}
```

Movies are always available: explicit requests start acquisition through the normal request API. The observer does not create those requests. `movies.shadow.enabled` controls only the observer. Shadow uses `movies.qualityPolicy` and the existing `agents.apiKey`. No additional model configuration. Changes take effect without restarting; polling interval changes apply after the current interval. Mount observed media read-only when possible; the evaluator itself only performs reads, even where existing application mounts are writable for book imports.

Include Radarr's URL base if configured (`/radarr` on Cyprus). Redirects are not followed with credentials. `radarrApiKey` is stored only in administrator settings, never in cases or the status response.

## Audit UI and RPC

Administrator-only `/app/movie-shadow` shows configuration with secrets omitted, latest sync, cases, independent decisions, probe evidence, and persistent human reviews. JSON-RPC and typed tRPC expose the same admin-only `movieShadow.status`, `list`, `get`, `sync`, `retry`, `review` methods. `retry` retries failed execution; a changed file snapshot creates a new case automatically.

SQLite `movie_shadow_cases` stores immutable input/policy/file fingerprints, result and human review. Reflow owns execution status/retries and checkpoints successful probes/agent responses. Repeated polls of unchanged evidence do not repay the agent. Inputs are bounded to 50 videos/5,000 directory entries/6 levels; symlink or path escapes fail closed. Missing/moved files are `unobservable`, not evidence of incorrect acquisition. History fallback to an already imported library file is labeled `library-fallback` and cannot evaluate the original download's file-selection ambiguity.

Radarr histories are whitelisted rather than persisted wholesale: torrent download URLs, passkeys and credentials are not retained. Shadow results reflect Podible's configured quality policy, not necessarily Radarr's quality profile. A disagreement is a review case, not a claim that Radarr is wrong. Human verdicts: `podible-correct`, `radarr-correct`, `both-correct`, `both-wrong`, `uncertain`.
