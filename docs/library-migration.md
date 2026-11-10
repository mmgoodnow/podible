# Existing library preview

Settings has an administrator-only Radarr/Sonarr inventory preview. It reuses
`movies.shadow.radarrUrl` / `radarrApiKey` and `tv.sonarrUrl` / `sonarrApiKey`.
It only reads `/api/v3/movie` or `/api/v3/series`, respecting configured URL bases.
There are no upstream calls until Preview library is clicked, no periodic refresh,
no redirects, and no new credentials to configure.

The preview distinguishes monitoring, reported files, unknown file status, and
missing TMDB/TVDB identity. Sonarr episode-file counts do not prove complete season
coverage. No reported file count is a Plex verification. Credentials and source
filesystem paths are excluded from the result.

Reads are bounded at 30 seconds, 16 MiB and 25,000 inventory records, with at most
100 presented per page. A failed or unsupported read never appears as an empty
successful library. Nothing is saved, adopted, requested, downloaded or deleted.

Selective adoption remains a subsequent step: it needs reviewed season membership,
an explicit availability intent and external-versus-managed ownership. In particular,
observing a Radarr/Sonarr path must never grant permission to modify or delete it.
