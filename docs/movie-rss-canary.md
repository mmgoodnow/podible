# Live Movie RSS Comparison

Existing Seerr canary: A Dog's Perfect Christmas (2026), TMDB 1570488,
Podible request/target 4, Radarr movie 2346. Neither system had a release in the
earlier 07:00 UTC check. No verifier initiated a search, download, request,
publication, or retry to obtain the later result.

## Observed Timeline

All times are UTC on 2026-11-04, read from persisted receipts and the exact
movie-specific Radarr history endpoint.

| Event | Time |
| --- | --- |
| Podible first captured BeyondHD RSS offers | 08:03:02.942 |
| Feed considerations/outbox for target 4 committed | 08:03:05.789 |
| Podible downloading | 08:03:44.874 |
| Podible verifying | 08:08:03.405 |
| Podible publishing | 08:12:45.770 |
| Podible available | 08:13:04.545 |
| Radarr grabbed | 08:15:41 |
| Radarr imported | 08:24:52 |

Inbox IDs 32031/32032 captured the 720p/1080p FLUX NF WEB-DL offers. Their
target considerations reference acquisition job 2302 and source feed-match job
2301. Acquisition 5 proceeded from these feed offers without a new `searching`
event; the original unsuccessful initial search was at 00:39 UTC.

Feed capture to downloading took about 42 seconds, and capture to availability
about 10 minutes. Podible's observed download start preceded Radarr's grab by
about 12 minutes; its availability preceded Radarr's import by about 11 minutes
47 seconds. Provider coverage, polling schedules, selection and transfer paths
differ, so this is not an isolated algorithm benchmark or proof of a general
speed advantage. Upstream publication-to-capture latency is not known.

## Independent Publication Check

Read-only checks against deployed `dfb6eba` confirmed target 4 selects version 4.
The derived view has one real symlink resolving to the expected positive-sized
canonical object. `PlexMovieLibrary.verify` made GET requests only and confirmed
the movie's exact TMDB identity and expected server-visible path in isolated
section 15:

```text
/media/MediaStorage/Media/podible-movies/views/plex/movies/A Dog's Perfect Christmas (2026) {tmdb-1570488}/A Dog's Perfect Christmas - t4v4.mkv
```

The later FileList and PassThePopcorn feed batches produced acquisitions 6/7,
which emitted `retained` events at 08:18:39/08:19:40 and left selected version 4
unchanged. This is live equal/lower-offer suppression evidence, not proof that
a genuinely better release was downloaded and promoted. That broader real-world
upgrade observation remains separate.
