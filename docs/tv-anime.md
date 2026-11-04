# Anime Metadata Sidecar

`src/tv/anime.ts` splits an aggregate TV catalog into independently identified
anime works. Anime is not just a provider boolean: several AniDB works can have
the same TVDB series ID. Neither shared TVDB IDs nor similar titles imply identity
or relations. The sidecar has no persistence, requests, acquisition, or deployment
side effects beyond its bounded public metadata reads.

## Integration Contract

```ts
import { createAnimeMetadataAdapter } from "../src/tv/anime";

// Reuse one adapter; its snapshot, TTL, LRU, and rate-limit state are instance-local.
const anime = createAnimeMetadataAdapter({ fetch: fetcher });
const { entries, warnings, unmappedEpisodeKeys } =
  await anime.enrichAnimeEntry(sonarrCatalogEntry);
// Persist each returned TvCatalogEntry, then resolve requests within its membership.
```

The named `enrichAnimeEntry(entry, adapter?)` export uses the supplied adapter or
a lazy shared default adapter. Both return `AnimeCatalogProjection`, not a bare
array. The input/output catalog types come from `src/tv/types.ts`.

- Input episodes need unambiguous `tvdb:aired` season/episode coordinates. Scene
  numbering, titles, and absolute numbers are not substitutes.
- Certain works have `anidb:<id>` entry keys and explicit AniDB/AniList refs.
  The aggregate TVDB/TMDB IDs remain references, not unique work identities.
  Work premiere year stays null rather than inheriting the aggregate's year or
  guessing from a requested episode date; aggregate aliases are not copied.
- Episode keys, original title/date/runtime, identity, refs, and **all** original
  numbering survive unchanged. A sourced `anidb` numbering record is appended.
  Its season is AniDB's regular `1` or special `0`, not the TVDB season.
- AniList episode numbering is not invented from an ID crosswalk. Native AniDB
  and original TVDB numbering remain distinct; provider/Plex grouping can still
  use TVDB aired coordinates.
- Uncertain/unmatched episodes remain in a fallback entry under the original
  provider key. They are not silently discarded or assigned to a guessed anime.
  A failed dataset load returns the original catalog entry plus a warning.
- `missing-relations` means unknown graph data, not evidence that no relations
  exist. A unique AniDB work without an AniList link remains legitimate anime.
- No anime boolean is required to attempt enrichment. A positive result requires
  explicit dataset evidence; an ordinary unmapped TV entry is retained.

The wrapper is intended for a basic provider catalog entry before persistence.
It does not itself wire into Sonarr, Seerr, or request creation.

## Pure APIs And Evidence

`parseAnimeListXml(xml)` returns `{records,warnings}`. The primary
[Anime-Lists master XML](https://github.com/Anime-Lists/anime-lists/blob/master/anime-list-master.xml)
supplies AniDB IDs, TVDB IDs, numeric default seasons, offsets, and explicit
episode mappings. Its [format specification](https://github.com/Anime-Lists/anime-lists#format)
defines the offset direction: native episode plus offset equals TVDB episode.

`parseAnimeLinksJson(json)` returns `{links,warnings}` from
[Fribb's full JSON](https://github.com/Fribb/anime-lists/blob/master/anime-list-full.json).
Only explicit `anidb_id` / `anilist_id` links are used. Conflicts in either
direction withhold the link; duplicate identical rows are harmless. Fribb's TVDB
season fields are not used as replacement episode mappings.

`parseAniListMedia(payload, requestedId)` accepts only the exact requested anime
ID and rejects GraphQL errors/partial results. AniList's official
[media query documentation](https://github.com/AniList/docs/blob/master/docs/guide/graphql/queries/media.md)
describes ID queries and relation edges;
[`MediaEdge`](https://github.com/AniList/docs/blob/master/docs/reference/object/mediaedge.md)
provides `relationType(version: 3)` plus target node IDs. The sidecar retains anime
edges only, with source-ID provenance. It does not synthesize reciprocal edges,
watch order, or season-title relations. A uniquely crosswalked target uses an
AniDB key; otherwise its exact `anilist:<id>` key is retained as a reference.

`projectAnimeEntries({tvdbId,episodes,mappings,links,media?})` is network-free and
returns `{entries,warnings,unmappedEpisodeKeys}` with independent helper types.
Its input episode shape is `{key,title?,airDate?,runtimeMinutes?,season,episode,
absolute?}`. `key` and `sourceEpisodeKey` in projected episodes equal the supplied
stable provider episode key. Supply parser outputs and optional exact-ID media;
there is no search API and no title matching.

## Supported Mapping Boundary

Supported: numeric default TVDB season plus signed episode offset; individual
`;native-tvdb;` pairs; finite inclusive `start`/`end` ranges with optional offset;
regular/special native seasons; and unmapped `native-0` sentinels. Individual
pairs override ranges/defaults for that native episode, including across seasons.

Multiple native coordinates or multiple works for one requested TVDB coordinate
are ambiguous, never first-match. Overlapping default offsets have no XML upper
bound. Optional episode counts from an exact linked AniList work restrict only
the regular default interval; explicit mappings remain authoritative. Without
enough evidence, the episode stays unmatched rather than partitioning by the next
offset, season name, or response order.

Unsupported rules fail closed with warnings: open/malformed ranges, combined
`1-1+2` mappings, trailer/other encodings (`>=201` in native season 0), and unknown
native seasons. An unsupported rule blocks that work and competing coordinates
in its season. Absolute/letter default seasons (`a`) are not interpreted as aired
seasons, although supported explicit coordinates can still be used. Special-season
one-offs with explicit regular mappings are treated as closed lists to avoid
claiming unrelated specials through an unbounded default. `before`, TMDB-only
rules, supplemental art/genres, and alternate providers are not mapping evidence.

## Network And Cache Bounds

Fixed HTTPS dataset/API URLs only; no API key, auth header, caller URLs,
redirects, search requests, or recursive relation traversal. Injection points
include `fetch`, `now`, and `sleep`. Adapter limits can be reduced for tests.

- One complete XML/JSON snapshot, default 24-hour TTL; each body at most 16 MiB.
  These are full datasets, not truncated excerpts. Media TTL defaults to 6 hours.
- Media LRU defaults to 64 entries, hard maximum 128. Same-ID/dataset inflight
  requests coalesce; one transport runs at a time, with no unbounded wait queue.
  Concurrent different requests can report busy rather than overload upstream.
- Default 8 exact work queries per enrichment, hard maximum 16; no relation-target
  queries. Default media response cap 256 KiB, hard maximum 1 MiB.
- Default network deadline 15 seconds (maximum 30), including body streaming;
  declared and streamed byte limits both apply, and abandoned bodies are cancelled.
- AniList starts are spaced by 2.1 seconds by default. HTTP/GraphQL 429 errors and
  exhausted quota suppress subsequent network reads. `Retry-After` seconds/date
  and `X-RateLimit-Reset` extend cooldown; no automatic retry occurs. Successful
  cache reads remain usable. See official
  [rate-limit guidance](https://github.com/AniList/docs/blob/master/docs/guide/rate-limiting.md).
- Parsers cap 100,000 rows, 1,000 expanded rules/work, and 1,000 warnings.
  Projection caps 2,000 requested episodes and 200 works per aggregate; media
  responses cap 200 relation edges. XML entities/DOCTYPE are rejected.

The sidecar never serves an expired snapshot as silently current. Hosts' cooldown
state survives media eviction; the dataset snapshot is published only after both
parsers succeed. Returned values are copied to prevent cache poisoning by callers.
Separate adapter instances/processes do not share rate-limit state.

## Canary And Verification

`bun test test/unit/tv-anime.test.ts` uses real dataset facts for Code Geass:
TVDB `79525` season 1 -> AniDB `4521` -> AniList `1575`, and season 2 -> AniDB
`5373` -> AniList `2904`. It also tests Fate/Zero's shared season: TVDB `275798`,
AniDB `8160` / AniList `10087`, and AniDB `8880` / AniList `11741` with offset
`13`. These are fixtures, not production TVDB episode-ID fetches. Test episode
keys and GraphQL responses are controlled fixtures.

During implementation, complete public snapshots (about 3.4 MiB XML and 7.2 MiB
JSON) parsed into 7,516 TVDB-linked works and 13,486 unique ID links. The Code
Geass split was also exercised against those full snapshots. This verifies real
mapping evidence, not merely `seriesType === "anime"`. A live AniList POST was
blocked by the environment proxy; relation behavior is fixture-tested against the
official documented response contract, **not** claimed live-API verification.

No historical snapshot is bundled or pinned here. Upstream edits and dataset/API
disagreements remain possible; callers must surface warnings and retain the base
TV catalog rather than promote uncertain identity to confirmed identity.
