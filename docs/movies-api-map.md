# Movie API Map

For Cmd-click/go-to-definition in a TypeScript editor, open the companion
`docs/movies-api-map.ts`. Its implemented contracts import the actual source
types/functions, and its public call signatures derive from the real router.

TypeScript interface pseudocode for the movie POC and the proposed managed
torrent/shadow additions. This covers cross-component contracts, not the entire
preexisting book/auth/admin API or every private helper.

**Implemented:** movie/request RPCs, service ports, agent decisions, probing,
quality checks, repository/outbox integration, canonical content and Plex views.

**Also implemented:** read-only Radarr import observation, shadow RPCs and the admin
audit UI. See [shadow configuration](movie-shadow.md) for settings and limitations.
Seerr attribution and release-selection A/B evaluation remain future work.

**Also implemented:** Docker-managed Transmission and its supervisor. The movie
factory constructs a private `TransmissionAdapter`; legacy book/rTorrent is unchanged.

Some interfaces group existing free functions for readability. They do not
imply that identically named facade classes exist. The live Zod schemas and
source interfaces remain authoritative.

## 1. Component Boundaries

```text
Browser / Kindling
  -> authenticated Podible RPC (JSON-RPC or tRPC)
     -> application transactions + command outbox
        -> Reflow execution
           -> catalog / indexers / semantic agents / ffprobe
           -> TorrentClient -> private Docker-managed Transmission (movies)
           -> canonical content -> declarative symlink view -> Plex

Proposed shadow path:
  Radarr observations (+ optional Seerr request attribution)
    -> durable shadow evaluation
       -> read-only files + ffprobe + the same import decision logic
       -> admin audit RPC/UI
  No torrent mutations, content adoption, library publication or reacquisition.
```

## 2. Shared Domain Types (Implemented)

Based on `src/movies/types.ts`, `quality.ts`, `probe.ts` and `contracts.ts`.
IDs below are plain numbers in the actual code, not interchangeable concepts.

```ts
type MovieId = number;       // Podible database ID
type TmdbId = number;        // Provider ID used to request a catalog movie
type RequestId = number;
type TargetId = number;      // Movie + immutable quality-policy snapshot
type AcquisitionId = number; // One search/acquire attempt sequence
type VersionId = number;     // Verified files/facts; may be retained, not active
type JobId = number;         // Stable app identity, backed by a Reflow run
type ISODate = string;
type UUID = string;
type Source = "web" | "bluray" | "hdtv" | "dvd" | "cam";

interface CatalogMovie {
  tmdbId: TmdbId;
  title: string;
  originalTitle: string;
  year: number | null;
  overview: string;
  posterUrl: string | null;
  originalLanguage: string;
  imdbId: string | null;
  runtimeMinutes?: number | null;
}
interface Movie extends CatalogMovie { id: MovieId }

interface MovieQualityPolicy {
  tiers: Array<{ source: Source; resolution: 480 | 720 | 1080 | 2160 }>;
  cut: string;
  audioLanguages: string[];
  allowedCodecs: string[];
  preferredCodecs: string[];
  allowHdr: boolean;
  minimumPreferenceGain: number;
  preferProper: boolean;
}
interface MovieFacts {
  identityVerified: boolean; // Semantic evidence claim, not cryptographic proof
  cut: string | null;
  source: Source | null;
  width: number | null;
  height: number | null;
  videoCodec: string | null;
  dynamicRange: "sdr" | "hdr" | null;
  audioLanguages: string[];
  proper: boolean | null;
}
interface QualityEvaluation {
  eligibility: "eligible" | "ineligible" | "needs-verification";
  tier: number | null; // Zero-based index in the ordered policy tiers
  preferenceScore: number;
  reasons: string[];
  breakdown: Array<{ rule: string; contribution: number }>;
}
type MovieStage =
  | "requested" | "searching" | "selecting" | "downloading"
  | "verifying" | "publishing" | "available" | "blocked"
  | "replacing" | "retained";
interface MovieEvent {
  id: number;
  stage: MovieStage;
  message: string;
  createdAt: ISODate;
}
interface JobView {
  id: JobId;
  status: "queued" | "running" | "succeeded" | "failed" | "cancelled";
  error: string | null;
}
interface MovieRequestView {
  id: RequestId;
  targetId: TargetId;
  movie: Movie;
  createdAt: ISODate;
  state: "queued" | "running" | "blocked" | "available" | "withdrawn";
  available: boolean; // Independent of a pending/failed upgrade
  upgrading: boolean;
  stage: MovieStage | null;
  job: JobView | null;
  quality: QualityEvaluation | null;
  events: MovieEvent[];
  decisionNote: string | null;
  download: {
    bytesDone: number | null;
    sizeBytes: number | null;
    bytesPerSecond: number | null;
  } | null;
}
```

## 3. Client to Podible RPC (Implemented)

Source: `src/rpc/movies-router.ts`, `src/rpc.ts`, `src/trpc.ts`.
"Public" here means callable by authenticated clients, not anonymous access.
The actor comes from the session, never a client-supplied `userId`.
Request operations check ownership; compatible requests can share internal work.

```ts
type RequestInput = { requestId: RequestId };
type RequestResult = { request: MovieRequestView };
interface ReleaseSearchResult {
  searchId: UUID; // SQLite-backed, expires in 30 minutes; user + target scoped
  warnings: string[];
  results: Array<{
    index: number;
    title: string;
    provider: string;
    sizeBytes: number | null;
    seeders: number | null;
  }>;
}
interface MoviesRpc {
  status(input: {}): Promise<{
    catalogConfigured: boolean;
    acquisitionConfigured: boolean; // Configuration presence, not a health probe
  }>;
  search(input: { query: string }): Promise<{ results: CatalogMovie[] }>;
  get(input: { tmdbId: TmdbId }): Promise<{
    movie: CatalogMovie;
    requests: MovieRequestView[]; // Only the current user's requests
  }>;
  request(input: { tmdbId: TmdbId }): Promise<RequestResult>;
}
interface RequestsRpc {
  list(input: {}): Promise<{ requests: MovieRequestView[] }>;
  get(input: RequestInput): Promise<RequestResult>;
  retry(input: RequestInput): Promise<RequestResult>; // Fresh recovery acquisition
  checkUpgrade(input: RequestInput): Promise<RequestResult>;
  withdraw(input: RequestInput): Promise<RequestResult>; // Not media deletion
  searchReleases(input: RequestInput & { query?: string }): Promise<ReleaseSearchResult>;
  selectRelease(input: RequestInput & {
    searchId: UUID;
    resultIndex: number;
  }): Promise<RequestResult>;
}
interface PublicMovieApi { movies: MoviesRpc; requests: RequestsRpc }
```

The same handlers are available as JSON-RPC methods such as `movies.request`
through `/rpc`, and through the typed tRPC router under `/api/trpc`. Transport
envelopes/client syntax differ. RPC mutations return the current request view;
they do not wait for a movie to finish downloading.

Clients receive opaque search references, not torrent download URLs, metainfo,
tracker credentials or filesystem-control handles. Even a manually selected
release still undergoes semantic and post-download checks.

### Administrator Settings

The existing admin-only JSON-RPC settings methods gained a `movies` section;
these methods are not mapped into the new tRPC router. No ordinary user can
read these settings through the movie API. Source: `src/rpc/settings-router.ts`.

```ts
interface MovieSettings {
  tmdbAccessToken: string;
  managedRoot: string;
  downloadGroupId: number | null;
  plexUrl: string;
  plexToken: string;
  plexSectionId: string;
  plexViewPath: string;
  automaticUpgrades: boolean;
  searchIntervalHours: number;
  maximumReleaseBytes: number;
  qualityPolicy: MovieQualityPolicy;
}
// AppSettings also contains the preexisting book/indexer/agent/auth settings.
interface AdminSettingsRpc {
  get(input: {}): Promise<AppSettings>; // AppSettings.movies: MovieSettings
  update(input: { settings: AppSettings }): Promise<AppSettings>; // Full replace
}
```

## 4. Worker to Catalog, Indexers and Agents (Implemented)

Sources: `src/movies/catalog.ts`, `offers.ts`, `operations.ts`, `agents.ts`.
These are server-only ports. `MovieOffer` must not be used as a public DTO.

```ts
interface MovieOffer {
  key: string;
  title: string;
  provider: string;
  sizeBytes: number | null;
  seeders: number | null;
  url: string; // Private: may contain a provider's download credential
  infoHash: string | null;
  attributes: Record<string, unknown>;
}
interface MovieSearch { offers: MovieOffer[]; warnings: string[] }
interface MovieCatalog {
  search(query: string): Promise<CatalogMovie[]>;
  get(tmdbId: TmdbId): Promise<CatalogMovie>;
}
interface MovieIndexersPort {
  search(movie: Movie, query?: string): Promise<MovieSearch>;
}
interface MovieSelection {
  offerKey: string | null;
  facts: MovieFacts; // Provisional; probe facts override technical claims
  reason: string;   // Concise evidence summary, not private chain-of-thought
}
interface MovieFileCandidate {
  index: number; // Torrent/metainfo file index
  name: string;
  sizeBytes: number;
  probe: MovieProbe;
}
interface ImportDecision {
  fileIndex: number | null; // Null means no acceptable main feature
  facts: Pick<MovieFacts, "identityVerified" | "source" | "cut" | "proper">;
  reason: string;
}
interface MovieDecider {
  select(input: {
    movie: Movie;
    policy: MovieQualityPolicy;
    offers: MovieOffer[];
    current: MovieFacts | null;
  }): Promise<MovieSelection>;
  inspect(input: {
    movie: Movie;
    policy: MovieQualityPolicy;
    offer: MovieOffer;
    files: MovieFileCandidate[];
    selection: MovieSelection;
  }): Promise<ImportDecision>;
}

// The selector agent's bounded tool, not an RPC clients can invoke:
interface SelectorTools {
  inspectRelease(input: { index: number }): Promise<
    | { files: Array<{ path: string; size: number }>; totalSize: number }
    | { error: string }
  >;
}
```

The adapter constructs agent prompts from permitted fields. Provider download
URLs and raw attributes are not passed through as prompt input. Agents cannot
run arbitrary shell commands or invoke torrent mutations. Selection and import
explanations are persisted on the acquisition.

## 5. Technical Evidence and Policy (Implemented)

Sources: `src/movies/probe.ts`, `quality.ts`, `src/media/metainfo.ts`.
These interfaces group actual free functions, not additional HTTP endpoints.

```ts
interface MovieProbe {
  streams: Array<{
    codec_type: string;
    codec_name?: string;
    width?: number;
    height?: number;
    color_transfer?: string;
    tags?: { language?: string };
    disposition?: { attached_pic?: number };
  }>;
  format?: { duration?: string };
}
interface EvidenceFunctions {
  fetchMovieMetainfo(offer: MovieOffer): Promise<Uint8Array>;
  inspectMetainfo(bytes: Uint8Array): {
    infoHash: string;
    name: string;
    files: Array<{ path: string; size: number }>;
    size: number;
  };
  probeMovie(absoluteFilePath: string): Promise<MovieProbe>; // ffprobe subprocess
  factsFromMovieProbe(
    probe: MovieProbe,
    semanticClaims: ImportDecision["facts"],
  ): MovieFacts;
  evaluateMovieQuality(policy: MovieQualityPolicy, facts: MovieFacts): QualityEvaluation;
  assessMovieUpgrade(policy: MovieQualityPolicy, current: MovieFacts, candidate: MovieFacts): {
    upgrade: boolean;
    reason: string;
    from: QualityEvaluation;
    to: QualityEvaluation;
  };
}
```

Metainfo has size/path/hash checks. Probe-derived codec, resolution, language
tags and dynamic range override title claims. Identity, cut and source remain
semantic evidence; ffprobe is not proof of the movie's identity.

## 6. Worker to Torrent Engine (Implemented Port)

Source: `src/media/torrent-client.ts`. The production adapter today is rTorrent;
the proposed Transmission adapter will implement this same contract.

```ts
type TorrentRef = { clientId: string; nativeId: string };
type DownloadLocation = { rootId: string; relativePath: string };
interface TorrentSnapshot {
  ref: TorrentRef;
  location: DownloadLocation;
  state: "downloading" | "seeding" | "checking" | "inactive";
  complete: boolean;
  bytesDone: number | null;
  sizeBytes: number | null;
  downloadBytesPerSecond: number | null;
  message: string | null;
  observedAt: ISODate;
}
interface TorrentClient {
  readonly id: string;
  readonly capabilities: {
    pausedAdd: boolean;
    recheck: boolean;
    derivedLayouts: boolean; // False for the current rTorrent adapter
    metainfoVersions: readonly number[];
  };
  ensureAdded(input: { metainfo: Uint8Array; location: DownloadLocation }): Promise<TorrentRef>;
  inspect(ref: TorrentRef): Promise<TorrentSnapshot>;
  files(ref: TorrentRef): Promise<Array<{ index: number; path: string }>>;
  quiesce(ref: TorrentRef): Promise<void>;
  recheck(ref: TorrentRef): Promise<void>;
  resume(ref: TorrentRef): Promise<void>;
  removeRecord(ref: TorrentRef): Promise<void>; // Never deletes content bytes
}
```

Adding is paused/idempotent; the adapter checks ownership and layout. It does
not provide arbitrary filesystem paths supplied by a public RPC caller.

## 7. Canonical Content, Views and Plex (Implemented)

Sources: `src/media/content-store.ts`, `library-view.ts`,
`src/movies/publication.ts`, `src/media/plex-library.ts`.

```ts
interface CanonicalFile {
  id: UUID;
  relativePath: string;
  sizeBytes: number;
  sha256: string;
}
interface LibraryViewEntry { relativePath: string; contentPath: string }
interface MovieViewRoots {
  contentRoot: string;
  viewRoot: string;
  serverViewRoot: string; // Plex's mount path, not necessarily Podible's path
}
interface ContentFunctions {
  initializeManagedContent(contentRoot: string): Promise<UUID>;
  adoptContentFile(input: {
    contentRoot: string;
    sourceRoot: string;
    sourcePath: string;
    fileId: UUID;
    expectedSize: number;
    assertActive?: () => void;
  }): Promise<CanonicalFile>;
  reconcileLibraryView(input: {
    contentRoot: string;
    viewRoot: string;
    entries: LibraryViewEntry[];
    onLinkApplied?: (relativePath: string) => void | Promise<void>;
  }): Promise<{ linked: number; removed: number; unchanged: number }>;
}
interface MovieLibrary {
  scan(paths?: string[]): Promise<void>;
  verify(movie: Movie, serverVisiblePaths: string[]): Promise<boolean>;
}
interface PublicationFunctions {
  movieViewEntries(repo: MovieRepo, roots: MovieViewRoots): LibraryViewEntry[];
  reconcileMovieView(repo: MovieRepo, roots: MovieViewRoots, assertActive?: () => void): Promise<{
    linked: number; removed: number; unchanged: number;
  }>;
  publishMovieVersion(input: {
    repo: MovieRepo;
    roots: MovieViewRoots;
    library: MovieLibrary;
    targetId: TargetId;
    versionId: VersionId;
    assertActive?: () => void;
    afterPromotion?: () => void | Promise<void>; // Failure-window test seam
  }): Promise<void>;
}
```

Adoption makes independent readonly bytes, not writable hardlinks to seeding
files. Reconciliation creates relative symlinks and removes obsolete owned
presentation links, not their target content. Publication keeps old and pending
views until Plex verifies identity and exact paths; only then does it promote
the version and retire the old presentation link.

## 8. Persistence and Durable Execution (Implemented)

Sources: `src/movies/repo.ts`, `service.ts`, `acquisition.ts`, `maintenance.ts`,
`src/worker/context.ts`, `execution.ts`, and the existing `BooksRepo` outbox.
This is the cross-component subset, not every repository accessor. Existing row
types (`MovieAcquisition`, `JobRow`, etc.) are referenced instead of repeated.

```ts
interface MovieRepo {
  transaction<T>(fn: () => T): T;
  request(userId: number, movie: CatalogMovie, policy: MovieQualityPolicy): {
    requestId: RequestId; targetId: TargetId; movieId: MovieId;
  };
  startAcquisition(targetId: TargetId, reason: "initial" | "upgrade" | "recovery",
    enqueue: (acquisitionId: AcquisitionId) => JobId,
  ): { acquisition: MovieAcquisition; shared: boolean };
  saveReleaseSearch(userId: number, targetId: TargetId, offers: MovieOffer[]): UUID;
  searchedOffer(userId: number, targetId: TargetId, searchId: UUID, index: number): MovieOffer | null;
  bindDownload(acquisitionId: AcquisitionId, ref: TorrentRef,
    location: DownloadLocation, offerKey: string): number;
  observeDownload(torrentId: number, snapshot: TorrentSnapshot): void;
  decision(acquisitionId: AcquisitionId, value: unknown): void;
  event(targetId: TargetId, acquisitionId: AcquisitionId | null,
    key: string, stage: MovieStage, message: string): void;
  recordVersion(acquisitionId: AcquisitionId, facts: MovieFacts,
    files: Array<{ id: UUID; relativePath: string; sizeBytes: number; probe: unknown }>): VersionId;
  stagePublication(targetId: TargetId, versionId: VersionId): void;
  selectVersion(targetId: TargetId, versionId: VersionId, expectedRevision: number): void;
  clearPublication(targetId: TargetId, versionId: VersionId): void;
  abortPublication(targetId: TargetId, versionId: VersionId): void;
  finishAcquisition(acquisitionId: AcquisitionId): void;
  withdrawRequest(requestId: RequestId): void;
}
interface DurableExecution { // WorkerContext.execution; adapter around Reflow
  key: string;
  run<T>(name: string, action: () => T | Promise<T>): Promise<T>;
  sleep(name: string, milliseconds: number): Promise<void>;
  assertActive(): void; // Cancellation/generation/lease fencing
}
interface JobExecutionRuntime {
  engine: ReflowEngine; // Actual Reflow engine, not an additional execution layer
  reconcile(): Promise<void>; // Deliver app job_commands idempotently to Reflow
  stop(): Promise<void>;
}
interface MovieApplicationFunctions {
  queueMovieAcquisition(repo: BooksRepo, targetId: TargetId,
    reason: "initial" | "upgrade" | "recovery", manualOffer?: MovieOffer,
  ): { acquisition: MovieAcquisition; shared: boolean };
  movieRequestView(repo: BooksRepo, requestId: RequestId): MovieRequestView;
  acquireMovie(ctx: WorkerContext, job: JobRow, services: MovieServices): Promise<"done">;
  reconcileMovies(ctx: WorkerContext): Promise<"done">;
}
interface MovieServices {
  catalog: MovieCatalog;
  search(movie: Movie, query?: string): Promise<MovieSearch>;
  decider: MovieDecider;
  client: TorrentClient;
  library: MovieLibrary;
  fetchMetainfo(offer: MovieOffer): Promise<Uint8Array>;
  probe(file: string): Promise<MovieProbe>;
  timing?: { pollMs?: number; stallMs?: number; plexAttempts?: number; plexRetryMs?: number };
}
```

The request/acquisition transaction and job-command outbox commit together.
Reflow owns execution status/retries/leases; domain rows retain movie intent,
decisions, files, and publication state. The real worker registration includes
`movie_acquire` and `movie_reconcile`. The service object is server-created,
never reconstructed from client JSON.

## 9. Docker-Managed Transmission

The backend adapter translates the existing `TorrentClient` interface into
Transmission's protocol. There is no new public torrent API. The Docker entrypoint
is `scripts/container.ts`, which supervises the app and foreground daemon.

```ts
type TransmissionAdapter = import("../src/media/transmission-client").TransmissionAdapter;
type TransmissionRpc = import("../src/media/transmission-client").TransmissionRpc;
type Prepare = typeof import("../src/media/managed-transmission").prepareTransmission;
type ClientFactory = typeof import("../src/media/managed-transmission").managedTransmission;
```

Only the adapter receives the private loopback RPC address and credentials.
Torrent/resume state persists across image upgrades. Daemon RPC/web UI must not
be published or proxied through Podible's user API. Peer ports are a separate
network concern. Legacy book/rTorrent work remains untouched during rollout.
Real container acceptance verifies webseed transfer, multi-file paths, restart
persistence, no-delete removal, authenticated loopback RPC and independent recovery.

## 10. Shadow Observation and Admin API

Available in the collapsed Radarr comparison audit on `/movie-dual-write`, with
admin-only JSON-RPC and typed tRPC. Old `/movie-shadow` links redirect there:

```ts
type ShadowInput = import("../src/movies/shadow-types").ShadowInput;
type ShadowResult = import("../src/movies/shadow-types").ShadowResult;
type ShadowEvidencePorts = import("../src/movies/shadow").MovieShadowServices;
type ShadowInputs = import("@trpc/server").inferRouterInputs<import("../src/trpc").AppRouter>["movieShadow"];
type ShadowOutputs = import("@trpc/server").inferRouterOutputs<import("../src/trpc").AppRouter>["movieShadow"];
type MovieShadowRpc = { [Method in keyof ShadowInputs]: (input: ShadowInputs[Method]) => Promise<ShadowOutputs[Method]> };
// status, list, get, sync, retry, review. See the .ts companion for Cmd-click navigation.
```

GET-only Radarr observations cover recent imports and completed/blocked queue entries.
The bounded evaluator has only observation, probe and import-verifier capabilities;
no downloads, torrent control, file adoption, symlinks, or Plex mutation.
It withholds Radarr outcomes from the agent, persists file/policy snapshots, and
deduplicates unchanged evidence. Missing sources are unobservable, not wrong imports.
Execution status is projected from Reflow. Human reviews are persisted on cases.
Seerr attribution, webhook acceleration, release-selection A/B and shadow-driven
automatic recovery are not part of this first read-only import audit.
