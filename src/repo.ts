import { Database } from "bun:sqlite";

import { nowIso } from "./db";
import { pseudoProgressForBook } from "./library/progress";
import { deriveBookStatus, deriveMediaStatus, MediaStatus, ReleaseStatus } from "./library/status";
import { defaultSettings, parseSettings } from "./settings";
import type {
  AppSettings,
  AppLoginAttemptRow,
  AuthProvider,
  AuthCodeRow,
  AssetFileRow,
  AssetTranscriptRow,
  AssetTranscriptStatus,
  AssetRow,
  BookRow,
  ManifestationKind,
  ManifestationRow,
  ChapterAnalysisRow,
  ChapterAnalysisStatus,
  DownloadView,
  JobRow,
  JobStatus,
  JobType,
  LibraryBook,
  MediaType,
  ReleaseRow,
  SessionWithUserRow,
  SessionKind,
  UserRow,
  TorrentCacheRow,
  PlexLoginAttemptRow,
} from "./app-types";

type CreateBookInput = {
  title: string;
  author: string;
};

type CreateReleaseInput = {
  bookId: number;
  provider: string;
  providerGuid?: string | null;
  title: string;
  mediaType: MediaType;
  infoHash: string;
  sizeBytes?: number | null;
  url: string;
  status?: ReleaseStatus;
};

type CreateJobInput = {
  type: JobType;
  status?: JobStatus;
  bookId?: number | null;
  releaseId?: number | null;
  payload?: unknown;
  maxAttempts?: number;
  nextRunAt?: string | null;
};

type UpsertChapterAnalysisInput = {
  assetId: number;
  status: ChapterAnalysisStatus;
  source: string;
  algorithmVersion: string;
  fingerprint: string;
  transcriptFingerprint?: string | null;
  chaptersJson?: string | null;
  debugJson?: string | null;
  resolvedBoundaryCount?: number;
  totalBoundaryCount?: number;
  error?: string | null;
};

type UpsertAssetTranscriptInput = {
  assetId: number;
  status: AssetTranscriptStatus;
  source: string;
  algorithmVersion: string;
  fingerprint: string;
  transcriptPath?: string | null;
  transcriptJson?: string | null;
  error?: string | null;
};

type AddAssetInput = {
  bookId: number;
  kind: "single" | "multi" | "ebook";
  mime: string;
  totalSize: number;
  durationMs?: number | null;
  sourceReleaseId?: number | null;
  // When set, the asset is added as a container in this existing
  // manifestation. When null/undefined, a new one-container manifestation is
  // auto-created so callers that don't know about manifestations get the
  // legacy 1:1 behavior.
  manifestationId?: number | null;
  sequenceInManifestation?: number;
  files: Array<{
    path: string;
    sourcePath?: string | null;
    size: number;
    start: number;
    end: number;
    durationMs: number;
    title?: string | null;
  }>;
};

type AddManifestationInput = {
  bookId: number;
  kind: ManifestationKind;
  label?: string | null;
  editionNote?: string | null;
  durationMs?: number | null;
  totalSize?: number;
  preferredScore?: number;
};

type UpsertUserInput = {
  provider: AuthProvider;
  providerUserId: string;
  username: string;
  displayName?: string | null;
  thumbUrl?: string | null;
  isAdmin?: boolean;
};

function parseIdentifiers(value: string | null): Record<string, string> {
  if (!value) return {};
  try {
    const parsed = JSON.parse(value) as Record<string, string>;
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function stringifySettings(value: AppSettings): string {
  return JSON.stringify(value, null, 2);
}

function normalizeHash(hash: string): string {
  return hash.trim().toLowerCase();
}

function assertPositiveInt(value: number): void {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error("Expected positive integer id");
  }
}

export class BooksRepo {
  constructor(private readonly db: Database) {
    // Foreign-key cascades are part of normal behavior; enforce per connection.
    this.db.exec("PRAGMA foreign_keys = ON;");
  }

  transaction<T>(fn: () => T): T {
    return this.db.transaction(fn)();
  }

  ensureSettings(): AppSettings {
    const existing = this.db.query("SELECT value_json FROM settings WHERE id = 1").get() as
      | { value_json: string }
      | null;
    if (existing) {
      return parseSettings(existing.value_json);
    }

    const defaults = defaultSettings();
    this.db
      .query("INSERT INTO settings (id, value_json) VALUES (1, ?)")
      .run(stringifySettings(defaults));
    return defaults;
  }

  getSettings(): AppSettings {
    return this.ensureSettings();
  }

  getJsonState<T>(key: string): T | null {
    const row = this.db.query("SELECT value_json FROM app_state WHERE key = ?").get(key) as
      | { value_json: string }
      | null;
    if (!row) return null;
    try {
      return JSON.parse(row.value_json) as T;
    } catch {
      return null;
    }
  }

  setJsonState(key: string, value: unknown): void {
    this.db
      .query(
        `INSERT INTO app_state (key, value_json, updated_at)
         VALUES (?, ?, ?)
         ON CONFLICT(key) DO UPDATE SET
           value_json = excluded.value_json,
           updated_at = excluded.updated_at`
      )
      .run(key, JSON.stringify(value), nowIso());
  }

  updateSettings(next: AppSettings): AppSettings {
    this.db
      .query("INSERT INTO settings (id, value_json) VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET value_json = excluded.value_json")
      .run(stringifySettings(next));
    return next;
  }

  upsertUser(input: UpsertUserInput): UserRow {
    const now = nowIso();
    return this.db
      .query(
        `INSERT INTO users (
           provider, provider_user_id, username, display_name, thumb_url, is_admin, created_at, updated_at
         )
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(provider, provider_user_id) DO UPDATE SET
           username = excluded.username,
           display_name = excluded.display_name,
           thumb_url = excluded.thumb_url,
           is_admin = excluded.is_admin,
           updated_at = excluded.updated_at
         RETURNING *`
      )
      .get(
        input.provider,
        input.providerUserId,
        input.username,
        input.displayName ?? null,
        input.thumbUrl ?? null,
        input.isAdmin ? 1 : 0,
        now,
        now
      ) as UserRow;
  }

  getUserById(userId: number): UserRow | null {
    assertPositiveInt(userId);
    return (this.db.query("SELECT * FROM users WHERE id = ?").get(userId) as UserRow | null) ?? null;
  }

  listUsers(provider?: AuthProvider): UserRow[] {
    if (provider) {
      return this.db.query("SELECT * FROM users WHERE provider = ? ORDER BY username COLLATE NOCASE ASC, id ASC").all(provider) as UserRow[];
    }
    return this.db.query("SELECT * FROM users ORDER BY username COLLATE NOCASE ASC, id ASC").all() as UserRow[];
  }

  createSession(userId: number, tokenHash: string, expiresAt: string, kind: SessionKind = "browser"): SessionWithUserRow {
    assertPositiveInt(userId);
    const now = nowIso();
    const row = this.db
      .query(
        `INSERT INTO sessions (user_id, kind, token_hash, expires_at, created_at, last_seen_at)
         VALUES (?, ?, ?, ?, ?, ?)
         RETURNING id`
      )
      .get(userId, kind, tokenHash, expiresAt, now, now) as { id: number } | null;
    if (!row) {
      throw new Error("Failed to create session");
    }
    const session = this.getSessionByTokenHash(tokenHash);
    if (!session) {
      throw new Error("Failed to load created session");
    }
    return session;
  }

  getSessionByTokenHash(tokenHash: string): SessionWithUserRow | null {
    return (
      this.db
        .query(
          `SELECT
             s.id, s.user_id, s.kind, s.token_hash, s.expires_at, s.created_at, s.last_seen_at,
             u.provider, u.provider_user_id, u.username, u.display_name, u.thumb_url, u.is_admin
           FROM sessions s
           JOIN users u ON u.id = s.user_id
           WHERE s.token_hash = ?`
        )
        .get(tokenHash) as SessionWithUserRow | null
    ) ?? null;
  }

  touchSession(sessionId: number): SessionWithUserRow | null {
    assertPositiveInt(sessionId);
    const now = nowIso();
    const row = this.db
      .query(
        `UPDATE sessions
         SET last_seen_at = ?
         WHERE id = ?
         RETURNING token_hash`
      )
      .get(now, sessionId) as { token_hash: string } | null;
    if (!row) return null;
    return this.getSessionByTokenHash(row.token_hash);
  }

  deleteSession(sessionId: number): boolean {
    assertPositiveInt(sessionId);
    return this.db.query("DELETE FROM sessions WHERE id = ?").run(sessionId).changes > 0;
  }

  createPlexLoginAttempt(input: {
    pinId: number;
    clientIdentifier: string;
    publicJwkJson: string;
    privateKeyPkcs8: string;
  }): PlexLoginAttemptRow {
    const now = nowIso();
    return this.db
      .query(
        `INSERT INTO plex_login_attempts (pin_id, client_identifier, public_jwk_json, private_key_pkcs8, created_at)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(pin_id) DO UPDATE SET
           client_identifier = excluded.client_identifier,
           public_jwk_json = excluded.public_jwk_json,
           private_key_pkcs8 = excluded.private_key_pkcs8,
           created_at = excluded.created_at
         RETURNING *`
      )
      .get(input.pinId, input.clientIdentifier, input.publicJwkJson, input.privateKeyPkcs8, now) as PlexLoginAttemptRow;
  }

  getPlexLoginAttempt(pinId: number): PlexLoginAttemptRow | null {
    assertPositiveInt(pinId);
    return (this.db.query("SELECT * FROM plex_login_attempts WHERE pin_id = ?").get(pinId) as PlexLoginAttemptRow | null) ?? null;
  }

  deletePlexLoginAttempt(pinId: number): boolean {
    assertPositiveInt(pinId);
    return this.db.query("DELETE FROM plex_login_attempts WHERE pin_id = ?").run(pinId).changes > 0;
  }

  deleteExpiredPlexLoginAttempts(beforeIso: string): number {
    return this.db.query("DELETE FROM plex_login_attempts WHERE created_at < ?").run(beforeIso).changes;
  }

  createAppLoginAttempt(input: { id: string; redirectUri: string; state: string; expiresAt: string }): AppLoginAttemptRow {
    const now = nowIso();
    return this.db
      .query(
        `INSERT INTO app_login_attempts (id, redirect_uri, state, expires_at, created_at)
         VALUES (?, ?, ?, ?, ?)
         RETURNING *`
      )
      .get(input.id, input.redirectUri, input.state, input.expiresAt, now) as AppLoginAttemptRow;
  }

  getAppLoginAttempt(attemptId: string): AppLoginAttemptRow | null {
    return (this.db.query("SELECT * FROM app_login_attempts WHERE id = ?").get(attemptId) as AppLoginAttemptRow | null) ?? null;
  }

  deleteAppLoginAttempt(attemptId: string): boolean {
    return this.db.query("DELETE FROM app_login_attempts WHERE id = ?").run(attemptId).changes > 0;
  }

  deleteExpiredAppLoginAttempts(beforeIso: string): number {
    return this.db.query("DELETE FROM app_login_attempts WHERE expires_at < ?").run(beforeIso).changes;
  }

  createAuthCode(input: { codeHash: string; userId: number; attemptId: string; expiresAt: string }): AuthCodeRow {
    const now = nowIso();
    return this.db
      .query(
        `INSERT INTO auth_codes (code_hash, user_id, attempt_id, expires_at, used_at, created_at)
         VALUES (?, ?, ?, ?, NULL, ?)
         RETURNING *`
      )
      .get(input.codeHash, input.userId, input.attemptId, input.expiresAt, now) as AuthCodeRow;
  }

  consumeAuthCode(codeHash: string): (AuthCodeRow & { user: UserRow }) | null {
    const now = nowIso();
    const row = this.db
      .query(
        `UPDATE auth_codes
         SET used_at = ?
         WHERE code_hash = ?
           AND used_at IS NULL
           AND expires_at > ?
         RETURNING code_hash, user_id, attempt_id, expires_at, used_at, created_at`
      )
      .get(now, codeHash, now) as AuthCodeRow | null;
    if (!row) return null;
    const user = this.getUserById(row.user_id);
    if (!user) return null;
    return {
      ...row,
      user,
    };
  }

  deleteExpiredAuthCodes(beforeIso: string): number {
    return this.db.query("DELETE FROM auth_codes WHERE expires_at < ? OR used_at IS NOT NULL").run(beforeIso).changes;
  }

  getTorrentCache(key: string): TorrentCacheRow | null {
    return (this.db.query("SELECT * FROM torrent_cache WHERE key = ?").get(key) as TorrentCacheRow | null) ?? null;
  }

  putTorrentCache(input: {
    key: string;
    provider?: string | null;
    providerGuid?: string | null;
    url: string;
    infoHash?: string | null;
    torrentBytes: Uint8Array;
  }): TorrentCacheRow {
    const now = nowIso();
    return this.db
      .query(
        `INSERT INTO torrent_cache (key, provider, provider_guid, url, info_hash, torrent_bytes, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(key) DO UPDATE SET
           provider = excluded.provider,
           provider_guid = excluded.provider_guid,
           url = excluded.url,
           info_hash = excluded.info_hash,
           torrent_bytes = excluded.torrent_bytes,
           updated_at = excluded.updated_at
         RETURNING *`
      )
      .get(
        input.key,
        input.provider ?? null,
        input.providerGuid ?? null,
        input.url,
        input.infoHash ?? null,
        input.torrentBytes,
        now,
        now
      ) as TorrentCacheRow;
  }

  createBook(input: CreateBookInput): BookRow {
    const now = nowIso();
    const row = this.db
      .query(
        "INSERT INTO books (title, author, added_at, updated_at) VALUES (?, ?, ?, ?) RETURNING *"
      )
      .get(input.title, input.author, now, now) as BookRow;
    return row;
  }

  findBookByTitleAuthor(title: string, author: string): BookRow | null {
    return (this.db
      .query("SELECT * FROM books WHERE title = ? AND author = ? ORDER BY id DESC LIMIT 1")
      .get(title, author) as BookRow | null) ?? null;
  }

  findBookByOpenLibraryKey(openLibraryKey: string): BookRow | null {
    const key = openLibraryKey.trim();
    if (!key) return null;
    const rows = this.db
      .query("SELECT * FROM books WHERE identifiers_json IS NOT NULL ORDER BY id DESC")
      .all() as BookRow[];
    return rows.find((row) => parseIdentifiers(row.identifiers_json).openlibrary === key) ?? null;
  }

  updateBookMetadata(
    bookId: number,
    patch: Partial<{
      coverPath: string | null;
      durationMs: number | null;
      wordCount: number | null;
      publishedAt: string | null;
      description: string | null;
      descriptionHtml: string | null;
      language: string | null;
      identifiers: Record<string, string>;
    }>
  ): BookRow {
    assertPositiveInt(bookId);
    const now = nowIso();
    const current = this.getBookRow(bookId);
    if (!current) {
      throw new Error(`Book ${bookId} not found`);
    }

    const row = this.db
      .query(
        `UPDATE books
         SET cover_path = ?,
             duration_ms = ?,
             word_count = ?,
             published_at = ?,
             description = ?,
             description_html = ?,
             language = ?,
             identifiers_json = ?,
             updated_at = ?
         WHERE id = ?
         RETURNING *`
      )
      .get(
        patch.coverPath ?? current.cover_path,
        patch.durationMs ?? current.duration_ms,
        patch.wordCount ?? current.word_count,
        patch.publishedAt ?? current.published_at,
        patch.description ?? current.description,
        patch.descriptionHtml ?? current.description_html,
        patch.language ?? current.language,
        patch.identifiers ? JSON.stringify(patch.identifiers) : current.identifiers_json,
        now,
        bookId
      ) as BookRow;
    return row;
  }

  getBookRow(bookId: number): BookRow | null {
    assertPositiveInt(bookId);
    return (this.db.query("SELECT * FROM books WHERE id = ?").get(bookId) as BookRow | null) ?? null;
  }

  getBook(bookId: number): LibraryBook | null {
    const row = this.getBookRow(bookId);
    if (!row) return null;
    return this.toLibraryBook(row);
  }

  listBooks(limit: number, cursor?: number, q?: string): { items: LibraryBook[]; nextCursor?: number } {
    const safeLimit = Math.max(1, Math.min(200, Math.trunc(limit || 50)));
    const where: string[] = [];
    const args: Array<number | string> = [];

    if (typeof cursor === "number" && cursor > 0) {
      where.push("id < ?");
      args.push(cursor);
    }

    if (q && q.trim()) {
      where.push("(title LIKE ? OR author LIKE ?)");
      const like = `%${q.trim()}%`;
      args.push(like, like);
    }

    const clause = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
    const rows = this.db
      .query(`SELECT * FROM books ${clause} ORDER BY id DESC LIMIT ?`)
      .all(...args, safeLimit) as BookRow[];
    const items = rows.map((row) => this.toLibraryBook(row));
    const nextCursor = rows.length === safeLimit ? rows[rows.length - 1]?.id : undefined;
    return { items, nextCursor };
  }

  listAllBooks(): LibraryBook[] {
    const rows = this.db.query("SELECT * FROM books ORDER BY added_at DESC, id DESC").all() as BookRow[];
    return rows.map((row) => this.toLibraryBook(row));
  }

  listInProgressBooks(bookIds?: number[]): LibraryBook[] {
    const candidates = Array.isArray(bookIds) && bookIds.length > 0
      ? Array.from(new Set(bookIds))
          .map((bookId) => this.getBook(bookId))
          .filter((book): book is LibraryBook => Boolean(book))
      : this.listAllBooks();
    return candidates.filter((book) => {
      if (book.status === "imported") return false;
      if (book.status !== "error") return true;
      return this.hasActiveBookWork(book.id);
    });
  }

  hasActiveBookWork(bookId: number): boolean {
    assertPositiveInt(bookId);
    const row = this.db
      .query(
        `SELECT id
         FROM jobs
         WHERE book_id = ?
           AND status IN ('queued', 'running')
           AND type IN ('acquire', 'download', 'import', 'reconcile')
         LIMIT 1`
      )
      .get(bookId) as { id: number } | null;
    return Boolean(row);
  }

  /**
   * Returns filesystem artifacts owned exclusively by a single book so callers
   * can safely delete files after DB cascade delete.
   */
  getBookDeleteArtifacts(bookId: number): { assetPaths: string[]; transcriptPaths: string[]; coverPath: string | null } {
    assertPositiveInt(bookId);
    const assetPaths = this.db
      .query(
        `SELECT DISTINCT af.path AS path
         FROM asset_files af
         INNER JOIN assets a ON a.id = af.asset_id
         WHERE a.book_id = ?
           AND NOT EXISTS (
             SELECT 1
             FROM asset_files af2
             INNER JOIN assets a2 ON a2.id = af2.asset_id
             WHERE af2.path = af.path
               AND a2.book_id <> ?
           )`
      )
      .all(bookId, bookId) as Array<{ path: string }>;
    const transcriptPaths = this.db
      .query(
        `SELECT DISTINCT at.transcript_path AS path
         FROM asset_transcripts at
         INNER JOIN assets a ON a.id = at.asset_id
         WHERE a.book_id = ?
           AND at.transcript_path IS NOT NULL
           AND at.transcript_path <> ''
           AND NOT EXISTS (
             SELECT 1
             FROM asset_transcripts at2
             INNER JOIN assets a2 ON a2.id = at2.asset_id
             WHERE at2.transcript_path = at.transcript_path
               AND a2.book_id <> ?
           )`
      )
      .all(bookId, bookId) as Array<{ path: string }>;

    const book = this.getBookRow(bookId);
    let coverPath: string | null = null;
    if (book?.cover_path) {
      const shared = this.db
        .query("SELECT id FROM books WHERE id <> ? AND cover_path = ? LIMIT 1")
        .get(bookId, book.cover_path) as { id: number } | null;
      if (!shared) {
        coverPath = book.cover_path;
      }
    }

    return {
      assetPaths: assetPaths.map((row) => row.path),
      transcriptPaths: transcriptPaths.map((row) => row.path),
      coverPath,
    };
  }

  deleteBook(bookId: number): boolean {
    assertPositiveInt(bookId);
    const result = this.db.query("DELETE FROM books WHERE id = ?").run(bookId);
    return Number(result.changes) > 0;
  }

  /**
   * Returns all on-disk artifacts referenced by the current DB so callers can
   * perform a full local-dev wipe including imported files/covers.
   */
  getWipeArtifacts(): { assetPaths: string[]; transcriptPaths: string[]; coverPaths: string[] } {
    const assetPaths = this.db
      .query("SELECT DISTINCT path FROM asset_files WHERE path IS NOT NULL AND path <> ''")
      .all() as Array<{ path: string }>;
    const transcriptPaths = this.db
      .query("SELECT DISTINCT transcript_path FROM asset_transcripts WHERE transcript_path IS NOT NULL AND transcript_path <> ''")
      .all() as Array<{ transcript_path: string }>;
    const coverPaths = this.db
      .query("SELECT DISTINCT cover_path FROM books WHERE cover_path IS NOT NULL AND cover_path <> ''")
      .all() as Array<{ cover_path: string }>;
    return {
      assetPaths: assetPaths.map((row) => row.path),
      transcriptPaths: transcriptPaths.map((row) => row.transcript_path),
      coverPaths: coverPaths.map((row) => row.cover_path),
    };
  }

  /**
   * Clears all mutable Kindling data while preserving settings and schema
   * migration state for local-dev reset workflows.
   */
  wipeDatabase(): {
    deleted: {
      books: number;
      releases: number;
      assets: number;
      assetFiles: number;
      jobs: number;
      torrentCache: number;
      chapterAnalysis: number;
      assetTranscripts: number;
      users: number;
      sessions: number;
      plexLoginAttempts: number;
    };
    settingsPreserved: boolean;
  } {
    return this.db.transaction(() => {
      const countRow = (
        table:
          | "books"
          | "releases"
          | "assets"
          | "asset_files"
          | "jobs"
          | "torrent_cache"
          | "chapter_analysis"
          | "asset_transcripts"
          | "users"
          | "sessions"
          | "plex_login_attempts"
          | "app_login_attempts"
          | "auth_codes"
          | "app_state"
      ) =>
        ((this.db.query(`SELECT COUNT(*) AS count FROM ${table}`).get() as { count: number } | null)?.count ?? 0);
      const deleted = {
        books: countRow("books"),
        releases: countRow("releases"),
        assets: countRow("assets"),
        assetFiles: countRow("asset_files"),
        jobs: countRow("jobs"),
        torrentCache: countRow("torrent_cache"),
        chapterAnalysis: countRow("chapter_analysis"),
        assetTranscripts: countRow("asset_transcripts"),
        users: countRow("users"),
        sessions: countRow("sessions"),
        plexLoginAttempts: countRow("plex_login_attempts"),
        appLoginAttempts: countRow("app_login_attempts"),
        authCodes: countRow("auth_codes"),
        appState: countRow("app_state"),
      };

      this.db.query("DELETE FROM auth_codes").run();
      this.db.query("DELETE FROM app_login_attempts").run();
      this.db.query("DELETE FROM plex_login_attempts").run();
      this.db.query("DELETE FROM sessions").run();
      this.db.query("DELETE FROM users").run();
      this.db.query("DELETE FROM app_state").run();
      this.db.query("DELETE FROM asset_transcripts").run();
      this.db.query("DELETE FROM chapter_analysis").run();
      this.db.query("DELETE FROM torrent_cache").run();
      this.db.query("DELETE FROM jobs").run();
      this.db.query("DELETE FROM asset_files").run();
      this.db.query("DELETE FROM assets").run();
      this.db.query("DELETE FROM releases").run();
      this.db.query("DELETE FROM books").run();
      this.db
        .query(
          "DELETE FROM sqlite_sequence WHERE name IN ('books', 'releases', 'assets', 'asset_files', 'jobs', 'chapter_analysis', 'asset_transcripts', 'users', 'sessions')"
        )
        .run();

      const settingsPreserved =
        ((this.db.query("SELECT id FROM settings WHERE id = 1").get() as { id: number } | null)?.id ?? null) === 1;
      return { deleted, settingsPreserved };
    })();
  }

  createRelease(input: CreateReleaseInput): ReleaseRow {
    const now = nowIso();
    const hash = normalizeHash(input.infoHash);
    const row = this.db
      .query(
        `INSERT INTO releases (book_id, provider, provider_guid, title, media_type, info_hash, size_bytes, url, snatched_at, status, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         RETURNING *`
      )
      .get(
        input.bookId,
        input.provider,
        input.providerGuid ?? null,
        input.title,
        input.mediaType,
        hash,
        input.sizeBytes ?? null,
        input.url,
        now,
        input.status ?? "snatched",
        now
      ) as ReleaseRow;
    return row;
  }

  findReleaseByInfoHash(infoHash: string): ReleaseRow | null {
    const hash = normalizeHash(infoHash);
    return (this.db.query("SELECT * FROM releases WHERE info_hash = ?").get(hash) as ReleaseRow | null) ?? null;
  }

  findReleaseByProviderGuid(provider: string, providerGuid: string): ReleaseRow | null {
    return (this.db
      .query("SELECT * FROM releases WHERE provider = ? AND provider_guid = ?")
      .get(provider, providerGuid) as ReleaseRow | null) ?? null;
  }

  getRelease(releaseId: number): ReleaseRow | null {
    assertPositiveInt(releaseId);
    return (this.db.query("SELECT * FROM releases WHERE id = ?").get(releaseId) as ReleaseRow | null) ?? null;
  }

  listReleasesByBook(bookId: number): ReleaseRow[] {
    assertPositiveInt(bookId);
    return this.db
      .query("SELECT * FROM releases WHERE book_id = ? ORDER BY id DESC")
      .all(bookId) as ReleaseRow[];
  }

  setReleaseStatus(releaseId: number, status: ReleaseStatus, error?: string | null): ReleaseRow {
    assertPositiveInt(releaseId);
    const now = nowIso();
    const row = this.db
      .query("UPDATE releases SET status = ?, error = ?, updated_at = ? WHERE id = ? RETURNING *")
      .get(status, error ?? null, now, releaseId) as ReleaseRow;
    return row;
  }

  findReleaseById(releaseId: number): ReleaseRow | null {
    return this.getRelease(releaseId);
  }

  findReleasesDownloadedWithoutAssets(): ReleaseRow[] {
    return this.db
      .query(
        `SELECT r.*
         FROM releases r
         LEFT JOIN assets a ON a.source_release_id = r.id
         WHERE r.status = 'downloaded' AND a.id IS NULL
         ORDER BY r.id ASC`
      )
      .all() as ReleaseRow[];
  }

  addAsset(input: AddAssetInput): AssetRow {
    assertPositiveInt(input.bookId);
    if (input.files.length === 0) {
      throw new Error("Asset requires at least one file");
    }
    const now = nowIso();
    return this.db.transaction(() => {
      // Resolve the manifestation we attach this container to. If the caller
      // didn't supply one, auto-create a single-container manifestation that
      // mirrors the asset 1:1 — the legacy shape every existing caller assumes.
      let manifestationId = input.manifestationId ?? null;
      let sequence = input.sequenceInManifestation ?? 0;
      if (manifestationId == null) {
        const manifestationKind: ManifestationKind = input.kind === "ebook" ? "ebook" : "audio";
        const created = this.db
          .query(
            `INSERT INTO manifestations (book_id, kind, label, edition_note, duration_ms, total_size, preferred_score, created_at, updated_at)
             VALUES (?, ?, NULL, NULL, ?, ?, 0, ?, ?)
             RETURNING id`
          )
          .get(
            input.bookId,
            manifestationKind,
            input.durationMs ?? null,
            input.totalSize,
            now,
            now
          ) as { id: number };
        manifestationId = created.id;
        sequence = 0;
      }

      const asset = this.db
        .query(
          `INSERT INTO assets (book_id, kind, mime, total_size, duration_ms, source_release_id, manifestation_id, sequence_in_manifestation, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           RETURNING *`
        )
        .get(
          input.bookId,
          input.kind,
          input.mime,
          input.totalSize,
          input.durationMs ?? null,
          input.sourceReleaseId ?? null,
          manifestationId,
          sequence,
          now,
          now
        ) as AssetRow;

      const insertFile = this.db.query(
        `INSERT INTO asset_files (asset_id, path, source_path, size, start, end, duration_ms, title, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );
      for (const file of input.files) {
        insertFile.run(
          asset.id,
          file.path,
          file.sourcePath ?? null,
          file.size,
          file.start,
          file.end,
          file.durationMs,
          file.title ?? null,
          now
        );
      }

      this.db
        .query("UPDATE books SET updated_at = ?, duration_ms = COALESCE(?, duration_ms) WHERE id = ?")
        .run(now, input.durationMs ?? null, input.bookId);

      // Refresh aggregate manifestation stats. For a brand-new manifestation
      // this is a no-op since the seed values already match. For an existing
      // one (when a caller attaches a new container to it later), this picks
      // up the new container's contribution.
      this.recomputeManifestationAggregates(manifestationId);

      return asset;
    })();
  }

  addManifestation(input: AddManifestationInput): ManifestationRow {
    assertPositiveInt(input.bookId);
    const now = nowIso();
    return this.db
      .query(
        `INSERT INTO manifestations (book_id, kind, label, edition_note, duration_ms, total_size, preferred_score, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
         RETURNING *`
      )
      .get(
        input.bookId,
        input.kind,
        input.label ?? null,
        input.editionNote ?? null,
        input.durationMs ?? null,
        input.totalSize ?? 0,
        input.preferredScore ?? 0,
        now,
        now
      ) as ManifestationRow;
  }

  getManifestation(manifestationId: number): ManifestationRow | null {
    assertPositiveInt(manifestationId);
    return (this.db.query("SELECT * FROM manifestations WHERE id = ?").get(manifestationId) as ManifestationRow | null) ?? null;
  }

  listManifestationsByBook(bookId: number): ManifestationRow[] {
    assertPositiveInt(bookId);
    return this.db
      .query("SELECT * FROM manifestations WHERE book_id = ? ORDER BY preferred_score DESC, created_at DESC, id DESC")
      .all(bookId) as ManifestationRow[];
  }

  listAssetsByManifestation(manifestationId: number): AssetRow[] {
    assertPositiveInt(manifestationId);
    return this.db
      .query("SELECT * FROM assets WHERE manifestation_id = ? ORDER BY sequence_in_manifestation ASC, id ASC")
      .all(manifestationId) as AssetRow[];
  }

  getManifestationWithContainers(
    manifestationId: number
  ): { manifestation: ManifestationRow; containers: Array<{ asset: AssetRow; files: AssetFileRow[] }> } | null {
    const manifestation = this.getManifestation(manifestationId);
    if (!manifestation) return null;
    const assets = this.listAssetsByManifestation(manifestationId);
    const containers = assets.map((asset) => ({ asset, files: this.getAssetFiles(asset.id) }));
    return { manifestation, containers };
  }

  // Sums duration / size across the manifestation's containers and writes
  // them back. Called whenever a container is added/removed/changed.
  private recomputeManifestationAggregates(manifestationId: number): void {
    const row = this.db
      .query(
        `SELECT
           COALESCE(SUM(duration_ms), 0) AS duration_ms,
           COALESCE(SUM(total_size), 0) AS total_size,
           COUNT(*) AS container_count
         FROM assets WHERE manifestation_id = ?`
      )
      .get(manifestationId) as { duration_ms: number; total_size: number; container_count: number };
    const durationMs = row.container_count > 0 && row.duration_ms > 0 ? row.duration_ms : null;
    this.db
      .query("UPDATE manifestations SET duration_ms = ?, total_size = ?, updated_at = ? WHERE id = ?")
      .run(durationMs, row.total_size, nowIso(), manifestationId);
  }

  getAsset(assetId: number): AssetRow | null {
    assertPositiveInt(assetId);
    return (this.db.query("SELECT * FROM assets WHERE id = ?").get(assetId) as AssetRow | null) ?? null;
  }

  deleteAsset(assetId: number): boolean {
    assertPositiveInt(assetId);
    const result = this.db.query("DELETE FROM assets WHERE id = ?").run(assetId);
    return Number(result.changes) > 0;
  }

  getAssetWithFiles(assetId: number): { asset: AssetRow; files: AssetFileRow[] } | null {
    const asset = this.getAsset(assetId);
    if (!asset) return null;
    const files = this.getAssetFiles(assetId);
    return { asset, files };
  }

  getBookByAsset(assetId: number): BookRow | null {
    assertPositiveInt(assetId);
    return (this.db
      .query(
        `SELECT b.*
         FROM books b
         INNER JOIN assets a ON a.book_id = b.id
         WHERE a.id = ?`
      )
      .get(assetId) as BookRow | null) ?? null;
  }

  listAssetsByBook(bookId: number): AssetRow[] {
    assertPositiveInt(bookId);
    return this.db
      .query("SELECT * FROM assets WHERE book_id = ? ORDER BY created_at DESC, id DESC")
      .all(bookId) as AssetRow[];
  }

  getAssetFiles(assetId: number): AssetFileRow[] {
    assertPositiveInt(assetId);
    return this.db
      .query("SELECT * FROM asset_files WHERE asset_id = ? ORDER BY start ASC, id ASC")
      .all(assetId) as AssetFileRow[];
  }

  hasAssetFilePath(filePath: string): boolean {
    const row = this.db
      .query("SELECT id FROM asset_files WHERE path = ? LIMIT 1")
      .get(filePath) as { id: number } | null;
    return Boolean(row);
  }

  createJob(input: CreateJobInput): JobRow {
    const now = nowIso();
    const row = this.db
      .query(
        `INSERT INTO jobs (type, status, book_id, release_id, payload_json, error, attempt_count, max_attempts, next_run_at, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, NULL, 0, ?, ?, ?, ?)
         RETURNING *`
      )
      .get(
        input.type,
        input.status ?? "queued",
        input.bookId ?? null,
        input.releaseId ?? null,
        input.payload === undefined ? null : JSON.stringify(input.payload),
        input.maxAttempts ?? 5,
        input.nextRunAt ?? null,
        now,
        now
      ) as JobRow;
    return row;
  }

  findQueuedOrRunningJobByAsset(type: JobType, assetId: number): JobRow | null {
    const rows = this.listJobsByType(type).filter((job) => job.status === "queued" || job.status === "running");
    for (const row of rows) {
      try {
        const payload = row.payload_json ? (JSON.parse(row.payload_json) as Record<string, unknown>) : {};
        if (Number(payload.assetId) === assetId) return row;
      } catch {
        // ignore malformed payloads
      }
    }
    return null;
  }

  getJob(jobId: number): JobRow | null {
    assertPositiveInt(jobId);
    return (this.db.query("SELECT * FROM jobs WHERE id = ?").get(jobId) as JobRow | null) ?? null;
  }

  listJobsByType(type: JobType): JobRow[] {
    return this.db
      .query("SELECT * FROM jobs WHERE type = ? ORDER BY id DESC")
      .all(type) as JobRow[];
  }

  markJobSucceeded(jobId: number): JobRow {
    const now = nowIso();
    return this.db
      .query("UPDATE jobs SET status = 'succeeded', error = NULL, updated_at = ? WHERE id = ? RETURNING *")
      .get(now, jobId) as JobRow;
  }

  markJobCancelled(jobId: number, error?: string | null): JobRow {
    assertPositiveInt(jobId);
    const now = nowIso();
    return this.db
      .query("UPDATE jobs SET status = 'cancelled', error = ?, updated_at = ? WHERE id = ? RETURNING *")
      .get(error ?? null, now, jobId) as JobRow;
  }

  markJobFailed(jobId: number, error: string, nextRunAt?: string | null): JobRow {
    const now = nowIso();
    return this.db
      .query(
        `UPDATE jobs
         SET status = CASE WHEN attempt_count + 1 >= max_attempts THEN 'failed' ELSE 'queued' END,
             error = ?,
             attempt_count = attempt_count + 1,
             next_run_at = CASE WHEN attempt_count + 1 >= max_attempts THEN NULL ELSE ? END,
             updated_at = ?
         WHERE id = ?
         RETURNING *`
      )
      .get(error, nextRunAt ?? null, now, jobId) as JobRow;
  }

  retryJob(jobId: number): JobRow {
    const now = nowIso();
    return this.db
      .query(
        "UPDATE jobs SET status = 'queued', next_run_at = ?, error = NULL, updated_at = ? WHERE id = ? RETURNING *"
      )
      .get(now, now, jobId) as JobRow;
  }

  rescheduleJob(jobId: number, nextRunAt: string, payload?: unknown): JobRow {
    assertPositiveInt(jobId);
    const now = nowIso();
    return this.db
      .query(
        "UPDATE jobs SET status = 'queued', next_run_at = ?, payload_json = ?, error = NULL, updated_at = ? WHERE id = ? RETURNING *"
      )
      .get(nextRunAt, payload === undefined ? this.getJob(jobId)?.payload_json ?? null : JSON.stringify(payload), now, jobId) as JobRow;
  }

  requeueRunningJobs(): number {
    const now = nowIso();
    const result = this.db
      .query("UPDATE jobs SET status = 'queued', next_run_at = ?, updated_at = ? WHERE status = 'running'")
      .run(now, now);
    return Number(result.changes);
  }

  listRunnableJobs(now = nowIso(), limit = 10): JobRow[] {
    return this.db
      .query(
        `SELECT * FROM jobs
         WHERE status = 'queued'
           AND (next_run_at IS NULL OR next_run_at <= ?)
         ORDER BY created_at ASC, id ASC
         LIMIT ?`
      )
      .all(now, limit) as JobRow[];
  }

  claimQueuedJob(jobId: number, now = nowIso()): JobRow | null {
    assertPositiveInt(jobId);
    return this.db.transaction(() => {
      return (this.db
        .query(
          `UPDATE jobs
           SET status = 'running', updated_at = ?
           WHERE id = ? AND status = 'queued'
           RETURNING *`
        )
        .get(now, jobId) as JobRow | null) ?? null;
    })();
  }

  claimNextRunnableJob(now = nowIso()): JobRow | null {
    const candidate = this.listRunnableJobs(now, 1)[0];
    if (!candidate) return null;
    return this.claimQueuedJob(candidate.id, nowIso());
  }

  listDownloads(): DownloadView[] {
    return this.db
      .query(
        `SELECT
          j.id AS job_id,
          j.type AS job_type,
          j.status AS job_status,
          j.error AS job_error,
          r.id AS release_id,
          r.status AS release_status,
          r.error AS release_error,
          r.media_type AS media_type,
          r.info_hash AS info_hash,
          r.book_id AS book_id
         FROM jobs j
         LEFT JOIN releases r ON r.id = j.release_id
         WHERE j.type = 'download'
         ORDER BY j.id DESC`
      )
      .all() as DownloadView[];
  }

  getDownload(jobId: number): DownloadView | null {
    assertPositiveInt(jobId);
    return (this.db
      .query(
        `SELECT
          j.id AS job_id,
          j.type AS job_type,
          j.status AS job_status,
          j.error AS job_error,
          r.id AS release_id,
          r.status AS release_status,
          r.error AS release_error,
          r.media_type AS media_type,
          r.info_hash AS info_hash,
          r.book_id AS book_id
         FROM jobs j
         LEFT JOIN releases r ON r.id = j.release_id
         WHERE j.type = 'download' AND j.id = ?`
      )
      .get(jobId) as DownloadView | null) ?? null;
  }

  getHealthSummary(): {
    jobs: Record<string, number>;
    releases: Record<string, number>;
    queueSize: number;
  } {
    const jobsRows = this.db
      .query("SELECT status, COUNT(*) AS c FROM jobs GROUP BY status")
      .all() as Array<{ status: string; c: number }>;
    const releaseRows = this.db
      .query("SELECT status, COUNT(*) AS c FROM releases GROUP BY status")
      .all() as Array<{ status: string; c: number }>;
    const queueRow = this.db
      .query("SELECT COUNT(*) AS c FROM jobs WHERE status = 'queued'")
      .get() as { c: number };
    const jobs = Object.fromEntries(jobsRows.map((row) => [row.status, row.c]));
    const releases = Object.fromEntries(releaseRows.map((row) => [row.status, row.c]));
    return {
      jobs,
      releases,
      queueSize: queueRow?.c ?? 0,
    };
  }

  getChapterAnalysis(assetId: number): ChapterAnalysisRow | null {
    assertPositiveInt(assetId);
    return (this.db.query("SELECT * FROM chapter_analysis WHERE asset_id = ?").get(assetId) as ChapterAnalysisRow | null) ?? null;
  }

  getAssetTranscript(assetId: number): AssetTranscriptRow | null {
    assertPositiveInt(assetId);
    return (this.db.query("SELECT * FROM asset_transcripts WHERE asset_id = ?").get(assetId) as AssetTranscriptRow | null) ?? null;
  }

  upsertAssetTranscript(input: UpsertAssetTranscriptInput): AssetTranscriptRow {
    assertPositiveInt(input.assetId);
    const now = nowIso();
    return this.db
      .query(
        `INSERT INTO asset_transcripts (
           asset_id, status, source, algorithm_version, fingerprint,
           transcript_path, transcript_json, error, updated_at
         )
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(asset_id) DO UPDATE SET
           status = excluded.status,
           source = excluded.source,
           algorithm_version = excluded.algorithm_version,
           fingerprint = excluded.fingerprint,
           transcript_path = excluded.transcript_path,
           transcript_json = excluded.transcript_json,
           error = excluded.error,
           updated_at = excluded.updated_at
         RETURNING *`
      )
      .get(
        input.assetId,
        input.status,
        input.source,
        input.algorithmVersion,
        input.fingerprint,
        input.transcriptPath ?? null,
        input.transcriptJson ?? null,
        input.error ?? null,
        now
      ) as AssetTranscriptRow;
  }

  upsertChapterAnalysis(input: UpsertChapterAnalysisInput): ChapterAnalysisRow {
    assertPositiveInt(input.assetId);
    const now = nowIso();
    return this.db
      .query(
        `INSERT INTO chapter_analysis (
           asset_id, status, source, algorithm_version, fingerprint,
           transcript_fingerprint, chapters_json, debug_json, resolved_boundary_count, total_boundary_count, error, updated_at
         )
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(asset_id) DO UPDATE SET
           status = excluded.status,
           source = excluded.source,
           algorithm_version = excluded.algorithm_version,
           fingerprint = excluded.fingerprint,
           transcript_fingerprint = excluded.transcript_fingerprint,
           chapters_json = excluded.chapters_json,
           debug_json = excluded.debug_json,
           resolved_boundary_count = excluded.resolved_boundary_count,
           total_boundary_count = excluded.total_boundary_count,
           error = excluded.error,
           updated_at = excluded.updated_at
         RETURNING *`
      )
      .get(
        input.assetId,
        input.status,
        input.source,
        input.algorithmVersion,
        input.fingerprint,
        input.transcriptFingerprint ?? null,
        input.chaptersJson ?? null,
        input.debugJson ?? null,
        input.resolvedBoundaryCount ?? 0,
        input.totalBoundaryCount ?? 0,
        input.error ?? null,
        now
      ) as ChapterAnalysisRow;
  }

  private toLibraryBook(row: BookRow): LibraryBook {
    const releases = this.listReleasesByBook(row.id);
    const assets = this.listAssetsByBook(row.id);

    const audioStatuses = releases
      .filter((release) => release.media_type === "audio")
      .map((release) => release.status);
    const ebookStatuses = releases
      .filter((release) => release.media_type === "ebook")
      .map((release) => release.status);

    const hasAudioAsset = assets.some((asset) => asset.kind === "single" || asset.kind === "multi");
    const hasEbookAsset = assets.some((asset) => asset.kind === "ebook");

    const audioStatus = deriveMediaStatus({
      mediaType: "audio",
      releases: audioStatuses,
      hasAsset: hasAudioAsset,
    });
    const ebookStatus = deriveMediaStatus({
      mediaType: "ebook",
      releases: ebookStatuses,
      hasAsset: hasEbookAsset,
    });

    return {
      id: row.id,
      title: row.title,
      author: row.author,
      coverUrl: row.cover_path ? `/covers/${row.id}.jpg` : null,
      durationMs: row.duration_ms,
      wordCount: row.word_count,
      addedAt: row.added_at,
      updatedAt: row.updated_at,
      publishedAt: row.published_at,
      description: row.description,
      descriptionHtml: row.description_html,
      language: row.language,
      identifiers: parseIdentifiers(row.identifiers_json),
      audioStatus,
      ebookStatus,
      status: deriveBookStatus(audioStatus as MediaStatus, ebookStatus as MediaStatus),
      fullPseudoProgress: pseudoProgressForBook(audioStatus as MediaStatus, ebookStatus as MediaStatus),
    };
  }
}
