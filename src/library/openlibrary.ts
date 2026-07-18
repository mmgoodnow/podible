import type { BookRow, BookSeriesMembership, LibraryBook } from "../app-types";

type OpenLibraryDoc = {
  title?: string;
  author_name?: string[];
  first_publish_year?: number;
  key?: string;
  cover_i?: number;
  series_key?: string[];
  series_name?: string[];
  series_position?: Array<string | number>;
  editions?: {
    docs?: Array<{
      language?: string[];
    }>;
  };
};

type OpenLibraryResponse = {
  docs?: OpenLibraryDoc[];
};

type OpenLibraryWorkResponse = {
  description?: string | { value?: string };
  covers?: number[];
  location?: string;
  series?: Array<{
    series?: { key?: string };
    position?: string | number;
  }>;
};

type OpenLibrarySeriesResponse = {
  name?: string;
};

type OpenLibraryEditionsResponse = {
  entries?: OpenLibraryEdition[];
};

type OpenLibraryEdition = {
  key?: string;
  title?: string;
  subtitle?: string;
  series?: string[];
  covers?: number[];
  publish_date?: string;
  publishers?: string[];
  languages?: Array<{ key?: string }>;
  isbn_10?: string[];
  isbn_13?: string[];
};

export type OpenLibraryMetadata = {
  publishedAt?: string;
  language?: string;
  identifiers?: Record<string, string>;
  description?: string;
  descriptionHtml?: string;
  coverUrl?: string;
  series?: BookSeriesMembership[];
};

export type OpenLibraryCandidate = {
  openLibraryKey: string;
  title: string;
  author: string;
  publishedAt?: string;
  language?: string;
  coverId?: number;
  identifiers: Record<string, string>;
  series: BookSeriesMembership[];
};

export type OpenLibraryCoverCandidate = {
  coverId: number;
  coverUrl: string;
  source: "work" | "edition";
  openLibraryKey: string;
  editionKey?: string;
  title?: string;
  publishDate?: string;
  publisher?: string;
  language?: string;
  isbn?: string;
};

type ResolveOptions = {
  openLibraryKey?: string | null;
  title?: string | null;
  author?: string | null;
};

const SEARCH_FIELDS = [
  "key",
  "title",
  "author_name",
  "first_publish_year",
  "cover_i",
  "series_key",
  "series_name",
  "series_position",
  "editions",
  "editions.language",
].join(",");

const OPEN_LIBRARY_PREFERRED_LANGUAGE = "en";

function normalizeOpenLibraryKey(value: string): string | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (/^\/works\/OL\d+W$/i.test(trimmed)) {
    const suffix = trimmed.slice("/works/".length);
    return `/works/${suffix.toUpperCase()}`;
  }
  if (/^OL\d+W$/i.test(trimmed)) {
    return `/works/${trimmed.toUpperCase()}`;
  }
  return null;
}

export function coverUrlFromId(coverId: number): string {
  return `https://covers.openlibrary.org/b/id/${coverId}-L.jpg`;
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function toDescriptionHtml(value: string): string {
  const paragraphs = value
    .split(/\n{2,}/)
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => `<p>${escapeHtml(part).replace(/\n/g, "<br/>")}</p>`);
  if (paragraphs.length > 0) return paragraphs.join("\n");
  return `<p>${escapeHtml(value)}</p>`;
}

async function fetchSearchDocs(params: Record<string, string>): Promise<OpenLibraryDoc[]> {
  const url = new URL("https://openlibrary.org/search.json");
  url.searchParams.set("fields", SEARCH_FIELDS);
  url.searchParams.set("lang", OPEN_LIBRARY_PREFERRED_LANGUAGE);
  for (const [key, value] of Object.entries(params)) {
    if (value) {
      url.searchParams.set(key, value);
    }
  }

  const response = await fetch(url, { method: "GET" });
  if (!response.ok) return [];
  const payload = (await response.json()) as OpenLibraryResponse;
  return payload.docs ?? [];
}

async function fetchSeriesName(seriesKey: string): Promise<string | null> {
  const key = normalizeOpenLibrarySeriesKey(seriesKey);
  if (!key || !/^OL\d+L$/i.test(key)) return null;
  const response = await fetch(`https://openlibrary.org/series/${key}.json`, { method: "GET" });
  if (!response.ok) return null;
  const payload = (await response.json()) as OpenLibrarySeriesResponse;
  return payload.name?.trim() || null;
}

async function seriesFromWork(payload: OpenLibraryWorkResponse): Promise<BookSeriesMembership[]> {
  const memberships = await Promise.all(
    (payload.series ?? []).map(async (item): Promise<BookSeriesMembership | null> => {
      const rawKey = item.series?.key;
      if (!rawKey) return null;
      const key = normalizeOpenLibrarySeriesKey(rawKey);
      if (!key) return null;
      const name = await fetchSeriesName(key).catch(() => null);
      if (!name) return null;
      return {
        key,
        name,
        position: item.position === undefined ? null : String(item.position),
      };
    })
  );
  return normalizeSeriesMemberships(memberships.filter((item): item is BookSeriesMembership => item !== null));
}

async function fetchWorkDetails(
  openLibraryKey: string
): Promise<{ description?: string; coverId?: number; series: BookSeriesMembership[] } | null> {
  const url = new URL(`https://openlibrary.org${openLibraryKey}.json`);
  const response = await fetch(url, { method: "GET" });
  if (!response.ok) return null;
  const payload = (await response.json()) as OpenLibraryWorkResponse;
  const rawDescription =
    typeof payload.description === "string"
      ? payload.description
      : payload.description && typeof payload.description.value === "string"
        ? payload.description.value
        : undefined;
  const description = rawDescription?.trim() || undefined;
  const coverId = Array.isArray(payload.covers) ? payload.covers.find((value) => Number.isInteger(value) && value > 0) : undefined;
  const series = await seriesFromWork(payload);
  return {
    description,
    coverId,
    series,
  };
}

async function fetchWorkRedirectTarget(openLibraryKey: string): Promise<string | null> {
  const response = await fetch(`https://openlibrary.org${openLibraryKey}.json`, { method: "GET" });
  if (!response.ok) return null;
  const payload = (await response.json()) as OpenLibraryWorkResponse;
  return payload.location ? normalizeOpenLibraryKey(payload.location) : null;
}

async function fetchWorkCoverIds(openLibraryKey: string): Promise<number[]> {
  const url = new URL(`https://openlibrary.org${openLibraryKey}.json`);
  const response = await fetch(url, { method: "GET" });
  if (!response.ok) return [];
  const payload = (await response.json()) as OpenLibraryWorkResponse;
  return Array.isArray(payload.covers) ? payload.covers.filter((value) => Number.isInteger(value) && value > 0) : [];
}

async function fetchWorkEditions(openLibraryKey: string, limit: number): Promise<OpenLibraryEdition[]> {
  const url = new URL(`https://openlibrary.org${openLibraryKey}/editions.json`);
  url.searchParams.set("limit", String(Math.max(1, Math.min(200, Math.trunc(limit || 50)))));
  const response = await fetch(url, { method: "GET" });
  if (!response.ok) return [];
  const payload = (await response.json()) as OpenLibraryEditionsResponse;
  return payload.entries ?? [];
}

function normalizeOpenLibrarySeriesKey(value: string): string | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (/^\/series\/OL\d+L$/i.test(trimmed)) {
    return trimmed.slice("/series/".length).toUpperCase();
  }
  if (/^OL\d+L$/i.test(trimmed)) {
    return trimmed.toUpperCase();
  }
  return trimmed;
}

function normalizeSeriesMemberships(series: BookSeriesMembership[]): BookSeriesMembership[] {
  const seen = new Set<string>();
  const out: BookSeriesMembership[] = [];
  for (const item of series) {
    const name = item.name.trim();
    if (!name) continue;
    const key = item.key ? normalizeOpenLibrarySeriesKey(item.key) : null;
    const position = item.position?.trim() || null;
    const dedupeKey = key ? `key:${key}` : `name:${name.toLowerCase()}`;
    if (seen.has(dedupeKey)) continue;
    seen.add(dedupeKey);
    out.push({ key, name, position });
  }
  return out;
}

function seriesFromDoc(doc: OpenLibraryDoc): BookSeriesMembership[] {
  const names = Array.isArray(doc.series_name) ? doc.series_name : [];
  const keys = Array.isArray(doc.series_key) ? doc.series_key : [];
  const positions = Array.isArray(doc.series_position) ? doc.series_position : [];
  return normalizeSeriesMemberships(
    names.map((name, index) => ({
      key: typeof keys[index] === "string" ? keys[index] : null,
      name,
      position: positions[index] === undefined ? null : String(positions[index]),
    }))
  );
}

type EditionSeriesEvidence = {
  name: string;
  editionKeys: Set<string>;
  nameCounts: Map<string, number>;
  positions: Map<string, Set<string>>;
};

const NUMBER_WORD_POSITIONS: Record<string, string> = {
  one: "1",
  first: "1",
  two: "2",
  second: "2",
  three: "3",
  third: "3",
  four: "4",
  fourth: "4",
  five: "5",
  fifth: "5",
  six: "6",
  sixth: "6",
  seven: "7",
  seventh: "7",
  eight: "8",
  eighth: "8",
  nine: "9",
  ninth: "9",
  ten: "10",
  tenth: "10",
  eleven: "11",
  eleventh: "11",
  twelve: "12",
  twelfth: "12",
  thirteen: "13",
  thirteenth: "13",
  fourteen: "14",
  fourteenth: "14",
  fifteen: "15",
  fifteenth: "15",
  sixteen: "16",
  sixteenth: "16",
  seventeen: "17",
  seventeenth: "17",
  eighteen: "18",
  eighteenth: "18",
  nineteen: "19",
  nineteenth: "19",
  twenty: "20",
  twentieth: "20",
};

function normalizeSeriesName(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/\p{Diacritic}/gu, "")
    .toLocaleLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function romanNumeralValue(value: string): number | null {
  if (!/^[ivxlcdm]+$/i.test(value)) return null;
  const values: Record<string, number> = { i: 1, v: 5, x: 10, l: 50, c: 100, d: 500, m: 1_000 };
  let total = 0;
  let previous = 0;
  for (const character of value.toLocaleLowerCase().split("").reverse()) {
    const current = values[character]!;
    total += current < previous ? -current : current;
    previous = current;
  }
  return total > 0 ? total : null;
}

function normalizeSeriesPosition(value: string): string | null {
  const normalized = value.trim().replace(/^#\s*/, "").replace(/[.)]+$/, "").toLocaleLowerCase();
  if (!normalized) return null;
  if (/^\d+(?:\.\d+)?$/.test(normalized)) return String(Number(normalized));
  const wordPosition = NUMBER_WORD_POSITIONS[normalized];
  if (wordPosition) return wordPosition;
  const romanPosition = romanNumeralValue(normalized);
  return romanPosition === null ? null : String(romanPosition);
}

function parseEditionSeriesValue(value: string): { name: string; position: string | null } | null {
  const trimmed = value.replace(/\s+/g, " ").trim();
  if (!trimmed) return null;
  const labeledPosition = trimmed.match(
    /^(.*?)(?:\s*(?:,|--|[-–—:])\s*|\s+)(?:book|bk|volume|vol|tome|part)\.?\s*(#?\s*[a-z0-9.]+)$/i
  );
  const bareDelimitedPosition = trimmed.match(/^(.*?)\s*(?:,|--)\s*(#?\s*[a-z0-9.]+)$/i);
  const match = labeledPosition ?? bareDelimitedPosition;
  if (!match) return { name: trimmed, position: null };
  const position = normalizeSeriesPosition(match[2]!);
  const name = match[1]!.replace(/[\s,;:–—-]+$/, "").trim();
  return name && position ? { name, position } : { name: trimmed, position: null };
}

function addEditionPosition(evidence: EditionSeriesEvidence, position: string, editionKey: string): void {
  const editions = evidence.positions.get(position) ?? new Set<string>();
  editions.add(editionKey);
  evidence.positions.set(position, editions);
}

function seriesFromEditionConsensus(editions: OpenLibraryEdition[]): BookSeriesMembership[] {
  const evidenceByName = new Map<string, EditionSeriesEvidence>();

  editions.forEach((edition, index) => {
    const editionKey = edition.key?.trim() || `edition:${index}`;
    const seenInEdition = new Set<string>();
    const editionSeries = Array.isArray(edition.series) ? edition.series : [];
    for (const rawValue of editionSeries) {
      if (typeof rawValue !== "string") continue;
      const parsed = parseEditionSeriesValue(rawValue);
      if (!parsed) continue;
      const normalizedName = normalizeSeriesName(parsed.name);
      if (!normalizedName || seenInEdition.has(normalizedName)) continue;
      seenInEdition.add(normalizedName);
      const evidence = evidenceByName.get(normalizedName) ?? {
        name: parsed.name,
        editionKeys: new Set<string>(),
        nameCounts: new Map<string, number>(),
        positions: new Map<string, Set<string>>(),
      };
      evidence.editionKeys.add(editionKey);
      evidence.nameCounts.set(parsed.name, (evidence.nameCounts.get(parsed.name) ?? 0) + 1);
      if (parsed.position) addEditionPosition(evidence, parsed.position, editionKey);
      evidenceByName.set(normalizedName, evidence);
    }
  });

  const accepted = [...evidenceByName.entries()].filter(([, evidence]) => evidence.editionKeys.size >= 2);
  for (const [normalizedName, evidence] of accepted) {
    editions.forEach((edition, index) => {
      const parsedSubtitle = edition.subtitle ? parseEditionSeriesValue(edition.subtitle) : null;
      if (!parsedSubtitle?.position || normalizeSeriesName(parsedSubtitle.name) !== normalizedName) return;
      addEditionPosition(evidence, parsedSubtitle.position, edition.key?.trim() || `edition:${index}`);
    });
  }

  return normalizeSeriesMemberships(
    accepted.map(([, evidence]) => {
      const name = [...evidence.nameCounts.entries()].sort(
        ([leftName, leftCount], [rightName, rightCount]) => rightCount - leftCount || leftName.length - rightName.length
      )[0]?.[0] ?? evidence.name;
      const corroboratedPositions = [...evidence.positions.entries()]
        .filter(([, editionKeys]) => editionKeys.size >= 2)
        .map(([position]) => position);
      return {
        key: null,
        name,
        position: corroboratedPositions.length === 1 ? corroboratedPositions[0]! : null,
      };
    })
  );
}

function languageFromMatchedEdition(doc: OpenLibraryDoc): string | undefined {
  return doc.editions?.docs?.[0]?.language?.find((value) => value.trim())?.trim().toLowerCase() || undefined;
}

function docToCandidate(doc: OpenLibraryDoc, fallbackTitle?: string, fallbackAuthor?: string): OpenLibraryCandidate | null {
  const key = typeof doc.key === "string" ? normalizeOpenLibraryKey(doc.key) : null;
  const title = (doc.title ?? fallbackTitle ?? "").trim();
  const author = (doc.author_name?.[0] ?? fallbackAuthor ?? "").trim();
  if (!key || !title || !author) return null;

  const publishedAt = doc.first_publish_year ? `${doc.first_publish_year}-01-01T00:00:00.000Z` : undefined;
  const language = languageFromMatchedEdition(doc);
  const coverId = Number.isInteger(doc.cover_i) && Number(doc.cover_i) > 0 ? Number(doc.cover_i) : undefined;
  const identifiers: Record<string, string> = {
    openlibrary: key,
  };

  return {
    openLibraryKey: key,
    title,
    author,
    publishedAt,
    language,
    coverId,
    identifiers,
    series: seriesFromDoc(doc),
  };
}

function candidateToMetadata(
  candidate: OpenLibraryCandidate,
  details?: { description?: string; coverId?: number; series: BookSeriesMembership[] } | null
): OpenLibraryMetadata {
  const description = details?.description;
  const descriptionHtml = description ? toDescriptionHtml(description) : undefined;
  const coverId = details?.coverId ?? candidate.coverId;

  return {
    publishedAt: candidate.publishedAt,
    language: candidate.language,
    identifiers: { ...candidate.identifiers },
    description,
    descriptionHtml,
    coverUrl: coverId ? coverUrlFromId(coverId) : undefined,
    series: details?.series.length ? details.series : candidate.series,
  };
}

function pickCandidateByKey(candidates: OpenLibraryCandidate[], openLibraryKey: string): OpenLibraryCandidate | null {
  const normalized = normalizeOpenLibraryKey(openLibraryKey);
  if (!normalized) return null;
  for (const candidate of candidates) {
    if (candidate.openLibraryKey === normalized) return candidate;
  }
  return null;
}

export async function searchOpenLibrary(query: string, limit = 20): Promise<OpenLibraryCandidate[]> {
  const trimmed = query.trim();
  if (!trimmed) return [];
  const safeLimit = Math.max(1, Math.min(50, Math.trunc(limit || 20)));
  const docs = await fetchSearchDocs({
    q: trimmed,
    limit: String(safeLimit),
  });
  const out: OpenLibraryCandidate[] = [];
  for (const doc of docs) {
    const candidate = docToCandidate(doc);
    if (candidate) out.push(candidate);
  }
  return out;
}

export async function searchOpenLibrarySeries(
  series: { seriesKey?: string | null; seriesName?: string | null },
  limit = 50
): Promise<OpenLibraryCandidate[]> {
  const safeLimit = Math.max(1, Math.min(100, Math.trunc(limit || 50)));
  const key = series.seriesKey ? normalizeOpenLibrarySeriesKey(series.seriesKey) : null;
  const name = series.seriesName?.trim() || "";
  const query = key ? `series_key:${key}` : name ? `series_name:"${name.replace(/"/g, "")}"` : "";
  if (!query) return [];
  const docs = await fetchSearchDocs({
    q: query,
    limit: String(safeLimit),
  });
  return docs.map((doc) => docToCandidate(doc)).filter((value): value is OpenLibraryCandidate => value !== null);
}

export async function searchOpenLibraryAuthor(authorName: string, limit = 50): Promise<OpenLibraryCandidate[]> {
  const safeLimit = Math.max(1, Math.min(100, Math.trunc(limit || 50)));
  const name = authorName.trim().replace(/"/g, "");
  if (!name) return [];
  const docs = await fetchSearchDocs({
    q: `author:"${name}"`,
    limit: String(safeLimit),
  });
  const seen = new Set<string>();
  const books: OpenLibraryCandidate[] = [];
  for (const doc of docs) {
    const candidate = docToCandidate(doc);
    if (!candidate) continue;
    if (candidate.language && candidate.language !== OPEN_LIBRARY_PREFERRED_LANGUAGE) continue;
    const identity = `${candidate.title.normalize("NFKD").toLocaleLowerCase()}|${candidate.author.normalize("NFKD").toLocaleLowerCase()}`;
    if (seen.has(identity)) continue;
    seen.add(identity);
    books.push(candidate);
  }
  return books;
}

export async function resolveOpenLibraryCandidate(options: ResolveOptions): Promise<OpenLibraryCandidate | null> {
  const openLibraryKey = options.openLibraryKey?.trim() ?? "";
  if (openLibraryKey) {
    const normalized = normalizeOpenLibraryKey(openLibraryKey);
    if (!normalized) {
      throw new Error("Invalid openLibraryKey format");
    }
    const keyed = await searchOpenLibrary(`key:${normalized}`, 5);
    const exactKey = pickCandidateByKey(keyed, normalized);
    if (exactKey) return exactKey;

    const fallback = await searchOpenLibrary(normalized, 5);
    const fallbackExact = pickCandidateByKey(fallback, normalized);
    if (fallbackExact) return fallbackExact;

    const redirectedKey = await fetchWorkRedirectTarget(normalized).catch(() => null);
    if (redirectedKey && redirectedKey !== normalized) {
      const redirected = await searchOpenLibrary(`key:${redirectedKey}`, 5);
      const exactRedirect = pickCandidateByKey(redirected, redirectedKey);
      if (exactRedirect) return exactRedirect;
    }

    return null;
  }

  const title = options.title?.trim() ?? "";
  const author = options.author?.trim() ?? "";
  const query = `${title} ${author}`.trim();
  if (!query) return null;
  const docs = await fetchSearchDocs({
    q: query,
    limit: "1",
  });
  return (
    docs
      .map((doc) => docToCandidate(doc, title, author))
      .find((value): value is OpenLibraryCandidate => value !== null) ?? null
  );
}

export async function fetchOpenLibraryMetadata(
  book: Pick<BookRow, "title" | "author"> & { openLibraryKey?: string | null }
): Promise<OpenLibraryMetadata | null> {
  const candidate = await resolveOpenLibraryCandidate({
    openLibraryKey: book.openLibraryKey ?? null,
    title: book.title,
    author: book.author,
  });
  if (!candidate) return null;

  const details = await fetchWorkDetails(candidate.openLibraryKey).catch(() => null);
  const metadata = candidateToMetadata(candidate, details);
  if (metadata.series?.length) return metadata;

  const editions = await fetchWorkEditions(candidate.openLibraryKey, 50).catch(() => []);
  return { ...metadata, series: seriesFromEditionConsensus(editions) };
}

function languageFromEdition(edition: OpenLibraryEdition): string | undefined {
  const key = edition.languages?.[0]?.key;
  if (!key) return undefined;
  return key.split("/").pop();
}

function firstString(values: string[] | undefined): string | undefined {
  const value = values?.find((item) => item.trim());
  return value?.trim() || undefined;
}

export async function findOpenLibraryCoverCandidates(
  book: Pick<LibraryBook, "title" | "author" | "identifiers">,
  limit = 50
): Promise<OpenLibraryCoverCandidate[]> {
  const requestedLimit = Math.max(1, Math.min(200, Math.trunc(limit || 50)));
  const storedKey = book.identifiers.openlibrary ? normalizeOpenLibraryKey(book.identifiers.openlibrary) : null;
  let candidate: OpenLibraryCandidate | null;
  if (storedKey) {
    candidate = {
      openLibraryKey: storedKey,
      title: book.title,
      author: book.author,
      identifiers: { openlibrary: storedKey },
      series: [],
    };
  } else {
    candidate = await resolveOpenLibraryCandidate({
      title: book.title,
      author: book.author,
    });
  }
  if (!candidate) return [];

  const seen = new Set<number>();
  const results: OpenLibraryCoverCandidate[] = [];
  const addCandidate = (cover: OpenLibraryCoverCandidate) => {
    if (seen.has(cover.coverId) || results.length >= requestedLimit) return;
    seen.add(cover.coverId);
    results.push(cover);
  };

  const workCovers = await fetchWorkCoverIds(candidate.openLibraryKey).catch(() => []);
  for (const coverId of workCovers) {
    addCandidate({
      coverId,
      coverUrl: coverUrlFromId(coverId),
      source: "work",
      openLibraryKey: candidate.openLibraryKey,
      title: candidate.title,
      language: candidate.language,
    });
  }

  if (results.length >= requestedLimit) return results;

  const editions = await fetchWorkEditions(candidate.openLibraryKey, requestedLimit).catch(() => []);
  for (const edition of editions) {
    const covers = Array.isArray(edition.covers)
      ? edition.covers.filter((value) => Number.isInteger(value) && value > 0)
      : [];
    for (const coverId of covers) {
      addCandidate({
        coverId,
        coverUrl: coverUrlFromId(coverId),
        source: "edition",
        openLibraryKey: candidate.openLibraryKey,
        editionKey: typeof edition.key === "string" ? edition.key : undefined,
        title: edition.title?.trim() || candidate.title,
        publishDate: edition.publish_date?.trim() || undefined,
        publisher: firstString(edition.publishers),
        language: languageFromEdition(edition) ?? candidate.language,
        isbn: firstString(edition.isbn_13) ?? firstString(edition.isbn_10),
      });
    }
    if (results.length >= requestedLimit) return results;
  }

  return results;
}
