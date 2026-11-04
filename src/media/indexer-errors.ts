export type IndexerFailure = {
  provider: string;
  kind: "unavailable" | "configuration";
  status: number | null;
  retryAt: number | null;
  message: string;
};

export const INDEXER_RETRY_MS = 60_000;

/** Error metadata is safe to checkpoint: no request URLs, bodies, or causes. */
export class IndexerError extends Error {
  constructor(readonly failure: IndexerFailure) {
    super(failure.message);
    this.name = "IndexerError";
  }

  get retryAfterMs(): number | null {
    return this.failure.retryAt === null ? null : Math.max(0, this.failure.retryAt - Date.now());
  }

  get retryable(): boolean {
    return this.failure.kind === "unavailable";
  }
}

export class IndexerSearchError extends Error {
  constructor(readonly failures: IndexerFailure[]) {
    super(failures.map((failure) => failure.message).join("; "));
    this.name = "IndexerSearchError";
  }

  get retryAfterMs(): number | null {
    const times = this.failures.flatMap((failure) => failure.retryAt === null ? [] : [failure.retryAt]);
    return times.length ? Math.max(0, Math.min(...times) - Date.now()) : null;
  }

  get retryable(): boolean {
    return this.failures.some((failure) => failure.kind === "unavailable");
  }
}

export function indexerCooldown(provider: string, retryAt: number): IndexerFailure {
  const failure = indexerFailure(provider, null, retryAt);
  return {
    ...failure,
    message: `Indexer ${failure.provider} is temporarily unavailable because Prowlarr reports a cooldown. The cooldown does not identify the upstream cause; check indexer health and test its credentials.`,
  };
}

export function parseIndexerRetryAfter(value: string | null, now = Date.now()): number | null {
  if (!value?.trim()) return null;
  const trimmed = value.trim();
  const delay = /^\d+(?:\.\d+)?$/.test(trimmed) ? Number(trimmed) * 1000 : Date.parse(trimmed) - now;
  return Number.isFinite(delay) ? Math.max(0, delay) : null;
}

export function indexerFailure(
  provider: string,
  status: number | null,
  retryAt: number | null,
): IndexerFailure {
  // Names normally come from configuration, but must not smuggle credential URLs into errors.
  const name = provider.replace(/https?:\/\/\S+/gi, "[indexer]").replace(/[\r\n]/g, " ").slice(0, 120);
  const configuration = status !== null && status !== 408 && status !== 425 && status !== 429 && status < 500;
  return {
    provider: name,
    kind: configuration ? "configuration" : "unavailable",
    status,
    retryAt: configuration ? null : retryAt,
    message: configuration
      ? `Indexer ${name} rejected the request (HTTP ${status}); check its configuration and credentials.`
      : status === 429
        ? `Indexer ${name} is unavailable (HTTP 429). Prowlarr may be blocking an upstream failure, not rate limiting; check indexer health and credentials before retrying.`
        : `Indexer ${name} is temporarily unavailable${status === null ? "" : ` (HTTP ${status})`}; waiting before retrying.`,
  };
}
