type LabelValue = string | number | boolean | null | undefined;
type Labels = Record<string, LabelValue>;

const DEFAULT_DURATION_BUCKETS_SECONDS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];

class Counter {
  private values = new Map<string, { labels: Record<string, string>; value: number }>();

  constructor(
    readonly name: string,
    readonly help: string,
    private readonly labelNames: string[]
  ) {}

  inc(labels: Labels, amount = 1): void {
    const normalized = normalizeLabels(this.labelNames, labels);
    const key = labelKey(normalized);
    const current = this.values.get(key);
    if (current) {
      current.value += amount;
      return;
    }
    this.values.set(key, { labels: normalized, value: amount });
  }

  reset(): void {
    this.values.clear();
  }

  render(): string {
    const lines = [`# HELP ${this.name} ${this.help}`, `# TYPE ${this.name} counter`];
    for (const { labels, value } of sortedMetricValues(this.values)) {
      lines.push(`${this.name}${formatLabels(labels)} ${formatNumber(value)}`);
    }
    return lines.join("\n");
  }
}

class Histogram {
  private values = new Map<string, { labels: Record<string, string>; buckets: number[]; sum: number; count: number }>();

  constructor(
    readonly name: string,
    readonly help: string,
    private readonly labelNames: string[],
    private readonly buckets = DEFAULT_DURATION_BUCKETS_SECONDS
  ) {}

  observe(labels: Labels, value: number): void {
    const normalized = normalizeLabels(this.labelNames, labels);
    const key = labelKey(normalized);
    let current = this.values.get(key);
    if (!current) {
      current = { labels: normalized, buckets: Array(this.buckets.length).fill(0), sum: 0, count: 0 };
      this.values.set(key, current);
    }
    for (let i = 0; i < this.buckets.length; i += 1) {
      if (value <= this.buckets[i]) {
        current.buckets[i] += 1;
      }
    }
    current.sum += value;
    current.count += 1;
  }

  reset(): void {
    this.values.clear();
  }

  render(): string {
    const lines = [`# HELP ${this.name} ${this.help}`, `# TYPE ${this.name} histogram`];
    for (const { labels, buckets, sum, count } of sortedMetricValues(this.values)) {
      for (let i = 0; i < this.buckets.length; i += 1) {
        lines.push(`${this.name}_bucket${formatLabels({ ...labels, le: String(this.buckets[i]) })} ${formatNumber(buckets[i])}`);
      }
      lines.push(`${this.name}_bucket${formatLabels({ ...labels, le: "+Inf" })} ${formatNumber(count)}`);
      lines.push(`${this.name}_sum${formatLabels(labels)} ${formatNumber(sum)}`);
      lines.push(`${this.name}_count${formatLabels(labels)} ${formatNumber(count)}`);
    }
    return lines.join("\n");
  }
}

const httpRequests = new Counter("podible_http_requests_total", "Total HTTP requests handled by Podible.", [
  "method",
  "route",
  "status",
]);
const httpDuration = new Histogram("podible_http_request_duration_seconds", "HTTP request duration in seconds.", [
  "method",
  "route",
  "status",
]);
const rpcRequests = new Counter("podible_rpc_requests_total", "Total Podible RPC method dispatches.", [
  "method",
  "transport",
  "status",
  "error_code",
]);
const rpcDuration = new Histogram("podible_rpc_request_duration_seconds", "Podible RPC method duration in seconds.", [
  "method",
  "transport",
  "status",
  "error_code",
]);
const userJourneyActions = new Counter("podible_user_journey_actions_total", "Core user journey actions completed through HTTP or RPC.", [
  "action",
  "status",
]);

const rpcJourneyActions: Record<string, string> = {
  "auth.beginAppLogin": "begin_app_login",
  "auth.exchange": "finish_app_login",
  "auth.logout": "logout",
  "downloads.retry": "retry_download",
  "import.manual": "manual_import",
  "import.reconcile": "reconcile_imports",
  "jobs.retry": "retry_job",
  "library.acquire": "queue_acquire",
  "library.create": "add_book",
  "library.createManifestationFromSearch": "queue_selected_releases",
  "library.delete": "delete_book",
  "library.refresh": "refresh_library",
  "library.reportImportIssue": "report_import_issue",
  "library.requestTranscription": "request_transcription",
  "library.searchReleases": "search_releases",
  "openlibrary.search": "search_openlibrary",
  "openlibrary.setCover": "change_cover",
  "snatch.create": "queue_selected_release",
  "snatch.createGroup": "queue_selected_releases",
};

function normalizeLabels(labelNames: string[], labels: Labels): Record<string, string> {
  const normalized: Record<string, string> = {};
  for (const name of labelNames) {
    normalized[name] = String(labels[name] ?? "");
  }
  return normalized;
}

function labelKey(labels: Record<string, string>): string {
  return Object.keys(labels)
    .sort()
    .map((key) => `${key}=${labels[key]}`)
    .join("\n");
}

function sortedMetricValues<T extends { labels: Record<string, string> }>(values: Map<string, T>): T[] {
  return [...values.values()].sort((a, b) => labelKey(a.labels).localeCompare(labelKey(b.labels)));
}

function formatLabels(labels: Record<string, string>): string {
  const entries = Object.entries(labels);
  if (entries.length === 0) return "";
  return `{${entries.map(([key, value]) => `${key}="${escapeLabelValue(value)}"`).join(",")}}`;
}

function escapeLabelValue(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/\n/g, "\\n").replace(/"/g, '\\"');
}

function formatNumber(value: number): string {
  if (!Number.isFinite(value)) return "0";
  return String(value);
}

function durationSeconds(startedAt: number): number {
  return Math.max(0, (performance.now() - startedAt) / 1000);
}

export function normalizeHttpRoute(pathname: string): string {
  if (pathname === "/") return "/";
  if (pathname === "/metrics") return "/metrics";
  if (pathname === "/feed.xml" || pathname === "/feed.json") return pathname;
  if (pathname === "/login" || pathname === "/logout" || pathname === "/library" || pathname === "/add" || pathname === "/activity") {
    return pathname;
  }
  if (/^\/login\/plex\/(start|loading|complete)$/.test(pathname)) return pathname;
  if (/^\/auth\/app\/[^/]+\/complete$/.test(pathname)) return "/auth/app/:attemptId/complete";
  if (/^\/auth\/app\/[^/]+$/.test(pathname)) return "/auth/app/:attemptId";
  if (/^\/book\/[^/]+\/acquire$/.test(pathname)) return "/book/:bookId/acquire";
  if (/^\/book\/[^/]+$/.test(pathname)) return "/book/:bookId";
  if (/^\/activity\/refresh$/.test(pathname)) return "/activity/refresh";
  if (/^\/admin(\/.*)?$/.test(pathname)) return "/admin";
  if (pathname === "/rpc") return "/rpc";
  if (/^\/rpc\/[^/]+\/[^/]+$/.test(pathname)) return "/rpc/:namespace/:method";
  if (pathname === "/assets") return "/assets";
  if (/^\/stream\/m\/[^/]+$/.test(pathname)) return "/stream/m/:manifestationId";
  if (/^\/stream\/[^/]+$/.test(pathname)) return "/stream/:assetId";
  if (/^\/chapters\/m\/[^/]+$/.test(pathname)) return "/chapters/m/:manifestationId";
  if (/^\/chapters\/[^/]+$/.test(pathname)) return "/chapters/:assetId";
  if (/^\/transcripts\/m\/[^/]+$/.test(pathname)) return "/transcripts/m/:manifestationId";
  if (/^\/transcripts\/[^/]+$/.test(pathname)) return "/transcripts/:assetId";
  if (/^\/covers\/[^/]+$/.test(pathname)) return "/covers/:bookId";
  if (/^\/ebook\/[^/]+$/.test(pathname)) return "/ebook/:assetId";
  return "unknown";
}

export function recordHttpRequest(method: string, pathname: string, status: number, startedAt: number): void {
  const labels = {
    method: method.toUpperCase(),
    route: normalizeHttpRoute(pathname),
    status: String(status),
  };
  httpRequests.inc(labels);
  httpDuration.observe(labels, durationSeconds(startedAt));
}

export function recordRpcRequest(
  method: string,
  transport: string,
  status: "ok" | "error",
  errorCode: number | null,
  startedAt: number
): void {
  const labels = {
    method,
    transport,
    status,
    error_code: errorCode === null ? "" : String(errorCode),
  };
  rpcRequests.inc(labels);
  rpcDuration.observe(labels, durationSeconds(startedAt));
  const action = rpcJourneyActions[method];
  if (action) {
    recordUserJourneyAction(action, status);
  }
}

export function recordUserJourneyAction(action: string, status: "ok" | "error"): void {
  userJourneyActions.inc({ action, status });
}

export function renderPrometheusMetrics(startTime: number): string {
  const uptimeSeconds = Math.max(0, (Date.now() - startTime) / 1000);
  return [
    "# HELP podible_uptime_seconds Podible process uptime in seconds.",
    "# TYPE podible_uptime_seconds gauge",
    `podible_uptime_seconds ${formatNumber(uptimeSeconds)}`,
    httpRequests.render(),
    httpDuration.render(),
    rpcRequests.render(),
    rpcDuration.render(),
    userJourneyActions.render(),
    "",
  ].join("\n");
}

export function resetMetricsForTest(): void {
  httpRequests.reset();
  httpDuration.reset();
  rpcRequests.reset();
  rpcDuration.reset();
  userJourneyActions.reset();
}
