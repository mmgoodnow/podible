import { createHash } from "node:crypto";
import path from "node:path";

export interface ReuseRootSnapshot {
  path: string;
  device: string;
  inode: string;
  marker: { name: string; ownerId: string; sha256: string; device: string; inode: string };
}

export interface ReuseFileIdentity {
  device: string;
  inode: string;
  size: number;
  mtimeNs: string;
  ctimeNs: string;
  sha256?: string | null;
}

export interface ReuseSourceFile {
  id: string;
  path: string;
  size: number;
  sourceTorrentId?: string;
  root: ReuseRootSnapshot;
  identity: ReuseFileIdentity;
}

export interface ReuseCandidateManifest {
  identity: { v1?: string; v2?: string };
  /** Paths are exactly the paths relative to the candidate download directory. */
  files: Array<{ path: string; size: number; role?: "content" | "sidecar" }>;
  v1?: { pieceLength: number; pieceHashes: string[] };
}

export interface ReusePolicy {
  version: 1;
  minReuseRatio: number;
  maxExtraDownloadBytes: number;
}

export interface ReusePlanFile {
  candidatePath: string;
  size: number;
  role: "content" | "sidecar";
  offset: number;
  source?: ReuseSourceFile;
  alternatives: string[];
  proof: "likely" | "verified-full" | "verified-partial" | "mismatch" | "missing";
  verifiedBytes: number;
}

export interface ReusePieceEvidence {
  index: number;
  status: "verified" | "mismatch" | "unavailable";
  actualSha1?: string;
}

export interface ReusePlan {
  version: 1;
  id: string;
  candidate: ReuseCandidateManifest;
  sources: ReuseSourceFile[];
  bindings: Array<{ candidatePath: string; sourceId: string }>;
  policy: ReusePolicy;
  files: ReusePlanFile[];
  verification: "pending" | "v1-pieces" | "unavailable";
  pieces: ReusePieceEvidence[];
  summary: {
    totalBytes: number;
    likelyReuseBytes: number;
    verifiedReuseBytes: number;
    /** Conservative download budget, including pieces crossing absent files. */
    extraDownloadBytesUpperBound: number;
    missingFileBytes: number;
  };
  status: "verification-required" | "ready" | "rejected";
  reasons: string[];
}

export function assertReusePath(value: string): void {
  if (
    !value || value.length > 4096 || /[\x00-\x1f\\:]/.test(value) ||
    value.split("/").some((part) => !part || part === "." || part === ".." || part.length > 255) ||
    path.posix.isAbsolute(value)
  ) throw new Error("Unsafe candidate path");
}

function safeSize(value: number) {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error("Invalid reuse file size");
}

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object")
    return `{${Object.entries(value).filter(([, entry]) => entry !== undefined)
      .sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)
      .map(([key, entry]) => `${JSON.stringify(key)}:${canonical(entry)}`).join(",")}}`;
  return JSON.stringify(value);
}

export function reuseDigest(value: unknown): string {
  return createHash("sha256").update(canonical(value)).digest("hex");
}

function validateManifest(candidate: ReuseCandidateManifest) {
  if (!candidate.files.length || candidate.files.length > 100_000)
    throw new Error("Invalid candidate file count");
  if ((!candidate.identity.v1 && !candidate.identity.v2) ||
    (candidate.identity.v1 && !/^[a-f0-9]{40}$/.test(candidate.identity.v1)) ||
    (candidate.identity.v2 && !/^[a-f0-9]{64}$/.test(candidate.identity.v2)))
    throw new Error("Invalid candidate torrent identity");
  const paths = new Set<string>();
  let total = 0;
  for (const file of candidate.files) {
    assertReusePath(file.path);
    safeSize(file.size);
    if (file.role !== undefined && file.role !== "content" && file.role !== "sidecar")
      throw new Error("Invalid candidate file role");
    const normalized = file.path.normalize("NFC").toLowerCase();
    if (paths.has(normalized)) throw new Error("Candidate path collision");
    paths.add(normalized);
    total += file.size;
  }
  safeSize(total);
  for (const file of paths) {
    const components = file.split("/");
    for (let i = 1; i < components.length; i++)
      if (paths.has(components.slice(0, i).join("/"))) throw new Error("Candidate path collision");
  }
  if (candidate.v1) {
    const { pieceLength, pieceHashes } = candidate.v1;
    if (!candidate.identity.v1 || !Number.isSafeInteger(pieceLength) || pieceLength <= 0 ||
      pieceHashes.length !== Math.ceil(total / pieceLength) ||
      pieceHashes.some((hash) => !/^[a-f0-9]{40}$/.test(hash)))
      throw new Error("Invalid original v1 piece manifest");
  }
}

/** Pure candidate-layout planning; a filename or equal size is never proof. */
export function planContentReuse(input: {
  candidate: ReuseCandidateManifest;
  sources: ReuseSourceFile[];
  bindings?: Array<{ candidatePath: string; sourceId: string }>;
  policy: ReusePolicy;
}): ReusePlan {
  const { candidate, policy } = structuredClone(input);
  validateManifest(candidate);
  if (policy.version !== 1 || !Number.isFinite(policy.minReuseRatio) ||
    policy.minReuseRatio < 0 || policy.minReuseRatio > 1)
    throw new Error("Invalid reuse policy");
  safeSize(policy.maxExtraDownloadBytes);
  const sources = structuredClone(input.sources).sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
  const byId = new Map<string, ReuseSourceFile>();
  for (const source of sources) {
    safeSize(source.size);
    if (!source.id || byId.has(source.id) || source.size !== source.identity.size ||
      (source.identity.sha256 != null && !/^[a-f0-9]{64}$/.test(source.identity.sha256)) ||
      !path.isAbsolute(source.path) || path.resolve(source.path) !== source.path ||
      !path.isAbsolute(source.root.path) || path.resolve(source.root.path) !== source.root.path ||
      !source.path.startsWith(`${source.root.path}${path.sep}`))
      throw new Error("Invalid owned source snapshot");
    byId.set(source.id, source);
  }
  const explicit = new Map<string, string>();
  for (const binding of input.bindings ?? []) {
    if (explicit.has(binding.candidatePath) || !byId.has(binding.sourceId) ||
      !candidate.files.some((file) => file.path === binding.candidatePath))
      throw new Error("Invalid or duplicate reuse binding");
    explicit.set(binding.candidatePath, binding.sourceId);
  }
  let offset = 0;
  const files = candidate.files.map((file): ReusePlanFile => {
    const candidates = sources.filter((source) => source.size === file.size);
    const names = candidates.filter((source) => path.basename(source.path) === path.posix.basename(file.path));
    const selected = explicit.get(file.path);
    // Ambiguous equal-size matches need an explicit domain-level binding.
    const source = selected ? byId.get(selected) : file.role === "sidecar" ? undefined :
      names.length === 1 ? names[0] : candidates.length === 1 ? candidates[0] : undefined;
    const result: ReusePlanFile = {
      candidatePath: file.path, size: file.size, role: file.role ?? "content", offset,
      source, alternatives: candidates.map((entry) => entry.id),
      proof: source ? "likely" : "missing", verifiedBytes: 0,
    };
    offset += file.size;
    return result;
  });
  const bindings = files.filter((file) => file.source).map((file) => ({
    candidatePath: file.candidatePath, sourceId: file.source!.id,
  }));
  const definition = { version: 1, candidate, sources, bindings, policy };
  return finishPlan({
    ...definition, version: 1, id: reuseDigest(definition), files,
    verification: "pending", pieces: [],
    summary: { totalBytes: offset, likelyReuseBytes: 0, verifiedReuseBytes: 0,
      extraDownloadBytesUpperBound: offset, missingFileBytes: 0 },
    status: "verification-required", reasons: [],
  });
}

/** Used by the filesystem verifier; evidence is always recomputed before publication. */
export function applyReusePieceEvidence(plan: ReusePlan, pieces: ReusePieceEvidence[]): ReusePlan {
  const result = structuredClone(plan);
  const manifest = result.candidate.v1;
  if (!manifest) {
    if (pieces.length) throw new Error("Piece evidence without a v1 manifest");
    result.verification = "unavailable";
    return finishPlan(result);
  }
  if (pieces.length !== manifest.pieceHashes.length || pieces.some((entry, index) =>
    entry.index !== index || !["verified", "mismatch", "unavailable"].includes(entry.status) ||
    (entry.status !== "unavailable" && !/^[a-f0-9]{40}$/.test(entry.actualSha1 ?? "")) ||
    (entry.status === "verified" && entry.actualSha1 !== manifest.pieceHashes[index]) ||
    (entry.status === "mismatch" && entry.actualSha1 === manifest.pieceHashes[index])))
    throw new Error("Invalid piece evidence");
  result.verification = "v1-pieces";
  result.pieces = structuredClone(pieces);
  for (const file of result.files) {
    file.verifiedBytes = 0;
    if (!file.source) continue;
    const first = Math.floor(file.offset / manifest.pieceLength);
    const end = Math.ceil((file.offset + file.size) / manifest.pieceLength);
    let mismatch = false;
    for (let index = first; index < end; index++) {
      mismatch ||= pieces[index]?.status === "mismatch";
      if (pieces[index]?.status !== "verified") continue;
      file.verifiedBytes += Math.max(0, Math.min(file.offset + file.size, (index + 1) * manifest.pieceLength) -
        Math.max(file.offset, index * manifest.pieceLength));
    }
    file.proof = file.verifiedBytes === file.size && file.source.size === file.size ? "verified-full" :
      file.verifiedBytes > 0 ? "verified-partial" : mismatch ? "mismatch" : "likely";
  }
  return finishPlan(result);
}

function finishPlan(plan: ReusePlan): ReusePlan {
  const total = plan.summary.totalBytes;
  const likely = plan.files.reduce((sum, file) => sum + (file.source ? Math.min(file.size, file.source.size) : 0), 0);
  const verified = plan.files.reduce((sum, file) => sum + file.verifiedBytes, 0);
  plan.summary = { totalBytes: total, likelyReuseBytes: likely, verifiedReuseBytes: verified,
    extraDownloadBytesUpperBound: total - verified,
    missingFileBytes: plan.files.filter((file) => !file.source).reduce((sum, file) => sum + file.size, 0) };
  const available = plan.verification === "pending" ? likely : verified;
  plan.reasons = [];
  if ((total ? available / total : 1) < plan.policy.minReuseRatio)
    plan.reasons.push("Reuse ratio is below policy minimum");
  if (total - available > plan.policy.maxExtraDownloadBytes)
    plan.reasons.push("Extra download exceeds policy budget");
  plan.status = plan.reasons.length ? "rejected" : plan.verification === "pending" ? "verification-required" : "ready";
  return plan;
}
