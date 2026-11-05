import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import {
  applyReusePieceEvidence, planContentReuse,
  type ReuseCandidateManifest, type ReusePolicy, type ReuseSourceFile,
} from "../../src/media/reuse-plan";

const sha1 = (text: string) => createHash("sha1").update(text).digest("hex");
const policy: ReusePolicy = { version: 1, minReuseRatio: 0.5, maxExtraDownloadBytes: 100 };
const root = { path: "/owned", device: "1", inode: "2", marker: {
  name: ".owner.json", ownerId: "test", sha256: "a".repeat(64), device: "1", inode: "3",
} };
function source(id: string, filePath: string, size: number): ReuseSourceFile {
  return { id, path: `/owned/${filePath}`, size, root, sourceTorrentId: `torrent-${id}`,
    identity: { device: "1", inode: id, size, mtimeNs: "1", ctimeNs: "2" } };
}
function candidate(files: ReuseCandidateManifest["files"], pieces?: string[], pieceLength = 4): ReuseCandidateManifest {
  return { identity: { v1: "a".repeat(40) }, files,
    ...(pieces ? { v1: { pieceLength, pieceHashes: pieces } } : {}) };
}

test("renamed equal-size matches remain likely, never cryptographically verified", () => {
  const result = planContentReuse({ candidate: candidate([{ path: "pack/E01.mkv", size: 8 }]),
    sources: [source("1", "totally-different-name.mkv", 8)], policy });
  expect(result.files[0].source?.id).toBe("1");
  expect(result.files[0].proof).toBe("likely");
  expect(result.verification).toBe("pending");
  expect(result.summary).toMatchObject({ likelyReuseBytes: 8, verifiedReuseBytes: 0, extraDownloadBytesUpperBound: 8 });
});

test("a real pack selects files from independently owned source torrents", () => {
  const result = planContentReuse({ candidate: candidate([
    { path: "Season/E01.mkv", size: 8 }, { path: "Season/E02.mkv", size: 12 },
    { path: "Season/release.nfo", size: 2, role: "sidecar" },
    { path: "Season/E03.mkv", size: 16 },
  ]), sources: [source("1", "one.mkv", 8), source("2", "two.mkv", 12)], policy });
  expect(result.bindings).toEqual([
    { candidatePath: "Season/E01.mkv", sourceId: "1" },
    { candidatePath: "Season/E02.mkv", sourceId: "2" },
  ]);
  expect(result.files.map((file) => file.source?.sourceTorrentId)).toEqual(["torrent-1", "torrent-2", undefined, undefined]);
  expect(result.files[2].role).toBe("sidecar");
  expect(result.summary.missingFileBytes).toBe(18);
  expect(result.status).toBe("verification-required");
});

test("ambiguous size matches need explicit bindings, and a source may serve multiple candidate paths", () => {
  const input = { candidate: candidate([{ path: "pack/E01.mkv", size: 8 }]),
    sources: [source("1", "a.mkv", 8), source("2", "b.mkv", 8)], policy };
  expect(planContentReuse(input).files[0]).toMatchObject({ proof: "missing", alternatives: ["1", "2"] });
  expect(planContentReuse({ ...input, bindings: [{ candidatePath: "pack/E01.mkv", sourceId: "2" }] })
    .files[0].source?.id).toBe("2");
  expect(() => planContentReuse({ ...input, bindings: [{ candidatePath: "../bad", sourceId: "2" }] })).toThrow("binding");
});

test("same-length changed-tail piece proof is partial, with a bounded repair budget", () => {
  const plan = planContentReuse({ candidate: candidate([{ path: "E01.mkv", size: 8 }], [sha1("abcd"), sha1("efgh")]),
    sources: [source("1", "old.mkv", 8)], policy: { ...policy, maxExtraDownloadBytes: 4 } });
  const checked = applyReusePieceEvidence(plan, [
    { index: 0, status: "verified", actualSha1: sha1("abcd") },
    { index: 1, status: "mismatch", actualSha1: sha1("efgX") },
  ]);
  expect(checked.files[0]).toMatchObject({ proof: "verified-partial", verifiedBytes: 4 });
  expect(checked.summary.extraDownloadBytesUpperBound).toBe(4);
  expect(checked.status).toBe("ready");
  expect(plan.files[0].proof).toBe("likely");
  const rejected = applyReusePieceEvidence({ ...plan, policy: { ...policy, maxExtraDownloadBytes: 3 } }, checked.pieces);
  expect(rejected.status).toBe("rejected");
  expect(rejected.reasons).toContain("Extra download exceeds policy budget");
});

test("a piece crossing an absent sidecar does not prove the adjoining media bytes", () => {
  const plan = planContentReuse({ candidate: candidate([
    { path: "E01.mkv", size: 6 }, { path: "info.nfo", size: 2, role: "sidecar" },
  ], [sha1("abcd"), sha1("ef!!")]), sources: [source("1", "old.mkv", 6)], policy });
  const checked = applyReusePieceEvidence(plan, [
    { index: 0, status: "verified", actualSha1: sha1("abcd") }, { index: 1, status: "unavailable" },
  ]);
  expect(checked.files[0].proof).toBe("verified-partial");
  expect(checked.summary.verifiedReuseBytes).toBe(4);
  expect(checked.summary.extraDownloadBytesUpperBound).toBe(4);
});

test("unavailable cross-file evidence is not reported as a detected mismatch", () => {
  const plan = planContentReuse({ candidate: candidate([
    { path: "E01.mkv", size: 2 }, { path: "info.nfo", size: 2, role: "sidecar" },
  ], [sha1("ab!!")]), sources: [source("1", "old.mkv", 2)], policy });
  const checked = applyReusePieceEvidence(plan, [{ index: 0, status: "unavailable" }]);
  expect(checked.files[0].proof).toBe("likely");
  expect(checked.summary.verifiedReuseBytes).toBe(0);
});

test("explicit shorter source supports writable prefix reuse without claiming a full file", () => {
  const plan = planContentReuse({ candidate: candidate([{ path: "E01.mkv", size: 8 }], [sha1("abcd"), sha1("efgh")]),
    sources: [source("1", "prefix.mkv", 4)], bindings: [{ candidatePath: "E01.mkv", sourceId: "1" }], policy });
  expect(plan.summary.likelyReuseBytes).toBe(4);
  const checked = applyReusePieceEvidence(plan, [
    { index: 0, status: "verified", actualSha1: sha1("abcd") }, { index: 1, status: "unavailable" },
  ]);
  expect(checked.files[0].proof).toBe("verified-partial");
});

test("hybrid protocol identities are retained and metadata absence is explicit", () => {
  const manifest = { ...candidate([{ path: "E01.mkv", size: 8 }]), identity: { v1: "a".repeat(40), v2: "b".repeat(64) } };
  const plan = planContentReuse({ candidate: manifest, sources: [source("1", "old.mkv", 8)], policy });
  expect(plan.candidate.identity).toEqual(manifest.identity);
  expect(applyReusePieceEvidence(plan, []).verification).toBe("unavailable");
});

test("plan identity is deterministic, immutable and tied to inode, manifest and policy revision", () => {
  const a = source("1", "old.mkv", 8);
  const b = source("2", "two.mkv", 12);
  const input = { candidate: candidate([{ path: "E01.mkv", size: 8 }]), sources: [a, b], policy };
  const first = planContentReuse(input);
  expect(planContentReuse({ ...input, sources: [b, a] }).id).toBe(first.id);
  expect(planContentReuse({ ...input, policy: { ...policy, minReuseRatio: 0.75 } }).id).not.toBe(first.id);
  expect(planContentReuse({ ...input, sources: [{ ...a, identity: { ...a.identity, inode: "999" } }, b] }).id).not.toBe(first.id);
  a.path = "/owned/changed";
  expect(first.files[0].source?.path).toBe("/owned/old.mkv");
});

test("planner rejects traversal, case/unicode/file-directory collisions and invalid piece tables", () => {
  for (const filePath of ["/escape", "../escape", "pack/../escape", "a//b", "a\\b", "c:/file", "a\0b"])
    expect(() => planContentReuse({ candidate: candidate([{ path: filePath, size: 1 }]), sources: [], policy })).toThrow("path");
  for (const paths of [["a", "A"], ["a", "a/b"], ["caf\u00e9", "cafe\u0301"]])
    expect(() => planContentReuse({ candidate: candidate(paths.map((filePath) => ({ path: filePath, size: 1 }))),
      sources: [], policy })).toThrow("collision");
  expect(() => planContentReuse({ candidate: candidate([{ path: "file", size: 8 }], [sha1("abcd")]), sources: [], policy }))
    .toThrow("piece manifest");
  expect(() => planContentReuse({ candidate: candidate([{ path: "file", size: 8 }]), sources: [],
    policy: { ...policy, version: 2 } as unknown as ReusePolicy })).toThrow("policy");
});
