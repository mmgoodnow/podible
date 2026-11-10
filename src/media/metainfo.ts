import bencode from "bencode";
import { createHash } from "node:crypto";
import { z } from "zod";
import { infoHashFromTorrentBytes } from "../library/torrent";

// Model-facing tool errors must not expose exceptions from credential-bearing requests.
export const METAINFO_INSPECTION_FAILED = "Could not inspect this release's torrent file.";

const bytesSchema = z.instanceof(Uint8Array);
const component = (raw: Uint8Array) => {
  const value = new TextDecoder("utf-8", { fatal: true }).decode(raw);
  if (!value || value === "." || value === ".." || /[\x00-\x1f/\\]/.test(value))
    throw new Error("Unsafe metainfo path");
  return value;
};
const fileSchema = z
  .object({
    length: z.number().int().nonnegative().safe(),
    path: z.array(bytesSchema).min(1),
  })
  .passthrough();
const infoSchema = z
  .object({
    name: bytesSchema,
    length: z.number().int().nonnegative().safe().optional(),
    files: z.array(fileSchema).min(1).max(100_000).optional(),
    "piece length": z.number().int().positive().safe(),
    pieces: bytesSchema,
  })
  .passthrough();

export function inspectMetainfo(bytes: Uint8Array) {
  if (bytes.length > 8 * 1024 * 1024) throw new Error("Metainfo exceeds 8 MiB");
  const raw = z
    .object({ info: z.record(z.string(), z.unknown()) })
    .parse(bencode.decode(Buffer.from(bytes)));
  const info = infoSchema.parse(raw.info);
  if (
    info["symlink path"] !== undefined ||
    (info.attr instanceof Uint8Array &&
      new TextDecoder().decode(info.attr).includes("l"))
  )
    throw new Error("Metainfo symlinks are unsupported");
  if (
    info["meta version"] !== undefined ||
    (info.files === undefined) === (info.length === undefined)
  )
    throw new Error("Expected a v1 single-file or multi-file torrent");
  const hash = infoHashFromTorrentBytes(bytes);
  if (
    createHash("sha1").update(bencode.encode(raw.info)).digest("hex") !== hash
  )
    throw new Error("Noncanonical or ambiguous metainfo dictionary");
  const name = component(info.name);
  if (info["name.utf-8"] instanceof Uint8Array) component(info["name.utf-8"]);
  const files = info.files?.map((file) => {
    if (
      file["symlink path"] !== undefined ||
      (file.attr instanceof Uint8Array &&
        new TextDecoder().decode(file.attr).includes("l"))
    )
      throw new Error("Metainfo symlinks are unsupported");
    if (Array.isArray(file["path.utf-8"]))
      for (const part of file["path.utf-8"]) component(bytesSchema.parse(part));
    return {
      path: [name, ...file.path.map(component)].join("/"),
      size: file.length,
    };
  }) ?? [{ path: name, size: info.length! }];
  const paths = new Set<string>();
  for (const file of files) {
    const normalized = file.path.normalize("NFC").toLowerCase();
    if (paths.has(normalized)) throw new Error("Metainfo path collision");
    paths.add(normalized);
  }
  for (const file of paths) {
    const parts = file.split("/");
    for (let index = 1; index < parts.length; index++)
      if (paths.has(parts.slice(0, index).join("/")))
        throw new Error("Metainfo file/directory collision");
  }
  const size = files.reduce((sum, file) => sum + file.size, 0);
  if (
    !Number.isSafeInteger(size) ||
    info.pieces.length !== Math.ceil(size / info["piece length"]) * 20
  )
    throw new Error("Invalid metainfo piece count");
  return { infoHash: hash, name, files, size };
}
