import { Buffer } from "node:buffer";
import path from "node:path";

import { XMLParser } from "fast-xml-parser";

import type { AppSettings } from "./app-types";

type RtorrentDownloadState = {
  name: string | null;
  hash: string | null;
  complete: boolean;
  isActive: boolean;
  basePath: string | null;
  directory: string | null;
  isMultiFile: boolean;
  bytesDone: number | null;
  sizeBytes: number | null;
  leftBytes: number | null;
  downRate: number | null;
  message: string | null;
};

type RtorrentImportSource = {
  basePath: string | null;
  selectedPaths: string[];
};

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "",
  removeNSPrefix: true,
  parseTagValue: true,
  textNodeName: "#text",
});

function xmlEscape(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function xmlParamString(value: string): string {
  return `<param><value><string>${xmlEscape(value)}</string></value></param>`;
}

function xmlParamBase64(value: Uint8Array): string {
  return `<param><value><base64>${Buffer.from(value).toString("base64")}</base64></value></param>`;
}

function buildMethodCall(methodName: string, params: string[]): string {
  return `<?xml version="1.0"?><methodCall><methodName>${xmlEscape(methodName)}</methodName><params>${params.join("")}</params></methodCall>`;
}

function maybeText(value: unknown): string | null {
  if (typeof value === "string") return value;
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  if (value && typeof value === "object") {
    const t = (value as Record<string, unknown>)["#text"];
    if (typeof t === "string") return t;
  }
  return null;
}

function parseFault(doc: Record<string, unknown>): string | null {
  const response = doc.methodResponse as Record<string, unknown> | undefined;
  const fault = response?.fault as Record<string, unknown> | undefined;
  if (!fault) return null;
  return JSON.stringify(fault);
}

function parseXmlRpcValue(value: unknown): unknown {
  if (value === null || value === undefined) return value;
  if (typeof value !== "object") return value;
  const record = value as Record<string, unknown>;
  if ("string" in record) return record.string;
  if ("i4" in record) return record.i4;
  if ("i8" in record) return record.i8;
  if ("int" in record) return record.int;
  if ("long" in record) return record.long;
  if ("boolean" in record) return record.boolean;
  if ("double" in record) return record.double;
  if ("base64" in record) return record.base64;
  if ("#text" in record) return record["#text"];
  if ("array" in record) {
    const arrayRecord = record.array as Record<string, unknown> | undefined;
    const dataRecord = arrayRecord?.data as Record<string, unknown> | undefined;
    const rawItems = dataRecord?.value;
    const items = Array.isArray(rawItems) ? rawItems : rawItems === undefined ? [] : [rawItems];
    return items.map((item) => parseXmlRpcValue(item));
  }
  if ("struct" in record) {
    const structRecord = record.struct as Record<string, unknown> | undefined;
    const rawMembers = structRecord?.member;
    const members = Array.isArray(rawMembers) ? rawMembers : rawMembers === undefined ? [] : [rawMembers];
    const out: Record<string, unknown> = {};
    for (const rawMember of members) {
      const member = rawMember as Record<string, unknown>;
      const name = member.name;
      if (typeof name !== "string") continue;
      out[name] = parseXmlRpcValue(member.value);
    }
    return out;
  }
  return record;
}

function parseResponseValue(xml: string): unknown {
  const doc = parser.parse(xml) as Record<string, unknown>;
  const fault = parseFault(doc);
  if (fault) throw new Error(`rTorrent fault: ${fault}`);
  const params = ((doc.methodResponse as Record<string, unknown> | undefined)?.params ??
    {}) as Record<string, unknown>;
  const param = params.param as Record<string, unknown> | undefined;
  const value = (param?.value ?? null) as unknown;
  return parseXmlRpcValue(value);
}

function toNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return Math.trunc(value);
  if (typeof value === "string") {
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function toBool(value: unknown): boolean {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  if (typeof value === "string") return value === "1" || value.toLowerCase() === "true";
  return false;
}

function deriveDownloadPath(args: {
  name: string | null;
  basePath: string | null;
  directory: string | null;
  isMultiFile: boolean;
}): string | null {
  if (args.isMultiFile) {
    return args.directory ?? args.basePath;
  }
  if (args.basePath) {
    return args.basePath;
  }
  if (args.directory && args.name) {
    return path.join(args.directory, args.name);
  }
  return args.directory;
}

function asStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => {
      if (Array.isArray(item)) {
        return maybeText(item[0]) ?? "";
      }
      return maybeText(item) ?? "";
    })
    .filter((item) => item.length > 0);
}

export class RtorrentClient {
  constructor(private readonly settings: AppSettings["rtorrent"]) {}

  private async call(methodName: string, params: string[]): Promise<unknown> {
    const body = buildMethodCall(methodName, params);
    const headers: Record<string, string> = {
      "Content-Type": "text/xml",
    };
    if (this.settings.username || this.settings.password) {
      headers.Authorization = `Basic ${Buffer.from(`${this.settings.username ?? ""}:${this.settings.password ?? ""}`).toString("base64")}`;
    }

    const response = await fetch(this.settings.url, {
      method: "POST",
      headers,
      body,
    });

    if (!response.ok) {
      throw new Error(`rTorrent returned ${response.status}`);
    }

    const xml = await response.text();
    return parseResponseValue(xml);
  }

  async loadRawStart(torrentBytes: Uint8Array, commands: string[] = []): Promise<void> {
    await this.call("load.raw_start", [
      xmlParamString(""),
      xmlParamBase64(torrentBytes),
      ...commands.map((command) => xmlParamString(command)),
    ]);
  }

  async getDownloadState(infoHash: string): Promise<RtorrentDownloadState> {
    const hash = infoHash.toUpperCase();
    const [name, returnedHash, complete, isActive, basePath, directory, isMultiFile, bytesDone, sizeBytes, leftBytes, downRate, message] =
      await Promise.all([
        this.call("d.name", [xmlParamString(hash)]),
        this.call("d.hash", [xmlParamString(hash)]),
        this.call("d.complete", [xmlParamString(hash)]),
        this.call("d.is_active", [xmlParamString(hash)]),
        this.call("d.base_path", [xmlParamString(hash)]),
        this.call("d.directory", [xmlParamString(hash)]),
        this.call("d.is_multi_file", [xmlParamString(hash)]),
        this.call("d.bytes_done", [xmlParamString(hash)]),
        this.call("d.size_bytes", [xmlParamString(hash)]),
        this.call("d.left_bytes", [xmlParamString(hash)]),
        this.call("d.down.rate", [xmlParamString(hash)]),
        this.call("d.message", [xmlParamString(hash)]),
      ]);

    const parsedName = maybeText(name);
    const parsedBasePath = maybeText(basePath);
    const parsedDirectory = maybeText(directory);
    const parsedIsMultiFile = toBool(isMultiFile);

    return {
      name: parsedName,
      hash: maybeText(returnedHash),
      complete: toBool(complete),
      isActive: toBool(isActive),
      basePath: deriveDownloadPath({
        name: parsedName,
        basePath: parsedBasePath,
        directory: parsedDirectory,
        isMultiFile: parsedIsMultiFile,
      }),
      directory: parsedDirectory,
      isMultiFile: parsedIsMultiFile,
      bytesDone: toNumber(bytesDone),
      sizeBytes: toNumber(sizeBytes),
      leftBytes: toNumber(leftBytes),
      downRate: toNumber(downRate),
      message: maybeText(message),
    };
  }

  async getImportSource(infoHash: string): Promise<RtorrentImportSource> {
    const hash = infoHash.toUpperCase();
    const [name, basePath, directory, isMultiFile, fileRows] = await Promise.all([
      this.call("d.name", [xmlParamString(hash)]),
      this.call("d.base_path", [xmlParamString(hash)]),
      this.call("d.directory", [xmlParamString(hash)]),
      this.call("d.is_multi_file", [xmlParamString(hash)]),
      this.call("f.multicall", [xmlParamString(hash), xmlParamString(""), xmlParamString("f.path=")]),
    ]);

    const parsedName = maybeText(name);
    const parsedBasePath = maybeText(basePath);
    const parsedDirectory = maybeText(directory);
    const parsedIsMultiFile = toBool(isMultiFile);
    const selectedPaths = asStringArray(fileRows)
      .map((relativePath) =>
        parsedDirectory ? path.join(parsedDirectory, relativePath) : relativePath
      )
      .filter(Boolean);

    return {
      basePath: deriveDownloadPath({
        name: parsedName,
        basePath: parsedBasePath,
        directory: parsedDirectory,
        isMultiFile: parsedIsMultiFile,
      }),
      selectedPaths,
    };
  }
}

export { buildMethodCall, deriveDownloadPath, parseResponseValue };
export type { RtorrentDownloadState, RtorrentImportSource };
