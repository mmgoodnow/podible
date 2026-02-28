import { Buffer } from "node:buffer";

import { XMLParser } from "fast-xml-parser";

import type { AppSettings } from "./types";

type RtorrentDownloadState = {
  name: string | null;
  hash: string | null;
  complete: boolean;
  basePath: string | null;
  bytesDone: number | null;
  sizeBytes: number | null;
  leftBytes: number | null;
  downRate: number | null;
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

function parseResponseValue(xml: string): unknown {
  const doc = parser.parse(xml) as Record<string, unknown>;
  const fault = parseFault(doc);
  if (fault) throw new Error(`rTorrent fault: ${fault}`);
  const params = ((doc.methodResponse as Record<string, unknown> | undefined)?.params ??
    {}) as Record<string, unknown>;
  const param = params.param as Record<string, unknown> | undefined;
  const value = (param?.value ?? null) as unknown;
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    if ("string" in record) return record.string;
    if ("i4" in record) return record.i4;
    if ("i8" in record) return record.i8;
    if ("int" in record) return record.int;
    if ("long" in record) return record.long;
    if ("boolean" in record) return record.boolean;
    if ("double" in record) return record.double;
    return record["#text"] ?? record;
  }
  return value;
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
    const [name, returnedHash, complete, basePath, bytesDone, sizeBytes, leftBytes, downRate] = await Promise.all([
      this.call("d.name", [xmlParamString(hash)]),
      this.call("d.hash", [xmlParamString(hash)]),
      this.call("d.complete", [xmlParamString(hash)]),
      this.call("d.base_path", [xmlParamString(hash)]),
      this.call("d.bytes_done", [xmlParamString(hash)]),
      this.call("d.size_bytes", [xmlParamString(hash)]),
      this.call("d.left_bytes", [xmlParamString(hash)]),
      this.call("d.down.rate", [xmlParamString(hash)]),
    ]);

    return {
      name: maybeText(name),
      hash: maybeText(returnedHash),
      complete: toBool(complete),
      basePath: maybeText(basePath),
      bytesDone: toNumber(bytesDone),
      sizeBytes: toNumber(sizeBytes),
      leftBytes: toNumber(leftBytes),
      downRate: toNumber(downRate),
    };
  }
}

export { buildMethodCall, parseResponseValue };
export type { RtorrentDownloadState };
