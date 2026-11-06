import bencode from "bencode";

export function normalizeTrackerHost(raw: string): string {
  const host = raw.trim().toLowerCase().replace(/\.$/, "");
  if (host.length > 253 || !host.includes(".") || !host.split(".").every(part => /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(part)))
    throw new Error("Expected a tracker hostname, not an announce URL or credential");
  return host;
}

/** Only announce hostnames leave metainfo; paths, passkeys and unrelated URLs never do. */
export function metainfoTrackerHosts(bytes: Uint8Array): string[] {
  if (bytes.byteLength > 8 * 1024 * 1024) return [];
  try {
    const raw = bencode.decode(Buffer.from(bytes)) as Record<string, unknown>;
    const tiers = Array.isArray(raw["announce-list"]) ? raw["announce-list"].slice(0, 100) : [];
    const values = [raw.announce, ...tiers.flatMap(tier => Array.isArray(tier) ? tier.slice(0, 100) : [])];
    const hosts = new Set<string>();
    for (const value of values) {
      if (!(value instanceof Uint8Array)) continue;
      try {
        const url = new URL(new TextDecoder("utf-8", { fatal: true }).decode(value));
        if (["http:", "https:", "udp:"].includes(url.protocol)) hosts.add(normalizeTrackerHost(url.hostname));
      } catch { /* Malformed announces are not tracker identities. */ }
    }
    return [...hosts].sort();
  } catch { return []; }
}
