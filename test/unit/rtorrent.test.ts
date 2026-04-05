import { describe, expect, test } from "bun:test";

import { buildMethodCall, parseResponseValue, RtorrentClient } from "../../src/rtorrent";

describe("rtorrent xmlrpc helpers", () => {
  test("builds method call payload", () => {
    const xml = buildMethodCall("d.hash", ["<param><value><string>ABC</string></value></param>"]);
    expect(xml).toContain("<methodName>d.hash</methodName>");
    expect(xml).toContain("<string>ABC</string>");
  });

  test("parses xmlrpc scalar responses", () => {
    const str = parseResponseValue(
      '<?xml version="1.0"?><methodResponse><params><param><value><string>abc</string></value></param></params></methodResponse>'
    );
    const num = parseResponseValue(
      '<?xml version="1.0"?><methodResponse><params><param><value><int>1</int></value></param></params></methodResponse>'
    );
    const i8 = parseResponseValue(
      '<?xml version="1.0"?><methodResponse><params><param><value><i8>2</i8></value></param></params></methodResponse>'
    );
    expect(str).toBe("abc");
    expect(num).toBe(1);
    expect(i8).toBe(2);
  });

  test("throws on fault payload", () => {
    expect(() =>
      parseResponseValue(
        '<?xml version="1.0"?><methodResponse><fault><value><string>bad</string></value></fault></methodResponse>'
      )
    ).toThrow();
  });

  test("uses direct d.* methods for state snapshot", async () => {
    const originalFetch = globalThis.fetch;
    const calls: string[] = [];
    globalThis.fetch = (async (_input: unknown, init?: RequestInit) => {
      const body = String(init?.body ?? "");
      const method = /<methodName>([^<]+)<\/methodName>/.exec(body)?.[1] ?? "";
      calls.push(method);

      const responseXml = (() => {
        switch (method) {
          case "d.name":
            return '<?xml version="1.0"?><methodResponse><params><param><value><string>Name</string></value></param></params></methodResponse>';
          case "d.hash":
            return '<?xml version="1.0"?><methodResponse><params><param><value><string>ABCDEF</string></value></param></params></methodResponse>';
          case "d.complete":
            return '<?xml version="1.0"?><methodResponse><params><param><value><i8>1</i8></value></param></params></methodResponse>';
          case "d.is_active":
            return '<?xml version="1.0"?><methodResponse><params><param><value><i8>0</i8></value></param></params></methodResponse>';
          case "d.base_path":
            return '<?xml version="1.0"?><methodResponse><params><param><value><string>/downloads/book</string></value></param></params></methodResponse>';
          case "d.bytes_done":
            return '<?xml version="1.0"?><methodResponse><params><param><value><i8>50</i8></value></param></params></methodResponse>';
          case "d.size_bytes":
            return '<?xml version="1.0"?><methodResponse><params><param><value><i8>100</i8></value></param></params></methodResponse>';
          case "d.left_bytes":
            return '<?xml version="1.0"?><methodResponse><params><param><value><i8>50</i8></value></param></params></methodResponse>';
          case "d.down.rate":
            return '<?xml version="1.0"?><methodResponse><params><param><value><i8>10</i8></value></param></params></methodResponse>';
          case "d.message":
            return '<?xml version="1.0"?><methodResponse><params><param><value><string>tracker needs credits</string></value></param></params></methodResponse>';
          default:
            return '<?xml version="1.0"?><methodResponse><params><param><value><string></string></value></param></params></methodResponse>';
        }
      })();
      return new Response(responseXml, { status: 200, headers: { "Content-Type": "text/xml" } });
    }) as unknown as typeof fetch;

    try {
      const client = new RtorrentClient({
        transport: "http-xmlrpc",
        url: "http://mock.local/RPC2",
        username: "",
        password: "",
      });
      const state = await client.getDownloadState("0123456789abcdef0123456789abcdef01234567");
      expect(state.basePath).toBe("/downloads/book");
      expect(state.isActive).toBe(false);
      expect(state.bytesDone).toBe(50);
      expect(state.sizeBytes).toBe(100);
      expect(state.leftBytes).toBe(50);
      expect(state.downRate).toBe(10);
      expect(state.message).toBe("tracker needs credits");
      expect(calls.includes("d.is_active")).toBe(true);
      expect(calls.includes("d.base_path")).toBe(true);
      expect(calls.includes("d.bytes_done")).toBe(true);
      expect(calls.includes("d.size_bytes")).toBe(true);
      expect(calls.includes("d.left_bytes")).toBe(true);
      expect(calls.includes("d.down.rate")).toBe(true);
      expect(calls.includes("d.message")).toBe(true);
      expect(calls.includes("d.get_size_bytes")).toBe(false);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});
