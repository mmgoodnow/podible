import { describe, expect, test } from "bun:test";

import { buildMethodCall, parseResponseValue } from "../../src/kindling/rtorrent";

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
    expect(str).toBe("abc");
    expect(num).toBe(1);
  });

  test("throws on fault payload", () => {
    expect(() =>
      parseResponseValue(
        '<?xml version="1.0"?><methodResponse><fault><value><string>bad</string></value></fault></methodResponse>'
      )
    ).toThrow();
  });
});
