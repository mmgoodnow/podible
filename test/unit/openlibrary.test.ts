import { afterEach, expect, test } from "bun:test";
import { searchOpenLibrary } from "../../src/library/openlibrary";

const originalFetch = globalThis.fetch;
const oneDay = { key: "/works/OL123W", title: "One Day", author_name: ["David Nicholls"] };

afterEach(() => {
  globalThis.fetch = originalFetch;
});

test("retries an upstream search failure once with a smaller result count", async () => {
  const calls: URL[] = [];
  globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
    calls.push(new URL(String(input)));
    expect(init?.signal).toBeInstanceOf(AbortSignal);
    return calls.length === 1
      ? new Response("Internal Server Error", { status: 500 })
      : Response.json({ docs: [oneDay] });
  }) as typeof fetch;

  expect(await searchOpenLibrary("one day", 50)).toMatchObject([{ title: "One Day", author: "David Nicholls" }]);
  expect(calls.map((url) => url.searchParams.get("limit"))).toEqual(["50", "10"]);
  for (const url of calls) {
    expect(url.searchParams.get("q")).toBe("one day");
    expect(url.searchParams.get("lang")).toBe("en");
    expect(url.searchParams.get("fields")).toContain("editions.language");
  }
});

test.each([new TypeError("network failed"), new DOMException("timed out", "TimeoutError")])(
  "retries a network failure or timeout without increasing a small result limit",
  async (failure) => {
    const limits: string[] = [];
    globalThis.fetch = (async (input: unknown) => {
      limits.push(new URL(String(input)).searchParams.get("limit")!);
      if (limits.length === 1) throw failure;
      return Response.json({ docs: [oneDay] });
    }) as typeof fetch;
    expect(await searchOpenLibrary("one day", 3)).toHaveLength(1);
    expect(limits).toEqual(["3", "3"]);
  }
);

test("surfaces repeated upstream failures instead of returning no matches", async () => {
  let calls = 0;
  globalThis.fetch = (async (_input: unknown) => {
    calls++;
    return new Response("Unavailable", { status: 503 });
  }) as typeof fetch;
  await expect(searchOpenLibrary("one day", 50)).rejects.toThrow("Open Library search failed (HTTP 503)");
  expect(calls).toBe(2);
});

test("surfaces repeated network failures after two attempts", async () => {
  let calls = 0;
  globalThis.fetch = (async (_input: unknown): Promise<Response> => {
    calls++;
    throw new TypeError("network failed");
  }) as typeof fetch;
  await expect(searchOpenLibrary("one day", 50)).rejects.toThrow("Open Library search could not be reached");
  expect(calls).toBe(2);
});

test.each([400, 401, 403, 404, 429])("does not retry non-transient HTTP %i responses", async (status) => {
  let calls = 0;
  globalThis.fetch = (async (_input: unknown) => {
    calls++;
    return new Response("Rejected", { status });
  }) as typeof fetch;
  await expect(searchOpenLibrary("one day", 50)).rejects.toThrow(`HTTP ${status}`);
  expect(calls).toBe(1);
});

test("a successful empty search remains a genuine no-match result without retrying", async () => {
  let calls = 0;
  globalThis.fetch = (async (_input: unknown) => {
    calls++;
    return Response.json({ docs: [] });
  }) as typeof fetch;
  expect(await searchOpenLibrary("no matches", 50)).toEqual([]);
  expect(calls).toBe(1);
});

test.each(["not json", "{}", '{"docs":null}'])("invalid upstream data is an error, not no matches: %s", async (body) => {
  globalThis.fetch = (async (_input: unknown) => new Response(body)) as typeof fetch;
  await expect(searchOpenLibrary("one day", 50)).rejects.toThrow("Open Library search returned an invalid response");
});
