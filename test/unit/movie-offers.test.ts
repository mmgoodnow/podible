import { expect, test } from "bun:test";
import { MovieIndexers } from "../../src/movies/offers";
import { fixtureMovie } from "../fixtures/movie-services";
test("movie searches coalesce, retain unknown release titles, and never treat upstream errors as empty results", async () => {
  let calls = 0;
  const indexers = new MovieIndexers(
    [
      {
        name: "test",
        baseUrl: "https://indexer.invalid/1",
        apiKey: "private-key",
      },
    ],
    async (url) => {
      calls++;
      expect(new URL(url).searchParams.get("cat")).toBe("2000");
      return new Response(
        `<rss><channel><item><title>Ambiguous Untagged Release</title><guid>1</guid><link>https://indexer.invalid/download?apikey=private-key</link></item></channel></rss>`,
      );
    },
  );
  const [a, b] = await Promise.all([
    indexers.search(fixtureMovie),
    indexers.search(fixtureMovie),
  ]);
  expect(a).toEqual(b);
  expect(a.offers).toHaveLength(1);
  expect(calls).toBe(1);
  const invalid = new MovieIndexers(
    [{ name: "bad", baseUrl: "https://indexer.invalid" }],
    async () => new Response("<html>unavailable</html>"),
  );
  await expect(invalid.search(fixtureMovie)).rejects.toThrow(
    "temporarily unavailable",
  );
});
test("indexer 429 backoff survives the next search and does not expose provider credentials", async () => {
  let calls = 0;
  const indexers = new MovieIndexers(
    [{ name: "limited", baseUrl: "https://indexer.invalid/private-key" }],
    async () => {
      calls++;
      return new Response("private-key", {
        status: 429,
        headers: { "retry-after": "1800" },
      });
    },
  );
  await expect(indexers.search(fixtureMovie)).rejects.toThrow("HTTP 429");
  await expect(
    indexers.search(fixtureMovie, "different query"),
  ).rejects.toThrow("HTTP 429");
  expect(calls).toBe(1);
});
