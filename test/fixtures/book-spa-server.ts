import { createPodibleFetchHandler } from "../../src/http";
import { createBookSpaData } from "./book-spa-data";
import { startMockTorznab } from "../mocks/torznab";
import { FakeBookTorrentClient, bookTorrentMetainfo } from "../mocks/book-torrent";

const data = await createBookSpaData();
const torrent = bookTorrentMetainfo("Piranesi.mp3");
const torznab = startMockTorznab({
  results: [
    {
      title: "Piranesi - Fixture audiobook MP3",
      torrentId: "piranesi",
      size: 10,
    },
  ],
  torrents: { piranesi: torrent },
});
const settings = data.repo.getSettings();
settings.torznab = [{ name: "fixture", baseUrl: torznab.baseUrl }];
data.repo.updateSettings(settings);
const bookTorrentClient = new FakeBookTorrentClient(settings.libraryRoot);
const originalFetch = globalThis.fetch;
const picture = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=",
  "base64",
);
globalThis.fetch = Object.assign(
  async (
    input: Parameters<typeof fetch>[0],
    init?: Parameters<typeof fetch>[1],
  ) => {
    const url = new URL(
      input instanceof Request ? input.url : input.toString(),
    );
    if (url.hostname === "covers.openlibrary.org")
      return new Response(picture, {
        headers: { "content-type": "image/png" },
      });
    if (url.hostname === "openlibrary.org") {
      if (
        url.pathname === "/search.json" &&
        url.searchParams.get("q") === "provider-error"
      )
        return Response.json({ error: "fixture outage" }, { status: 400 });
      if (url.pathname === "/search.json")
        return Response.json({
          docs: [
            {
              key: "/works/OL100W",
              title: "Piranesi",
              author_name: ["Susanna Clarke"],
              first_publish_year: 2020,
              series_key: ["OL100L"],
              series_name: ["The House"],
              series_position: ["1"],
            },
            {
              key: "/works/OL101W",
              title: "Jonathan Strange & Mr Norrell",
              author_name: ["Susanna Clarke"],
              first_publish_year: 2004,
            },
          ],
        });
      if (url.pathname.endsWith("/editions.json"))
        return Response.json({
          entries: [
            {
              key: "/books/OL100M",
              title: "Piranesi",
              covers: [12345],
              publishers: ["Fixture Press"],
              publish_date: "2020",
            },
          ],
        });
      if (url.pathname.startsWith("/series/"))
        return Response.json({ name: "The House" });
      return Response.json({
        description: "A world of infinite halls, statues, and tides.",
        covers: [12345, 12346],
        series: [{ series: { key: "/series/OL100L" }, position: "1" }],
      });
    }
    return originalFetch(input, init);
  },
  { preconnect: originalFetch.preconnect },
);
const handler = createPodibleFetchHandler(data.repo, Date.now(), null, { bookTorrentClient });
const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: handler });
console.log(
  `BOOK_FIXTURE=${JSON.stringify({ url: server.url.href, bookId: data.book.id, failedBookId: data.failedBook.id, manifestationIds: data.manifestations.map((item) => item.id), importPath: data.audioPath })}`,
);
process.on("SIGTERM", async () => {
  server.stop(true);
  torznab.stop();
  await data.close();
  process.exit(0);
});
