import { expect, test } from "bun:test";
import { PlexMovieLibrary } from "../../src/media/plex-library";

test("Plex publication verifies movie GUID and exact part paths, not a successful scan or matching filename", async () => {
  let guid = "tmdb://999";
  let file = "/data/view/movie.mkv";
  let scans = 0;
  const server = Bun.serve({
    port: 0,
    fetch(request) {
      expect(request.headers.get("X-Plex-Token")).toBe("private-fixture");
      expect(new URL(request.url).searchParams.has("X-Plex-Token")).toBe(false);
      if (new URL(request.url).pathname.endsWith("/refresh")) {
        scans++;
        return new Response("");
      }
      return Response.json({
        MediaContainer: {
          Metadata: [{ Guid: [{ id: guid }], Media: [{ Part: [{ file }] }] }],
        },
      });
    },
  });
  try {
    const library = new PlexMovieLibrary({
      url: server.url.toString(),
      token: "private-fixture",
      sectionId: "1",
    });
    const movie = {
      id: 1,
      tmdbId: 42,
      title: "Fixture",
      originalTitle: "Fixture",
      year: 2024,
      overview: "",
      originalLanguage: "en",
      imdbId: null,
      posterUrl: null,
    };
    await library.scan();
    expect(scans).toBe(1);
    expect(await library.verify(movie, [file])).toBe(false);
    guid = "tmdb://42";
    expect(await library.verify(movie, [file])).toBe(true);
    file = "/other/movie.mkv";
    expect(await library.verify(movie, ["/data/view/movie.mkv"])).toBe(false);
    expect(await library.verify(movie, [])).toBe(false);
  } finally {
    server.stop(true);
  }
});
