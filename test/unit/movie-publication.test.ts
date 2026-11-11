import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import {
  mkdtemp,
  realpath,
  mkdir,
  writeFile,
  readFile,
  readlink,
  rm,
} from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { initializeManagedContent } from "../../src/media/library-view";
import {
  movieViewEntries,
  publishMovieVersion,
  reconcileMovieView,
} from "../../src/movies/publication";
import { defaultMovieQualityPolicy as policy } from "../../src/movies/quality";
import { factsFromMovieProbe } from "../../src/movies/probe";

test("movie upgrade keeps the old playable view until verified and recovers a crash after promotion", async () => {
  const root = await realpath(
    await mkdtemp(path.join(os.tmpdir(), "movie-publication-")),
  );
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const user = repo.upsertUser({
    provider: "local",
    providerUserId: "publication",
    username: "tester",
    isAdmin: false,
  });
  const movie = {
    tmdbId: 42,
    title: "A / Movie",
    originalTitle: "A Movie",
    year: 2024,
    overview: "",
    originalLanguage: "en",
    imdbId: null,
    posterUrl: null,
  };
  const requested = repo.movies.request(user.id, movie, policy);
  const roots = {
    contentRoot: `${root}/objects`,
    viewRoot: `${root}/view`,
    serverViewRoot: "/data/view",
  };
  await initializeManagedContent(roots.contentRoot);
  const version = async (n: number, width: number) => {
    const acquisition = repo.movies.startAcquisition(
      requested.targetId,
      n === 1 ? "initial" : "upgrade",
      () => repo.createJob({ type: "reconcile" }).id,
    ).acquisition;
    await mkdir(`${roots.contentRoot}/object-${n}`);
    await writeFile(`${roots.contentRoot}/object-${n}/movie.mkv`, `movie ${n}`);
    const id = repo.movies.recordVersion(
      acquisition.id,
      {
        identityVerified: true,
        cut: "theatrical",
        source: "web",
        width,
        height: width === 1280 ? 720 : 1080,
        videoCodec: "h264",
        dynamicRange: "sdr",
        audioLanguages: ["en"],
        proper: false,
      },
      [
        {
          id: `file-${n}`,
          relativePath: `object-${n}/movie.mkv`,
          sizeBytes: 7,
          probe: {},
        },
      ],
    );
    repo.movies.finishAcquisition(acquisition.id);
    return id;
  };
  const library = { scan: async () => {}, verify: async () => true };
  try {
    const first = await version(1, 1280);
    await publishMovieVersion({
      repo: repo.movies,
      roots,
      library,
      targetId: requested.targetId,
      versionId: first,
    });
    const oldPath = movieViewEntries(repo.movies, roots)[0]!.relativePath;
    const second = await version(2, 1920);
    await expect(
      publishMovieVersion({
        repo: repo.movies,
        roots,
        library: { ...library, verify: async () => false },
        targetId: requested.targetId,
        versionId: second,
      }),
    ).rejects.toThrow("previous version remains active");
    expect(repo.movies.target(requested.targetId)?.active_version_id).toBe(
      first,
    );
    expect(await readFile(`${roots.viewRoot}/${oldPath}`, "utf8")).toBe(
      "movie 1",
    );
    expect(movieViewEntries(repo.movies, roots)).toHaveLength(2);
    await expect(
      publishMovieVersion({
        repo: repo.movies,
        roots,
        library,
        targetId: requested.targetId,
        versionId: second,
        afterPromotion: () => {
          throw new Error("process crash");
        },
      }),
    ).rejects.toThrow("process crash");
    expect(repo.movies.target(requested.targetId)?.active_version_id).toBe(
      second,
    );
    expect(repo.movies.publications()).toHaveLength(1);
    await publishMovieVersion({
      repo: repo.movies,
      roots,
      library,
      targetId: requested.targetId,
      versionId: second,
    });
    expect(repo.movies.publications()).toHaveLength(0);
    expect(movieViewEntries(repo.movies, roots)).toHaveLength(1);
    await expect(readlink(`${roots.viewRoot}/${oldPath}`)).rejects.toThrow();
    expect(
      await readFile(`${roots.contentRoot}/object-1/movie.mkv`, "utf8"),
    ).toBe("movie 1");
    expect(
      repo.movies
        .events(requested.targetId)
        .filter((event) => event.stage === "available"),
    ).toHaveLength(2);
    const equal = await version(3, 1920);
    expect(() =>
      repo.movies.stagePublication(requested.targetId, equal),
    ).toThrow("not an upgrade");
    repo.movies.upsertMovie({ ...movie, title: "Renamed" });
    await reconcileMovieView(repo.movies, roots);
    expect(
      await readFile(
        `${roots.viewRoot}/${movieViewEntries(repo.movies, roots)[0]!.relativePath}`,
        "utf8",
      ),
    ).toBe("movie 2");
    const remainingPath = path.join(roots.viewRoot, movieViewEntries(repo.movies, roots)[0]!.relativePath);
    db.query("UPDATE movie_targets SET active_version_id=NULL WHERE id=?").run(requested.targetId);
    let checks = 0;
    await expect(reconcileMovieView(repo.movies, roots, () => {
      if (++checks > 1) throw new Error("cancelled cleanup");
    })).rejects.toThrow("cancelled cleanup");
    expect(await readFile(remainingPath, "utf8")).toBe("movie 2");
    await reconcileMovieView(repo.movies, roots);
    await expect(readlink(remainingPath)).rejects.toThrow();
  } finally {
    db.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("movie probe keeps unknown language/HDR unknown and ignores cover-art video streams", () => {
  const claims = {
    identityVerified: true,
    source: "web" as const,
    cut: "theatrical",
    proper: false,
  };
  const facts = factsFromMovieProbe(
    {
      streams: [
        {
          codec_type: "video",
          codec_name: "mjpeg",
          width: 600,
          height: 900,
          disposition: { attached_pic: 1 },
        },
        { codec_type: "video", codec_name: "hevc", width: 1920, height: 800 },
        { codec_type: "audio", codec_name: "aac" },
      ],
    },
    claims,
  );
  expect(facts.width).toBe(1920);
  expect(facts.audioLanguages).toEqual(["und"]);
  expect(facts.dynamicRange).toBeNull();
  expect(
    factsFromMovieProbe(
      { streams: [{ codec_type: "video", color_transfer: "smpte2084" }] },
      claims,
    ).dynamicRange,
  ).toBe("hdr");
});
