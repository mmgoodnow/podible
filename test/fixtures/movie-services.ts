import path from "node:path";
import { promises as fs } from "node:fs";
import { createHash } from "node:crypto";
import bencode from "bencode";
import { inspectMetainfo } from "../../src/media/metainfo";
import type { MovieServices } from "../../src/movies/operations";
import type { MovieOffer } from "../../src/movies/offers";
import type { MovieFacts } from "../../src/movies/quality";
import {
  assessMovieUpgrade,
  evaluateMovieQuality,
} from "../../src/movies/quality";
import type { Movie } from "../../src/movies/types";
import type {
  DownloadLocation,
  TorrentRef,
  TorrentSnapshot,
} from "../../src/media/torrent-client";

export const fixtureMovie: Movie = {
  id: 1,
  tmdbId: 101,
  title: "A Fixture Film",
  originalTitle: "A Fixture Film",
  year: 2024,
  overview: "A synthetic movie for the isolated acceptance suite.",
  originalLanguage: "en",
  posterUrl: null,
  imdbId: null,
};
export function fixtureFacts(width = 1280): MovieFacts {
  return {
    identityVerified: true,
    cut: "theatrical",
    source: "web",
    width,
    height: width === 1280 ? 720 : 1080,
    videoCodec: "h264",
    dynamicRange: "sdr",
    audioLanguages: ["eng"],
    proper: false,
  };
}
export function createFixtureMovieServices(root: string) {
  const stats = {
    searches: 0,
    snatches: 0,
    probes: 0,
    scans: 0,
    removals: 0,
    quiesces: 0,
    rechecks: 0,
  };
  const payloads = new Map<
    string,
    { bytes: Buffer; metainfo: Uint8Array; width: number; stalled: boolean }
  >();
  const downloads = new Map<
    string,
    {
      ref: TorrentRef;
      location: DownloadLocation;
      resumed: boolean;
      polls: number;
    }
  >();
  let offers: MovieOffer[] = [];
  let verify = true;
  function addOffer(key: string, width: number, stalled = false) {
    const bytes = Buffer.from(`synthetic movie bytes ${key}`);
    const metainfo = new Uint8Array(
      bencode.encode({
        info: {
          name: `${key}.mkv`,
          length: bytes.length,
          "piece length": 16384,
          pieces: createHash("sha1").update(bytes).digest(),
        },
      }),
    );
    const hash = inspectMetainfo(metainfo).infoHash;
    payloads.set(hash, { bytes, metainfo, width, stalled });
    const offer: MovieOffer = {
      key,
      title: `A Fixture Film ${key}`,
      provider: "Fixture",
      sizeBytes: bytes.length,
      seeders: stalled ? 0 : 1,
      url: `https://fixture.invalid/${key}`,
      infoHash: hash,
      attributes: {},
    };
    offers.push(offer);
    return offer;
  }
  const services: MovieServices = {
    catalog: {
      search: async () => [fixtureMovie],
      get: async (id) => {
        if (id !== fixtureMovie.tmdbId)
          throw new Error("Unknown fixture movie");
        return fixtureMovie;
      },
    },
    search: async () => {
      stats.searches++;
      return { offers: [...offers], warnings: [] };
    },
    decider: {
      select: async ({ policy, offers: choices, current }) => {
        const suitable = choices
          .map((offer) => ({
            offer,
            facts: fixtureFacts(payloads.get(offer.infoHash!)!.width),
          }))
          .filter(
            ({ facts }) =>
              evaluateMovieQuality(policy, facts).eligibility === "eligible" &&
              (!current || assessMovieUpgrade(policy, current, facts).upgrade),
          );
        suitable.sort(
          (a, b) =>
            evaluateMovieQuality(policy, b.facts).tier! -
            evaluateMovieQuality(policy, a.facts).tier!,
        );
        return {
          offerKey: suitable[0]?.offer.key ?? null,
          facts: suitable[0]?.facts ?? fixtureFacts(),
          reason: suitable.length
            ? "Fixture movie/cut is correct and satisfies quality policy"
            : "No eligible improvement",
        };
      },
      inspect: async ({ files }) => ({
        fileIndex: files[0]?.index ?? null,
        facts: {
          identityVerified: true,
          source: "web",
          cut: "theatrical",
          proper: false,
        },
        reason: "Verified synthetic fixture identity",
      }),
    },
    client: {
      id: "fixture-client",
      capabilities: {
        pausedAdd: true,
        recheck: true,
        derivedLayouts: false,
        metainfoVersions: [1],
      },
      ensureAdded: async ({ metainfo, location }) => {
        const metadata = inspectMetainfo(metainfo);
        const prior = downloads.get(metadata.infoHash);
        if (prior) {
          if (JSON.stringify(prior.location) !== JSON.stringify(location))
            throw new Error("Different fixture layout");
          return prior.ref;
        }
        stats.snatches++;
        const ref = { clientId: "fixture-client", nativeId: metadata.infoHash };
        downloads.set(ref.nativeId, {
          ref,
          location,
          resumed: false,
          polls: 0,
        });
        return ref;
      },
      inspect: async (ref): Promise<TorrentSnapshot> => {
        const item = downloads.get(ref.nativeId)!;
        const payload = payloads.get(ref.nativeId)!;
        item.polls++;
        const complete = item.resumed && !payload.stalled;
        if (complete) {
          const directory = path.join(
            root,
            "downloads",
            item.location.relativePath,
          );
          await fs.mkdir(directory, { recursive: true });
          await fs.writeFile(
            path.join(directory, inspectMetainfo(payload.metainfo).name),
            payload.bytes,
          );
        }
        return {
          ref,
          location: item.location,
          state: item.resumed
            ? complete
              ? "seeding"
              : "downloading"
            : "inactive",
          complete,
          bytesDone: complete ? payload.bytes.length : 0,
          sizeBytes: payload.bytes.length,
          downloadBytesPerSecond: 0,
          message: null,
          observedAt: new Date(
            1_800_000_000_000 + item.polls * 1000,
          ).toISOString(),
        };
      },
      files: async (ref) => [
        {
          index: 0,
          path: path.join(
            root,
            "downloads",
            downloads.get(ref.nativeId)!.location.relativePath,
            inspectMetainfo(payloads.get(ref.nativeId)!.metainfo).name,
          ),
        },
      ],
      quiesce: async (ref) => {
        stats.quiesces++;
        downloads.get(ref.nativeId)!.resumed = false;
      },
      recheck: async (ref) => { stats.rechecks++; downloads.get(ref.nativeId)!.resumed = false; },
      resume: async (ref) => {
        downloads.get(ref.nativeId)!.resumed = true;
      },
      removeRecord: async () => {
        stats.removals++;
        throw new Error("Acquisition must not remove torrents");
      },
    },
    library: {
      scan: async () => {
        stats.scans++;
      },
      verify: async () => verify,
    },
    fetchMetainfo: async (offer) => payloads.get(offer.infoHash!)!.metainfo,
    probe: async (file) => {
      stats.probes++;
      const payload = [...payloads.values()].find(
        (p) => inspectMetainfo(p.metainfo).name === path.basename(file),
      )!;
      return {
        streams: [
          {
            codec_type: "video",
            codec_name: "h264",
            width: payload.width,
            height: payload.width === 1280 ? 720 : 1080,
            color_transfer: "bt709",
          },
          { codec_type: "audio", codec_name: "aac", tags: { language: "eng" } },
        ],
        format: { duration: "12" },
      };
    },
    timing: { pollMs: 1, stallMs: 1500, plexAttempts: 2, plexRetryMs: 1 },
  };
  return {
    services,
    stats,
    addOffer,
    clearOffers: () => {
      offers = [];
    },
    setPlexVerified: (value: boolean) => {
      verify = value;
    },
    downloads,
  };
}
