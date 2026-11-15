import type { TvFeedMatcher, TvFeedOffer, TvFeedTarget } from "../../src/tv/feed";
import type { TvEpisode } from "../../src/tv/types";

type Work = { title: string; year: number; alias?: string; season?: number; absoluteStart?: number; count?: number };
type Offer = { title: string; targets: number[] };
export type TvFeedCase = {
  id: string;
  split: "dev" | "holdout" | "sparse";
  mode: "acquire" | "reuse";
  input: Parameters<TvFeedMatcher>[0];
  expected: number[][];
};

// Public work names, synthetic offers and catalog scopes. Indexes are not provider IDs.
function fixture(id: string, split: TvFeedCase["split"], mode: TvFeedCase["mode"], works: Work[], positives: Offer[],
  negatives: string[], promotion?: number): TvFeedCase {
  const targets = works.map((work, index): TvFeedTarget => {
    const key = `tvdb:${index + 1}` as const, season = work.season ?? 1;
    const episodes: TvEpisode[] = Array.from({ length: work.count ?? 6 }, (_, i) => ({
      key: `${key}:${i + 1}`, id: 1000 * (index + 1) + i, title: `Episode ${i + 1}`, airDate: null, runtimeMinutes: null,
      refs: [], identity: { stability: "provider", source: "fixture" }, numbering: [
        { scheme: "tvdb-aired", season, episode: i + 1, absolute: null, source: "fixture" },
        ...(work.absoluteStart === undefined ? [] : [{ scheme: "absolute", season: null, episode: null,
          absolute: work.absoluteStart + i, source: "fixture" }]),
      ],
    }));
    return { targetId: index + 1, episodes, entry: { id: index + 1, key, title: work.title, year: work.year,
      aliases: work.alias ? [work.alias] : [], tvdbId: null, tmdbId: null, anime: false, episodes, relations: [], refs: [],
      posterUrl: null, backdropUrl: null },
      ...(promotion !== undefined && index === 0 ? { promotionSeasons: [promotion] } : {}) };
  });
  const rows = [...positives, ...negatives.map(title => ({ title, targets: [] }))];
  // Interleave gold positives and negatives without teaching the model a fixture ordering.
  const seed = (n: number) => Math.imul(n + id.length, 0x9e3779b1) >>> 0;
  const shuffled = rows.map((row, i) => ({ row, position: seed(i) })).sort((a, b) => a.position - b.position).map(x => x.row);
  const offers: TvFeedOffer[] = shuffled.map(({ title }) => ({ title, provider: "Fixture", sizeBytes: 5_000_000,
    seeders: 5, attributes: [] }));
  return { id, split, mode, input: { targets, offers }, expected: shuffled.map(row => row.targets) };
}

const offer = (target: number, ...titles: string[]): Offer[] => titles.map(title => ({ title, targets: [target] }));
const noise = ["Abbott Elementary S03E01", "The Bear S02E07", "The Traitors S01E02", "Bluey S03E04", "The Pitt S01E03",
  "Poker Face S02E04", "Only Murders in the Building S04E06", "Slow Horses S04E03", "My Brilliant Friend S03E04", "Matlock 2024 S01E01",
  "The Diplomat 2023 S02E03", "Severance S01 1080p", "Young Sheldon S06E03", "Mad Men S01 BluRay", "Industry S02E03",
  "Watchmen 2019 S01E04", "The Marvelous Mrs Maisel S02E03", "Dark S02E04", "Buffy the Vampire Slayer S04E03", "High Potential S01E02",
  "The White Lotus S02E02", "Heartstopper S01 1080p", "Ted Lasso S03E04", "NewsNight S02E03", "The Expanse S01E03"];

export const tvFeedCases = [
  fixture("ordinary-tv", "dev", "acquire", [{ title: "Detectorists", year: 2014 }, { title: "Stick", year: 2025, season: 2 }], [
    ...offer(1, "Detectorists S01E02", "Detectorists 2014 Season 1 Complete", "Detectorists 1x03"),
    ...offer(2, "Stick S02E02", "Stick 2025 2x01 1080p", "Stick S02E03 WEB-DL"),
  ], [...noise, "The Stick Up S02E02", "Stick 1985 1080p"]),
  fixture("long-numbering-catalog", "dev", "acquire", [{ title: "One Piece", year: 1999, season: 21, absoluteStart: 892, count: 194 },
    { title: "Attack on Titan", alias: "Shingeki no Kyojin", year: 2013, season: 4, absoluteStart: 60, count: 30 }], [
    ...offer(1, "[Fixture] One Piece - 1015 1080p", "One Piece S21E01 1080p", "One Piece 1999 Season 21", "One Piece 21x10"),
    ...offer(2, "Shingeki no Kyojin - 63", "Attack on Titan S04E30", "Attack on Titan The Final Season Complete", "Shingeki no Kyojin S04E02"),
  ], [...noise, "ONE PIECE 2023 S01E01 Live Action", "Titan A.E. 2000"]),
  fixture("retained-retro", "dev", "reuse", [{ title: "Tom and Jerry", year: 1940 }, { title: "Berserk", alias: "Kenpuu Denki Berserk", year: 1997 }], [
    ...offer(1, "Tom and Jerry 1940-1949 Complete", "Tom & Jerry (1940) S1940E01", "Tom And Jerry Classic Collection 1940"),
    ...offer(2, "Kenpuu Denki Berserk Complete 1997", "Berserk 1997 S01E12", "[Fixture] Berserk 1997 - 03"),
  ], [...noise, "Berserk 2016 Season 1", "Tom and Jerry Tales 2006 S01E02"]),
  fixture("season-upgrade", "dev", "acquire", [{ title: "Khom Khlang", alias: "Khom Klang", year: 2026, count: 10 }],
    offer(1, "Khom Khlang S01 COMPLETE", "Khom Klang Season 01 1080p Pack", "Khom Khlang 2026 S01 WEB-DL"),
    [...noise, "Khom Khlang S02 Complete"], 1),
  fixture("holdout-title-collisions", "holdout", "acquire", [{ title: "The Office", year: 2005, season: 2, count: 22 },
    { title: "Ghosts", year: 2019 }], [
    ...offer(1, "The Office US S02E08", "The Office 2005 Season 2 Complete", "The.Office.US.2x04"),
    ...offer(2, "Ghosts UK 2019 S01E05", "Ghosts 2019 Series 1", "Ghosts BBC S01E03"),
  ], [...noise, "The Office UK 2001 S02E04", "Ghosts US 2021 S01E02", "Ghost 1990"]),
  fixture("holdout-alternate-order", "holdout", "acquire", [{ title: "Fullmetal Alchemist: Brotherhood", alias: "Hagane no Renkinjutsushi Fullmetal Alchemist", year: 2009, count: 64, absoluteStart: 1 },
    { title: "Sense8", year: 2015, season: 0 }], [
    ...offer(1, "Fullmetal Alchemist Brotherhood - 22", "Hagane no Renkinjutsushi Fullmetal Alchemist 2009 - 46", "Fullmetal Alchemist Brotherhood 2009 Complete"),
    ...offer(2, "Sense8 S00E01", "Sense8 2015 Specials", "Sense8 2015 S00E02"),
  ], [...noise, "Fullmetal Alchemist 2003 S01", "Sense and Sensibility 2008 S01"]),
  fixture("holdout-translated-series", "holdout", "reuse", [{ title: "Money Heist", alias: "La casa de papel", year: 2017 },
    { title: "Scavengers Reign", year: 2023 }, { title: "Frieren: Beyond Journey's End", alias: "Sousou no Frieren", year: 2023 }], [
    ...offer(1, "La Casa de Papel Parte 3 Completa", "Money Heist 2017 S02E02", "La.Casa.De.Papel.2017.S01E03"),
    ...offer(2, "Scavengers.Reign.S01E12", "Scavengers Reign 2023 Season 1"),
    ...offer(3, "[Fixture] Sousou no Frieren - 08", "Frieren Beyond Journeys End S01 1080p", "Sousou no Frieren 2023 Complete"),
  ], [...noise, "Money Heist Korea Joint Economic Area S01", "Scavengers 2013", "Journey to the West 1986 S01"]),
  fixture("holdout-dated-seasons", "holdout", "acquire", [{ title: "Looney Tunes", year: 1930, season: 1948, count: 20 },
    { title: "Battlestar Galactica", year: 2004, season: 4 }], [
    ...offer(1, "Looney Tunes 1948 Complete", "Looney Tunes S1948E03", "Looney Tunes 1948 Season Pack"),
    ...offer(2, "Battlestar Galactica 2004 4x01", "Battlestar Galactica 2004 Season 4", "Battlestar.Galactica.2004.S04E04"),
  ], [...noise, "The Looney Tunes Show 2011 S02E01", "Battlestar Galactica 1978 S01E04"]),
  fixture("holdout-multi-page", "holdout", "reuse", [
    ...["Schitt's Creek", "Happy Valley", "Parks and Recreation", "Yellowjackets", "The Leftovers", "The Americans",
      "Rectify", "Orphan Black", "Gentleman Jack", "Halt and Catch Fire", "The Knick", "Fleabag"].map(title => ({ title, year: 2015 })),
    { title: "Better Call Saul", year: 2015 }, { title: "Derry Girls", year: 2018 },
  ], [...offer(3, "Parks and Recreation S04E06"), ...offer(12, "Fleabag S01E06"),
    ...offer(13, "Better.Call.Saul.S06.1080p"), ...offer(14, "Derry Girls Series 2 Complete")],
  [...noise, ...noise.map(title => `${title} English 1080p`), ...noise.map(title => `${title} 720p WEB`)]),
];

const stressWorks = [{ title: "The Office", year: 2005, season: 2, count: 22 },
  { title: "Money Heist", alias: "La casa de papel", year: 2017 },
  { title: "Attack on Titan", alias: "Shingeki no Kyojin", year: 2013, season: 4, absoluteStart: 60, count: 30 }];
export const sparseTvFeedCases = [
  fixture("sparse-no-match", "sparse", "acquire", stressWorks, [], noise),
  fixture("sparse-only-confusable-titles", "sparse", "acquire", stressWorks, [],
    [...noise, "The Office UK 2001 S02E04", "Money Heist Korea Joint Economic Area S01", "Titan A.E. 2000"]),
  fixture("sparse-one-exact-title", "sparse", "acquire", stressWorks, offer(1, "The Office US S02E08"), noise),
  fixture("sparse-one-translated-title", "sparse", "reuse", stressWorks, offer(2, "La.Casa.De.Papel.2017.S01E03"), noise),
  fixture("sparse-one-absolute-episode", "sparse", "acquire", stressWorks, offer(3, "[Fixture] Shingeki no Kyojin - 63"), noise),
];
