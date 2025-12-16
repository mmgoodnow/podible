import { promises as fs, mkdirSync, statSync } from "node:fs";
import path from "node:path";

const FEED_TITLE = process.env.POD_TITLE ?? "Podible Audiobooks";
const FEED_DESCRIPTION = process.env.POD_DESCRIPTION ?? "Podcast feed for audiobooks";
const FEED_LANGUAGE = process.env.POD_LANGUAGE ?? "en-us";
const FEED_COPYRIGHT = process.env.POD_COPYRIGHT ?? "";
const FEED_AUTHOR = process.env.POD_AUTHOR ?? "Unknown";
const FEED_OWNER_NAME = process.env.POD_OWNER_NAME ?? "Owner";
const FEED_OWNER_EMAIL = process.env.POD_OWNER_EMAIL ?? "owner@example.com";
const rawExplicit = (process.env.POD_EXPLICIT ?? "clean").toLowerCase();
const FEED_EXPLICIT = ["yes", "no", "clean"].includes(rawExplicit) ? rawExplicit : "clean";
const FEED_CATEGORY = process.env.POD_CATEGORY ?? "Arts";
const FEED_TYPE = process.env.POD_TYPE ?? "episodic";
const FEED_IMAGE_URL = process.env.POD_IMAGE_URL;

const dataDir = process.env.DATA_DIR ?? path.join(process.env.TMPDIR ?? "/tmp", "podible-transcodes");
const transcodeStatusPath = path.join(dataDir, "transcode-status.json");
const libraryIndexPath = path.join(dataDir, "library-index.json");
const probeCachePath = path.join(dataDir, "probe-cache.json");
const brandImagePath = path.join(process.cwd(), "podible.png");
const apiKeyPath = path.join(dataDir, "api-key.txt");
const port = Number(process.env.PORT ?? 80);

const brandImageExists = (() => {
  try {
    const stat = statSync(brandImagePath);
    return stat.isFile();
  } catch {
    return false;
  }
})();

async function ensureDataDir() {
  await fs.mkdir(dataDir, { recursive: true });
}

function ensureDataDirSync() {
  try {
    mkdirSync(dataDir, { recursive: true });
  } catch {
    // ignore sync mkdir errors; async ensureDataDir also runs elsewhere
  }
}

export {
  FEED_TITLE,
  FEED_DESCRIPTION,
  FEED_LANGUAGE,
  FEED_COPYRIGHT,
  FEED_AUTHOR,
  FEED_OWNER_NAME,
  FEED_OWNER_EMAIL,
  FEED_EXPLICIT,
  FEED_CATEGORY,
  FEED_TYPE,
  FEED_IMAGE_URL,
  dataDir,
  transcodeStatusPath,
  libraryIndexPath,
  probeCachePath,
  brandImagePath,
  apiKeyPath,
  brandImageExists,
  port,
  ensureDataDir,
  ensureDataDirSync,
};
