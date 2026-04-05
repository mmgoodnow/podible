import { promises as fs, mkdirSync } from "node:fs";
import path from "node:path";

const tmpRoot = process.env.TMPDIR ?? "/tmp";
const configDir = process.env.CONFIG_DIR ?? path.join(tmpRoot, "podible-config");
const booksDbPath = path.join(configDir, "podible.sqlite");
const port = Number(process.env.PORT ?? 80);

async function ensureConfigDir() {
  await fs.mkdir(configDir, { recursive: true });
}

function ensureConfigDirSync() {
  try {
    mkdirSync(configDir, { recursive: true });
  } catch {
    // ignore sync mkdir errors; async ensureConfigDir also runs elsewhere
  }
}

export {
  configDir,
  booksDbPath,
  port,
  ensureConfigDir,
  ensureConfigDirSync,
};
