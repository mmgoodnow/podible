import { createHash } from "node:crypto";

import { ensureConfigDir } from "../src/config";
import { createSessionToken, hashSessionToken, sessionExpiresAt } from "../src/books/auth";
import { openDatabase } from "../src/books/db";
import { BooksRepo } from "../src/books/repo";
import type { AuthProvider } from "../src/books/types";

function usage(): never {
  console.error('Usage: bun run dev-login --user "Michael Goodnow" [--admin] [--origin http://localhost:3187]');
  process.exit(1);
}

function parseArgs(argv: string[]): { user: string; admin: boolean; origin: string } {
  let user = "";
  let admin = false;
  let origin = process.env.PODIBLE_DEV_ORIGIN ?? `http://localhost:${process.env.PORT ?? "3187"}`;

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--user") {
      user = argv[i + 1] ?? "";
      i += 1;
      continue;
    }
    if (arg === "--admin") {
      admin = true;
      continue;
    }
    if (arg === "--origin") {
      origin = argv[i + 1] ?? "";
      i += 1;
      continue;
    }
    usage();
  }

  if (!user.trim()) {
    usage();
  }

  try {
    const parsed = new URL(origin);
    origin = parsed.origin;
  } catch {
    console.error(`Invalid --origin: ${origin}`);
    process.exit(1);
  }

  return {
    user: user.trim(),
    admin,
    origin,
  };
}

function slugify(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function stableLocalProviderUserId(displayName: string): string {
  const slug = slugify(displayName) || "dev-user";
  const digest = createHash("sha1").update(displayName.trim().toLowerCase()).digest("hex").slice(0, 8);
  return `dev:${slug}:${digest}`;
}

function recommendedCookieCommands(origin: string, token: string): string[] {
  const { hostname } = new URL(origin);
  const commands = [
    `playwright-cli cookie-set podible_session '${token}' --domain=${hostname} --path=/ --httpOnly`,
  ];
  if (hostname === "localhost") {
    commands.push(`playwright-cli cookie-set podible_session '${token}' --domain=127.0.0.1 --path=/ --httpOnly`);
  }
  return commands;
}

const options = parseArgs(process.argv.slice(2));
await ensureConfigDir();

const { booksDbPath } = await import("../src/config");
const db = openDatabase(booksDbPath);
const repo = new BooksRepo(db);

try {
  repo.ensureSettings();
  const provider = "local" as AuthProvider;
  const providerUserId = stableLocalProviderUserId(options.user);
  const existing = repo.listUsers(provider).find((user) => user.provider_user_id === providerUserId) ?? null;
  const username = slugify(options.user) || "dev-user";
  const user = repo.upsertUser({
    provider,
    providerUserId,
    username,
    displayName: options.user,
    isAdmin: options.admin || existing?.is_admin === 1,
  });

  const token = createSessionToken();
  const session = repo.createSession(user.id, hashSessionToken(token), sessionExpiresAt(), "browser");
  const loginUrl = `${options.origin}/`;

  console.log(`Dev login created for ${options.user}`);
  console.log(`User id: ${user.id}`);
  console.log(`Provider: ${user.provider}`);
  console.log(`Admin: ${user.is_admin ? "yes" : "no"}`);
  console.log(`Expires: ${session.expires_at}`);
  console.log("");
  console.log(`Open: ${loginUrl}`);
  console.log(`Cookie: podible_session=${token}`);
  console.log("");
  console.log("For playwright-cli:");
  for (const command of recommendedCookieCommands(options.origin, token)) {
    console.log(`  ${command}`);
  }
  console.log(`  playwright-cli open ${loginUrl}`);
} finally {
  db.close();
}
