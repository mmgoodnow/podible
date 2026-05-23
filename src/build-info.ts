import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, statSync } from "node:fs";
import { dirname, isAbsolute, join, resolve } from "node:path";

export type BuildInfo = {
  sha: string;
  message: string;
};

export const BUILD_INFO = getBuildInfo();

export function getBuildInfo(): BuildInfo | null {
  const envInfo = buildInfoFromEnv();
  if (envInfo) return envInfo;
  const fileInfo = buildInfoFromGitFiles();
  if (fileInfo) {
    return {
      sha: fileInfo.sha,
      message: runGit(["log", "-1", "--pretty=%s"]) || fileInfo.message,
    };
  }
  return buildInfoFromGitCommand();
}

export function formatProcessUptime(uptimeSeconds = process.uptime()): string {
  const totalSeconds = Math.max(0, Math.floor(uptimeSeconds));
  const days = Math.floor(totalSeconds / 86_400);
  const hours = Math.floor((totalSeconds % 86_400) / 3_600);
  const minutes = Math.floor((totalSeconds % 3_600) / 60);
  const seconds = totalSeconds % 60;
  if (days > 0) return `${days}d ${hours}h ${pad(minutes)}m`;
  if (hours > 0) return `${hours}h ${pad(minutes)}m`;
  if (minutes > 0) return `${minutes}m ${pad(seconds)}s`;
  return `${seconds}s`;
}

function buildInfoFromEnv(): BuildInfo | null {
  const sha = process.env.GIT_COMMIT_SHA || process.env.GIT_SHA || "";
  const message = process.env.GIT_COMMIT_MESSAGE || "";
  if (!sha && !message) return null;
  return {
    sha: sha || "unknown",
    message: message || "unknown commit message",
  };
}

function buildInfoFromGitCommand(): BuildInfo | null {
  const sha = runGit(["rev-parse", "HEAD"]);
  if (!sha) return null;
  const message = runGit(["log", "-1", "--pretty=%s"]) || "unknown commit message";
  return { sha, message };
}

function buildInfoFromGitFiles(): BuildInfo | null {
  const gitDir = resolveGitDir();
  if (!gitDir) return null;
  const head = readText(join(gitDir, "HEAD"));
  if (!head) return null;
  const refPrefix = "ref:";
  const refName = head.startsWith(refPrefix) ? head.slice(refPrefix.length).trim() : "";
  const sha = refName ? readText(join(gitDir, refName)) || readPackedRef(gitDir, refName) : head;
  if (!sha) return null;
  return { sha, message: "unknown commit message" };
}

function resolveGitDir(): string | null {
  const gitPath = join(process.cwd(), ".git");
  if (isDirectory(gitPath)) return gitPath;
  const gitFile = readText(gitPath);
  const prefix = "gitdir:";
  if (!gitFile?.startsWith(prefix)) return null;
  const target = gitFile.slice(prefix.length).trim();
  return isAbsolute(target) ? target : resolve(dirname(gitPath), target);
}

function readPackedRef(gitDir: string, refName: string): string | null {
  const packedRefs = readText(join(gitDir, "packed-refs"));
  if (!packedRefs) return null;
  for (const line of packedRefs.split("\n")) {
    if (line.startsWith("#") || !line.trim()) continue;
    const [sha, ref] = line.split(" ");
    if (ref === refName) return sha;
  }
  return null;
}

function runGit(args: string[]): string | null {
  const result = spawnSync("git", args, { cwd: process.cwd(), encoding: "utf8" });
  if (result.status !== 0) return null;
  return result.stdout.trim() || null;
}

function readText(path: string): string | null {
  if (!existsSync(path)) return null;
  return readFileSync(path, "utf8").trim();
}

function isDirectory(path: string): boolean {
  try {
    return statSync(path).isDirectory();
  } catch {
    return false;
  }
}

function pad(value: number): string {
  return String(value).padStart(2, "0");
}
