import { createWriteStream } from "node:fs";
import { mkdir } from "node:fs/promises";
import path from "node:path";

type LogTeePaths = {
  stdoutPath: string;
  stderrPath: string;
};

type LogTeeState = LogTeePaths & {
  installed: boolean;
};

const LOG_TEE_KEY = Symbol.for("podible.log_tee_state");

function globalState(): LogTeeState | undefined {
  return (globalThis as Record<PropertyKey, unknown>)[LOG_TEE_KEY] as LogTeeState | undefined;
}

function setGlobalState(state: LogTeeState): void {
  (globalThis as Record<PropertyKey, unknown>)[LOG_TEE_KEY] = state;
}

export async function installProcessLogTee(dataDir: string): Promise<LogTeePaths> {
  const existing = globalState();
  if (existing?.installed) {
    return {
      stdoutPath: existing.stdoutPath,
      stderrPath: existing.stderrPath,
    };
  }

  const logDir = path.join(dataDir, "logs");
  await mkdir(logDir, { recursive: true });

  const stdoutPath = path.join(logDir, "stdout.log");
  const stderrPath = path.join(logDir, "stderr.log");
  const stdoutFile = createWriteStream(stdoutPath, { flags: "a" });
  const stderrFile = createWriteStream(stderrPath, { flags: "a" });

  const originalStdoutWrite = process.stdout.write.bind(process.stdout) as (...args: unknown[]) => boolean;
  const originalStderrWrite = process.stderr.write.bind(process.stderr) as (...args: unknown[]) => boolean;

  (process.stdout as unknown as { write: (...args: unknown[]) => boolean }).write = (...args: unknown[]): boolean => {
    try {
      const chunk = args[0];
      if (typeof chunk === "string" || chunk instanceof Uint8Array) {
        stdoutFile.write(chunk);
      } else if (chunk != null) {
        stdoutFile.write(String(chunk));
      }
    } catch {
      // Logging should never break stdout.
    }
    return originalStdoutWrite(...args);
  };

  (process.stderr as unknown as { write: (...args: unknown[]) => boolean }).write = (...args: unknown[]): boolean => {
    try {
      const chunk = args[0];
      if (typeof chunk === "string" || chunk instanceof Uint8Array) {
        stderrFile.write(chunk);
      } else if (chunk != null) {
        stderrFile.write(String(chunk));
      }
    } catch {
      // Logging should never break stderr.
    }
    return originalStderrWrite(...args);
  };

  process.once("exit", () => {
    try {
      stdoutFile.end();
    } catch {}
    try {
      stderrFile.end();
    } catch {}
  });

  setGlobalState({
    installed: true,
    stdoutPath,
    stderrPath,
  });

  return { stdoutPath, stderrPath };
}
