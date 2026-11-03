import path from "node:path";
import { configDir } from "../src/config";
import { prepareTransmission } from "../src/media/managed-transmission";
import { TransmissionRpc } from "../src/media/transmission-client";

let stopping = false;
const children = new Map<string, ReturnType<typeof Bun.spawn>>();
async function supervise(
  name: string,
  command: string[],
  beforeStart?: () => void,
) {
  let failures = 0;
  while (!stopping) {
    let child: ReturnType<typeof Bun.spawn> | undefined;
    const started = Date.now();
    try {
      beforeStart?.();
      child = Bun.spawn(command, {
        stdin: "ignore",
        stdout: "inherit",
        stderr: "inherit",
      });
      children.set(name, child);
      await child.exited;
    } catch {
      console.error(`[supervisor] ${name} failed to start`);
    } finally {
      if (child) children.delete(name);
    }
    if (!stopping) {
      failures = Date.now() - started > 60_000 ? 1 : failures + 1;
      console.error(`[supervisor] ${name} exited; restarting`);
      await Bun.sleep(Math.min(30_000, 500 * 2 ** Math.min(failures, 6)));
    }
  }
}
async function shutdown() {
  if (stopping) return;
  stopping = true;
  // Stop the application before its private engine; bound shutdown for Docker.
  const running = [children.get("application"), children.get("torrent engine")].filter((child): child is ReturnType<typeof Bun.spawn> => Boolean(child));
  for (const child of running) {
    child.kill("SIGTERM");
    const timer = setTimeout(() => child.kill("SIGKILL"), 10_000);
    await child.exited;
    clearTimeout(timer);
  }
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
const directory = path.join(configDir, "transmission");
let identity: ReturnType<typeof prepareTransmission> | undefined;
try {
  identity = prepareTransmission(directory);
} catch {
  console.error(
    "[supervisor] torrent engine configuration unavailable; application remains available",
  );
}
const daemon = supervise(
  "torrent engine",
  ["transmission-daemon", "--foreground", "--config-dir", directory],
  () => {
    prepareTransmission(directory);
  },
);
const rpc = identity
  ? new TransmissionRpc({
      url: "http://127.0.0.1:9091/transmission/rpc",
      username: identity.username,
      password: identity.password,
      timeoutMs: 800,
    })
  : null;
let ready = false;
for (let attempt = 0; rpc && attempt < 5 && !stopping; attempt++) {
  try {
    await rpc.call("session-get");
    ready = true;
    break;
  } catch {
    await Bun.sleep(500);
  }
}
console.log(
  `[supervisor] torrent engine ${ready ? "ready" : "not ready; application remains available"}`,
);
if (!stopping)
  await supervise("application", [
    process.execPath,
    "run",
    "server.ts",
    ...process.argv.slice(2),
  ]);
await daemon;
