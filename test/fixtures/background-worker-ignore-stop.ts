import { parentPort, threadId } from "node:worker_threads";

parentPort!.on("message", () => {});
parentPort!.postMessage({ type: "ready", pid: process.pid, threadId, busyTimeoutMs: 2000 });
