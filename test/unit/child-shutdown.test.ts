import { expect, test } from "bun:test";
import { stopSupervisedChildren } from "../../src/runtime/child-shutdown";

test("a slow application cannot delay Transmission's graceful shutdown signal", async () => {
  const signals: string[] = [];
  let applicationExited!: () => void;
  const application = { exited: new Promise<void>(resolve => { applicationExited = resolve; }),
    kill(signal: string) { signals.push(`application:${signal}`); if (signal === "SIGKILL") applicationExited(); } };
  let daemonExited!: () => void;
  const daemon = { exited: new Promise<void>(resolve => { daemonExited = resolve; }),
    kill(signal: string) { signals.push(`daemon:${signal}`); daemonExited(); } };
  const stopped = stopSupervisedChildren([application, daemon], 10);
  expect(signals).toEqual(["application:SIGTERM", "daemon:SIGTERM"]);
  await stopped;
  expect(signals).toEqual(["application:SIGTERM", "daemon:SIGTERM", "application:SIGKILL"]);
});

test("all graceful exits clear their forced-kill deadlines", async () => {
  const signals: string[] = [];
  await stopSupervisedChildren([{ exited: Promise.resolve(), kill(signal) { signals.push(signal); } }], 5);
  await Bun.sleep(10);
  expect(signals).toEqual(["SIGTERM"]);
});
