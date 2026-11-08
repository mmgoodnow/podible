export type BackgroundWorkerInput = { databasePath: string };
export type BackgroundWorkerReady = {
  type: "ready";
  pid: number;
  threadId: number;
  busyTimeoutMs: number;
};
export type BackgroundWorkerMessage = BackgroundWorkerReady | { type: "stopped" } | { type: "failed" }
  | { type: "heartbeat"; stalledEffects: number; activeEffects: number; reservations: number };
export type BackgroundWorkerCommand = { type: "stop" };
