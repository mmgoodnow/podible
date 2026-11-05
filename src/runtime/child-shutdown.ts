export interface SupervisedChild {
  kill(signal: "SIGTERM" | "SIGKILL"): void;
  exited: Promise<unknown>;
}

/** Docker normally grants ten seconds to PID 1, not ten seconds per child. */
export async function stopSupervisedChildren(children: SupervisedChild[], timeoutMs = 8_000) {
  for (const child of children) child.kill("SIGTERM");
  await Promise.all(children.map(async child => {
    const timer = setTimeout(() => child.kill("SIGKILL"), timeoutMs);
    try { await child.exited; }
    finally { clearTimeout(timer); }
  }));
}
