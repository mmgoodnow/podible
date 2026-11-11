import { parentPort, workerData } from "node:worker_threads";
import { finalizeCanonicalDownloadFile } from "../../src/media/canonical-download";
import { withExecutionEffects } from "../../src/runtime/execution-effects";

const { contentRoot, sourceRoot, view, size } = workerData;
const finalize = () => withExecutionEffects({
  jobId: 1, runId: "fixture", stepName: "tv:attempt:0:adopt:0", signal: new AbortController().signal,
}, () => finalizeCanonicalDownloadFile(contentRoot, sourceRoot, view, size));
const first = await finalize();
const replay = await finalize();
parentPort!.postMessage({ first, replay });
parentPort!.close();
