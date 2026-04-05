import { parentPort } from "node:worker_threads";

import { runTranscriptAnalysis, type AnalysisResult, type TranscriptAnalysisInput } from "./chapter-analysis";

type AnalyzeRequest = {
  id: number;
  input: TranscriptAnalysisInput;
};

type AnalyzeResponse =
  | {
      id: number;
      result: AnalysisResult;
    }
  | {
      id: number;
      error: {
        name: string;
        message: string;
        stack?: string;
      };
    };

type ErrorPayload = Extract<AnalyzeResponse, { error: unknown }>["error"];

function serializeError(error: unknown): ErrorPayload {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack,
    };
  }
  return {
    name: "Error",
    message: typeof error === "string" ? error : JSON.stringify(error),
  };
}

const port = parentPort;

if (!port) {
  throw new Error("transcript analysis worker requires a parent port");
}

port.on("message", (message: AnalyzeRequest) => {
  let response: AnalyzeResponse;
  try {
    response = {
      id: message.id,
      result: runTranscriptAnalysis(message.input),
    };
  } catch (error) {
    response = {
      id: message.id,
      error: serializeError(error),
    };
  }
  port.postMessage(response);
});
