import { selectManualImportPaths } from "../library/agents";
import { importReleaseFromPath, inspectImportPath, type ImportInspectionFile } from "../library/importer";
import { RtorrentClient } from "../rtorrent";
import { isRunControlError } from "reflow-ts";
import type { JobRow } from "../app-types";
import { durableStep, workerLog, type WorkerContext } from "./context";

type ImportJobPayload = {
  basePath?: string | null;
  infoHash?: string;
  selectedPaths?: string[];
  reason?: string;
  userReportedIssue?: boolean;
  rejectedSourcePaths?: string[];
  preferAgentFirst?: boolean;
  manifestationId?: number | null;
  sequenceInManifestation?: number | null;
};

function importableFilesForMedia(files: ImportInspectionFile[], mediaType: "audio" | "ebook"): ImportInspectionFile[] {
  return files.filter((file) => (mediaType === "audio" ? file.supportedAudio : file.supportedEbook));
}

function importNote(
  context: "wrong_file_review" | "agent_first" | "deterministic" | "agent_recovery",
  detail?: { mode?: string; trigger?: string; confidence?: number; reason?: string | null; selectedPaths?: string[] }
): string {
  const selectedCount = detail?.selectedPaths?.length;
  const selectedText =
    selectedCount === undefined ? "" : `; selected ${selectedCount} path${selectedCount === 1 ? "" : "s"}`;
  if (!detail?.reason) {
    return `${context}${selectedText}`;
  }
  const mode = detail.mode ? `${detail.mode}` : "unknown";
  const trigger = detail.trigger ? `, trigger ${detail.trigger}` : "";
  const confidence = typeof detail.confidence === "number" ? `, confidence ${detail.confidence.toFixed(2)}` : "";
  return `${context}: ${mode}${trigger}${confidence}: ${detail.reason}${selectedText}`;
}

export async function processImportJob(ctx: WorkerContext, job: JobRow): Promise<"done"> {
  if (!job.release_id) {
    throw new Error("Import job missing release_id");
  }

  const release = ctx.repo.getRelease(job.release_id);
  if (!release) {
    throw new Error(`Release ${job.release_id} not found`);
  }

  const settings = ctx.getSettings();
  const payload = job.payload_json ? (JSON.parse(job.payload_json) as ImportJobPayload) : {};
  const client = new RtorrentClient(settings.rtorrent);
  const importSource = await durableStep(ctx, "import:source", async () => {
    const source =
      payload.basePath || (Array.isArray(payload.selectedPaths) && payload.selectedPaths.length > 0)
        ? {
            basePath: payload.basePath ?? null,
            selectedPaths: Array.isArray(payload.selectedPaths) ? payload.selectedPaths : []
          }
        : await client.getImportSource(release.info_hash);
    if (!source.basePath) throw new Error("Import source is not ready");
    return source;
  });
  const basePath = importSource.basePath;
  if (!basePath) {
    throw new Error("Import source is not ready");
  }
  workerLog(
    ctx,
    `[import] job=${job.id} release=${release.id} start base_path=${JSON.stringify(basePath)} prefer_agent_first=${payload.preferAgentFirst === true} user_reported_issue=${payload.userReportedIssue === true}`
  );

  if (payload.userReportedIssue === true) {
    let agentDecisionReason = "not_attempted";
    let agentDecisionError: string | null = null;
    try {
      const files = await durableStep(ctx, "import:review:inspect", () => inspectImportPath(basePath));
      const importableFiles = importableFilesForMedia(files, release.media_type);
      if (importableFiles.length <= 1) {
        agentDecisionReason = `deterministic:no_alternative_single_importable_file:${importableFiles.length}`;
      } else {
        const book = ctx.repo.getBookRow(release.book_id);
        const decision = await durableStep(ctx, "import:review:select", () =>
          selectManualImportPaths(
            settings,
            {
              mediaType: release.media_type,
              files,
              rejectedSourcePaths: Array.isArray(payload.rejectedSourcePaths) ? payload.rejectedSourcePaths : [],
              forceAgent: true,
              priorFailure: true,
              book: book ? { id: book.id, title: book.title, author: book.author } : null
            },
            { modelProvider: ctx.modelProvider }
          )
        );
        agentDecisionReason = `${decision.mode}:${decision.reason}`;
        agentDecisionError = decision.error;

        if (decision.selectedPaths.length > 0) {
          await durableStep(ctx, "import:review:publish", () =>
            importReleaseFromPath(ctx.repo, release, basePath, settings.libraryRoot, {
              selectedPaths: decision.selectedPaths,
              manifestationId: payload.manifestationId ?? null,
              sequenceInManifestation: payload.sequenceInManifestation ?? null,
              importNote: importNote("wrong_file_review", decision),
              importKey: `import:${job.id}`
            })
          );
          ctx.repo.setReleaseStatus(release.id, "imported", null);
          workerLog(
            ctx,
            `[import] job=${job.id} release=${release.id} wrong_file_agent_reimport=success selected=${decision.selectedPaths.length}`
          );
          return "done";
        }
      }
    } catch (agentImportError) {
      if (isRunControlError(agentImportError)) throw agentImportError;
      agentDecisionError = (agentImportError as Error).message || "agent import failed";
      agentDecisionReason = "agent_import_exception";
      workerLog(
        ctx,
        `[import] job=${job.id} release=${release.id} wrong_file_agent_error=${JSON.stringify(agentDecisionError)}`
      );
    }

    const terminalError = `Wrong-file review failed to produce alternate import. decision=${agentDecisionReason}; agentError=${agentDecisionError ?? "none"}`;
    await durableStep(ctx, "import:review:recover", () =>
      ctx.repo.transaction(() => {
        ctx.repo.setReleaseStatus(release.id, "failed", terminalError);
        ctx.repo.createJob({
          type: "acquire",
          bookId: release.book_id,
          idempotencyKey: `import:${job.id}:recover`,
          payload: {
            bookId: release.book_id,
            media: [release.media_type],
            priorFailure: true,
            forceAgent: true,
            rejectedUrls: [release.url],
            rejectedGuids: release.provider_guid ? [release.provider_guid] : [],
            rejectedInfoHashes: release.info_hash ? [release.info_hash] : []
          }
        });
        ctx.repo.markJobCancelled(job.id, `${terminalError}; recoveryQueued=true`);
      })
    );
    workerLog(
      ctx,
      `[import] job=${job.id} release=${release.id} wrong_file_agent_reimport=none queued_acquire_forced_agent=1`
    );
    return "done";
  }

  if (payload.preferAgentFirst === true) {
    let agentFirstError: string | null = null;
    try {
      const files = await durableStep(ctx, "import:agent-first:inspect", () => inspectImportPath(basePath));
      const book = ctx.repo.getBookRow(release.book_id);
      const decision = await durableStep(ctx, "import:agent-first:select", () =>
        selectManualImportPaths(
          settings,
          {
            mediaType: release.media_type,
            files,
            forceAgent: true,
            priorFailure: false,
            book: book ? { id: book.id, title: book.title, author: book.author } : null
          },
          { modelProvider: ctx.modelProvider }
        )
      );
      if (decision.selectedPaths.length > 0) {
        await durableStep(ctx, "import:agent-first:publish", () =>
          importReleaseFromPath(ctx.repo, release, basePath, settings.libraryRoot, {
            selectedPaths: decision.selectedPaths,
            manifestationId: payload.manifestationId ?? null,
            sequenceInManifestation: payload.sequenceInManifestation ?? null,
            importNote: importNote("agent_first", decision),
            importKey: `import:${job.id}`
          })
        );
        ctx.repo.setReleaseStatus(release.id, "imported", null);
        workerLog(
          ctx,
          `[import] job=${job.id} release=${release.id} agent_first=success mode=${decision.mode} selected=${decision.selectedPaths.length}`
        );
        return "done";
      }
      workerLog(
        ctx,
        `[import] job=${job.id} release=${release.id} agent_first=no_selection mode=${decision.mode} reason=${JSON.stringify(decision.reason)}`
      );
    } catch (error) {
      if (isRunControlError(error)) throw error;
      agentFirstError = (error as Error).message || "agent-first import failed";
      workerLog(
        ctx,
        `[import] job=${job.id} release=${release.id} agent_first_error=${JSON.stringify(agentFirstError)}`
      );
    }
  }

  try {
    await durableStep(ctx, "import:deterministic:publish", () =>
      importReleaseFromPath(ctx.repo, release, basePath, settings.libraryRoot, {
        selectedPaths: importSource.selectedPaths,
        manifestationId: payload.manifestationId ?? null,
        sequenceInManifestation: payload.sequenceInManifestation ?? null,
        importNote: importNote("deterministic", { selectedPaths: importSource.selectedPaths }),
        importKey: `import:${job.id}`
      })
    );
    ctx.repo.setReleaseStatus(release.id, "imported", null);
    return "done";
  } catch (firstError) {
    if (isRunControlError(firstError)) throw firstError;
    const deterministicMessage = (firstError as Error).message || "deterministic import failed";
    workerLog(
      ctx,
      `[import] job=${job.id} release=${release.id} deterministic_error=${JSON.stringify(deterministicMessage)}`
    );

    let agentDecisionReason = "not_attempted";
    let agentDecisionError: string | null = null;
    try {
      const files = await durableStep(ctx, "import:recovery:inspect", () => inspectImportPath(basePath));
      const book = ctx.repo.getBookRow(release.book_id);
      const decision = await durableStep(ctx, "import:recovery:select", () =>
        selectManualImportPaths(
          settings,
          {
            mediaType: release.media_type,
            files,
            forceAgent: true,
            priorFailure: true,
            book: book ? { id: book.id, title: book.title, author: book.author } : null
          },
          { modelProvider: ctx.modelProvider }
        )
      );
      agentDecisionReason = `${decision.mode}:${decision.reason}`;
      agentDecisionError = decision.error;

      if (decision.selectedPaths.length > 0) {
        await durableStep(ctx, "import:recovery:publish", () =>
          importReleaseFromPath(ctx.repo, release, basePath, settings.libraryRoot, {
            selectedPaths: decision.selectedPaths,
            manifestationId: payload.manifestationId ?? null,
            sequenceInManifestation: payload.sequenceInManifestation ?? null,
            importNote: importNote("agent_recovery", decision),
            importKey: `import:${job.id}`
          })
        );
        ctx.repo.setReleaseStatus(release.id, "imported", null);
        workerLog(
          ctx,
          `[import] job=${job.id} release=${release.id} agent_recovery=success selected=${decision.selectedPaths.length}`
        );
        return "done";
      }
    } catch (agentImportError) {
      if (isRunControlError(agentImportError)) throw agentImportError;
      agentDecisionError = (agentImportError as Error).message || "agent import failed";
      agentDecisionReason = "agent_import_exception";
      workerLog(ctx, `[import] job=${job.id} release=${release.id} agent_error=${JSON.stringify(agentDecisionError)}`);
    }

    const terminalError = `Import failed after deterministic+agent attempts. deterministic=${deterministicMessage}; decision=${agentDecisionReason}; agentError=${agentDecisionError ?? "none"}`;
    await durableStep(ctx, "import:recovery:reacquire", () =>
      ctx.repo.transaction(() => {
        ctx.repo.setReleaseStatus(release.id, "failed", terminalError);
        ctx.repo.createJob({
          type: "acquire",
          bookId: release.book_id,
          idempotencyKey: `import:${job.id}:recover`,
          payload: {
            bookId: release.book_id,
            media: [release.media_type],
            priorFailure: true,
            forceAgent: true,
            rejectedUrls: [release.url],
            rejectedGuids: release.provider_guid ? [release.provider_guid] : [],
            rejectedInfoHashes: release.info_hash ? [release.info_hash] : []
          }
        });
        ctx.repo.markJobCancelled(job.id, `${terminalError}; recoveryQueued=true`);
      })
    );
    workerLog(ctx, `[import] job=${job.id} release=${release.id} agent_recovery=none queued_acquire_forced_agent=1`);
    return "done";
  }
}
