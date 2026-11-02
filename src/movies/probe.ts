import { z } from "zod";
import type { MovieFacts } from "./quality";

const streamSchema = z.object({
  codec_type: z.string(),
  codec_name: z.string().optional(),
  width: z.number().optional(),
  height: z.number().optional(),
  color_transfer: z.string().optional(),
  tags: z.object({ language: z.string().optional() }).optional(),
  disposition: z.object({ attached_pic: z.number().optional() }).optional(),
});
export const movieProbeSchema = z.object({
  streams: z.array(streamSchema),
  format: z.object({ duration: z.string().optional() }).optional(),
});
export type MovieProbe = z.infer<typeof movieProbeSchema>;
export async function probeMovie(file: string): Promise<MovieProbe> {
  const subprocess = Bun.spawn(
    [
      "ffprobe",
      "-protocol_whitelist",
      "file,pipe",
      "-v",
      "error",
      "-print_format",
      "json",
      "-show_streams",
      "-show_format",
      file,
    ],
    { stdout: "pipe", stderr: "pipe", timeout: 120_000 },
  );
  const [stdout, stderr, status] = await Promise.all([
    new Response(subprocess.stdout).text(),
    new Response(subprocess.stderr).text(),
    subprocess.exited,
  ]);
  if (status !== 0)
    throw new Error(`Movie probe failed: ${stderr.slice(0, 300)}`);
  const result = movieProbeSchema.parse(JSON.parse(stdout));
  if (
    !result.streams.some(
      (stream) =>
        stream.codec_type === "video" && !stream.disposition?.attached_pic,
    )
  )
    throw new Error("File has no playable video stream");
  if (!result.streams.some((stream) => stream.codec_type === "audio"))
    throw new Error("File has no audio stream");
  return result;
}
/** Claims establish identity/cut/source, never override probe-derived codec, size or track facts. */
export function factsFromMovieProbe(
  probe: MovieProbe,
  claims: Pick<MovieFacts, "identityVerified" | "cut" | "source" | "proper">,
): MovieFacts {
  const video = probe.streams.find(
    (s) => s.codec_type === "video" && !s.disposition?.attached_pic,
  );
  return {
    ...claims,
    width: video?.width ?? null,
    height: video?.height ?? null,
    videoCodec: video?.codec_name ?? null,
    dynamicRange:
      !video?.color_transfer ||
      ["unknown", "unspecified"].includes(video.color_transfer)
        ? null
        : ["smpte2084", "arib-std-b67"].includes(video.color_transfer)
          ? "hdr"
          : "sdr",
    audioLanguages: probe.streams
      .filter((s) => s.codec_type === "audio")
      .map((s) => s.tags?.language ?? "und"),
  };
}
