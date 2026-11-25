import type { inspectMetainfo } from "../media/metainfo";

export const playableTvMember = (name: string) => /\.(mkv|mp4|m4v|avi|mov|ts)$/i.test(name);

/** TV imports exact metainfo members; archive extraction is not supported. */
export function tvMetainfoBlockers(metadata: Pick<ReturnType<typeof inspectMetainfo>, "files">): string[] {
  return metadata.files.some(file => file.size > 0 && playableTvMember(file.path)) ? [] : ["no-playable-video"];
}
