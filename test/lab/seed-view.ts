import { mkdir } from "node:fs/promises";
import {
  initializeManagedContent,
  reconcileLibraryView,
} from "../../src/media/library-view";

const contentRoot = "/data/content";
const viewRoot = "/data/views/plex/movies";
await initializeManagedContent(contentRoot);
await mkdir(`${contentRoot}/payload-fixture`, { recursive: true });
const contentPath = `${contentRoot}/payload-fixture/original.mp4`;
if (!(await Bun.file(contentPath).exists()))
  await Bun.write(contentPath, Bun.file("/fixture.mp4"));
const relativePath =
  process.argv[2] === "rename"
    ? "Lab Movie (2020) [local-42]/Lab Movie (2020).mp4"
    : "Lab Movie (2020)/Lab Movie (2020).mp4";
console.log(
  JSON.stringify(
    await reconcileLibraryView({
      contentRoot,
      viewRoot,
      entries: [{ relativePath, contentPath }],
    }),
  ),
);
