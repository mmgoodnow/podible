import { expect, test } from "bun:test";
import { downloadCapacityProblem } from "../../src/media/download-capacity";

test("capacity includes both canonical bytes and sidecars on the same volume", async () => {
  const files = [{ path: "movie.mkv", size: 80 * 1024 ** 2 }, { path: "sidecar.nfo", size: 4 * 1024 ** 2 }];
  const input = { sourceRoot: "/downloads", contentRoot: "/objects", canonical: true, files };
  const volume = async () => ({ device: 1n, available: 146n * 1024n ** 2n });
  expect(await downloadCapacityProblem(input, volume)).toContain("Not enough free disk space");
  expect(await downloadCapacityProblem(input, async root => ({ ...await volume(), device: root === "/objects" ? 1n : 2n }))).toBeNull();
});

test("noncanonical downloads only need space on the download volume at admission", async () => {
  const touched: string[] = [];
  const result = await downloadCapacityProblem({ sourceRoot: "/downloads", contentRoot: "/objects",
    canonical: false, files: [{ path: "film.mkv", size: 1024 }] }, async root => {
    touched.push(root);
    return { device: 1n, available: 1024n ** 3n };
  });
  expect(result).toBeNull();
  expect(touched).toEqual(["/downloads"]);
});
