import path from "node:path";
import { rm } from "node:fs/promises";

const outdir = path.resolve(import.meta.dir, "../web/dist");
await rm(outdir, { recursive: true, force: true });
const result = await Bun.build({
  entrypoints: [path.resolve(import.meta.dir, "../web/index.html")],
  outdir,
  target: "browser",
  minify: true,
  splitting: true,
  publicPath: "/app/",
  naming: {
    entry: "[name].[ext]",
    chunk: "assets/[name]-[hash].[ext]",
    asset: "assets/[name]-[hash].[ext]",
  },
  define: { "process.env.NODE_ENV": JSON.stringify("production") },
});
if (!result.success)
  throw new AggregateError(result.logs, "Frontend build failed");
if (!(await Bun.file(path.join(outdir, "index.html")).exists()))
  throw new Error("Frontend index.html was not generated");
console.log(`Built ${result.outputs.length} frontend artifacts.`);
