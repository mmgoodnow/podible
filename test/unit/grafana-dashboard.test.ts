import { expect, test } from "bun:test";

test("dashboard stacks comparable phases only and shows sample counts beside timing", () => {
  const result = Bun.spawnSync([process.execPath, "run", "scripts/grafana-dashboard.ts", "fixture-prometheus"]);
  expect(result.exitCode).toBe(0);
  const { panels } = JSON.parse(result.stdout.toString());
  const get = (title: string) => panels.find((panel: { title: string }) => panel.title === title);
  for (const [title, media] of [["Movies: download to Plex", "movie"], ["TV: download to Plex", "tv"]]) {
    const { fieldConfig, targets } = get(title);
    expect(fieldConfig.defaults.custom).toMatchObject({ drawStyle: "bars", spanNulls: false, stacking: { mode: "normal" } });
    const count = `sum(increase(podible_acquisition_latency_seconds_count{job="podible",phase="complete_to_ready",media="${media}"}[$__interval]))`;
    expect(targets[0].expr).toContain('phase="complete_to_publish"');
    expect(targets[1].expr).toContain('phase="complete_to_ready"');
    expect(targets[1].expr).not.toContain('phase="publish_to_ready"');
    for (const target of targets) {
      expect(target.expr).toContain(` / (${count} > 0)`);
      expect(target.interval).toBe("3h");
    }
  }
  expect(get("Downloads measured").targets[0].expr).not.toContain("> 0");
  expect(get("Library link updates (p95)").fieldConfig.defaults.custom)
    .toMatchObject({ drawStyle: "points", spanNulls: false });
  expect(get("Library link updates (p95)").fieldConfig.defaults.custom.stacking).toBeUndefined();
  expect(panels.some(({ title }: { title?: string }) => /finalization/i.test(title ?? ""))).toBeFalse();
});
