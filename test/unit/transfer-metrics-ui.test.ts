import { expect, test } from "bun:test";
import { chartSegments, metricBytes, metricDays } from "../../web/src/transfer-metrics-model";

test("history uses elapsed time, keeps real zero samples, and breaks at unavailable readings and outages", () => {
  const at = (minutes: number) => new Date(Date.UTC(2026, 10, 6, 0, minutes)).toISOString();
  const result = chartSegments([
    { at: at(0), value: 100 }, { at: at(1), value: 0 }, { at: at(2), value: null },
    { at: at(3), value: 50 }, { at: at(20), value: 100 }, { at: at(21), value: 50 },
  ], 60_000, 210, 100);
  expect(result.maximum).toBe(100);
  expect(result.paths).toEqual(["M0.00,0.00 L10.00,100.00", "M30.00,50.00", "M200.00,0.00 L210.00,50.00"]);
  expect(chartSegments([], 60_000).paths).toEqual([]);
  expect(chartSegments([{ at: at(0), value: null }], 60_000).paths).toEqual([]);
  expect(chartSegments([{ at: at(0), value: 0 }], 60_000).paths).toEqual(["M400.00,180.00"]);
});

test("unavailable transfer values are never displayed as zero", () => {
  expect(metricBytes(null)).toBe("Unknown"); expect(metricBytes(0)).toBe("0 B");
  expect(metricBytes(1024)).toBe("1 KiB"); expect(metricDays(null)).toBe("Unknown");
  expect(metricDays(86400)).toBe("1 days");
});
