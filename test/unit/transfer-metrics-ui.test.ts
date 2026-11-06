import { expect, test } from "bun:test";
import { chartDomain, chartSegments, metricBytes, metricDays, seriesChange } from "../../web/src/transfer-metrics-model";

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

test("tracker lines share a numeric scale and zoom reveals small real changes without inventing growth", () => {
  const points = (values: (number | null)[]) => values.map((value, i) => ({ at: new Date(i * 60000).toISOString(), value }));
  const low = points([10, 20]), high = points([100, 200]);
  const domain = chartDomain([low, high], false);
  expect(domain).toEqual({ minimum: 0, maximum: 200 });
  expect(chartSegments(low, 60000, 100, 100, domain).paths).toEqual(["M0.00,95.00 L100.00,90.00"]);
  expect(chartSegments(high, 60000, 100, 100, domain).paths).toEqual(["M0.00,50.00 L100.00,0.00"]);
  const tinyGrowth = points([1e12, 1e12 + 100]);
  const zoomed = chartDomain([tinyGrowth], true);
  expect(zoomed).toEqual({ minimum: 1e12 - 10, maximum: 1e12 + 110 });
  expect(seriesChange(tinyGrowth)).toBe(100);
  expect(seriesChange(points([10, null, 10]))).toBe(0);
  expect(seriesChange(points([null]))).toBeNull();
  expect(chartDomain([points([100, 100])], true)).toEqual({ minimum: 0, maximum: 100 });
});

test("unavailable transfer values are never displayed as zero", () => {
  expect(metricBytes(null)).toBe("Unknown"); expect(metricBytes(0)).toBe("0 B");
  expect(metricBytes(1024)).toBe("1 KiB"); expect(metricDays(null)).toBe("Unknown");
  expect(metricDays(86400)).toBe("1 days");
});
