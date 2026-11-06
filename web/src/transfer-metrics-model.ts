export type ChartPoint = { at: string; value: number | null };

// Break lines at missing observations and polling gaps instead of inventing activity.
export function chartSegments(points: ChartPoint[], cadenceMs: number, width = 800, height = 180) {
  const valid = points.filter(point => Number.isFinite(Date.parse(point.at)));
  if (!valid.length) return { paths: [] as string[], maximum: 0, start: null, end: null };
  const start = Date.parse(valid[0]!.at), end = Date.parse(valid.at(-1)!.at);
  const maximum = Math.max(0, ...valid.map(point => point.value ?? 0));
  const paths: string[] = [];
  let path = "", previous: number | null = null;
  for (const point of valid) {
    const at = Date.parse(point.at);
    if (point.value === null || !Number.isFinite(point.value)) {
      if (path) paths.push(path);
      path = ""; previous = null; continue;
    }
    if (previous !== null && at - previous > cadenceMs * 3) {
      if (path) paths.push(path);
      path = "";
    }
    const x = end === start ? width / 2 : (at - start) / (end - start) * width;
    const y = height - point.value / (maximum || 1) * height;
    path += `${path ? " L" : "M"}${x.toFixed(2)},${y.toFixed(2)}`;
    previous = at;
  }
  if (path) paths.push(path);
  return { paths, maximum, start, end };
}

export function metricBytes(value: number | null): string {
  if (value === null) return "Unknown";
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  const index = value > 0 ? Math.min(units.length - 1, Math.floor(Math.log(value) / Math.log(1024))) : 0;
  return `${(value / 1024 ** index).toLocaleString(undefined, { maximumFractionDigits: index ? 1 : 0 })} ${units[index]}`;
}

export function metricDays(seconds: number | null): string {
  if (seconds === null) return "Unknown";
  const [divisor, unit] = seconds < 3600 ? [60, "min"] : seconds < 86400 ? [3600, "h"] : [86400, "days"];
  return `${(seconds / divisor).toLocaleString(undefined, { maximumFractionDigits: 1 })} ${unit}`;
}
