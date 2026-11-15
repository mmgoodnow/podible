// Decode the actual legend sent to the model so tests check both row order and column names.
export function readFeedEvidence<T>(text: string, instructions: string | undefined): T {
  const input = JSON.parse(text);
  if (!Array.isArray(input.targets[0]) && !Array.isArray(input.releases[0])) return input;
  const columns = /Read targets as \[([^\]]+)\] and releases as \[([^\]]+)\]/.exec(instructions ?? "");
  if (!columns) throw new Error("Missing feed table legend");
  const decode = (rows: unknown[][], columnList: string) => {
    const keys = columnList.split(", ");
    return rows.map(row => {
      if (row.length !== keys.length) throw new Error("Invalid feed row width");
      return Object.fromEntries(keys.map((key, index) => [key, row[index]]));
    });
  };
  return {
    targets: decode(input.targets, columns[1]!),
    releases: decode(input.releases, columns[2]!).map(({ attributes, ...release }) => ({
      ...release,
      attributes: (attributes as [string, string][]).map(([name, value]) => ({ name, value })),
    })),
  } as T;
}
