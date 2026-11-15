/** Named columns are kept out of repeated JSON rows. Input must already be sanitized. */
export function evidenceRow<T extends object, K extends keyof T>(value: T, columns: readonly K[]): T[K][] {
  return columns.map(key => value[key]);
}

export function smallerAgentEvidence(original: unknown, compact: unknown, legend: string): boolean {
  return Buffer.byteLength(JSON.stringify(compact)) + Buffer.byteLength(legend) <
    Buffer.byteLength(JSON.stringify(original));
}

export function feedEvidenceTable<T extends object, R extends { attributes: { name: string; value: string }[] }>(
  targets: T[], releases: R[], targetColumns: readonly (keyof T)[], releaseColumns: readonly (keyof R)[],
) {
  const compact = {
    targets: targets.map(target => evidenceRow(target, targetColumns)),
    releases: releases.map(release => evidenceRow({
      ...release, attributes: release.attributes.map(({ name, value }) => [name, value]),
    }, releaseColumns)),
  };
  const legend = `Read targets as [${targetColumns.join(", ")}] and releases as [${releaseColumns.join(", ")}]. Attributes are [name, value] pairs.`;
  return smallerAgentEvidence({ targets, releases }, compact, legend) ? { compact, legend } : null;
}
