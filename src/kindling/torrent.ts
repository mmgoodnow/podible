export function normalizeInfoHash(value: string): string {
  const trimmed = value.trim();
  if (!/^[0-9a-fA-F]{40}$/.test(trimmed)) {
    throw new Error("Unsupported info hash format");
  }
  return trimmed.toLowerCase();
}
