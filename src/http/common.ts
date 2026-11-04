import { safeReturnPath } from "../redirects";

/** Escaping for document/feed assets; application screens are rendered by the SPA. */
export function escapeHtml(value: string): string {
  return value.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

export function addApiKey(path: string, _apiKey: string | null): string { return path; }
export const sanitizeRedirectPath = safeReturnPath;
