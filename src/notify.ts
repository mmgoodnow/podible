import type { AppSettings } from "./app-types";

type NotificationRequest = {
  title: string;
  message: string;
};
export type NotificationFetch = (url: string, init: RequestInit) => Promise<Response>;

const PUSHOVER_TIMEOUT_MS = 3_000;

function trimTo(value: string, max: number): string {
  const normalized = value.trim();
  if (normalized.length <= max) return normalized;
  return `${normalized.slice(0, Math.max(0, max - 1)).trimEnd()}…`;
}

export function canSendPushover(settings: AppSettings): boolean {
  const pushover = settings.notifications.pushover;
  return pushover.enabled && pushover.apiToken.trim().length > 0 && pushover.userKey.trim().length > 0;
}

export async function sendPushoverNotification(
  settings: AppSettings,
  request: NotificationRequest
): Promise<{ delivered: boolean; reason?: string }> {
  if (!canSendPushover(settings)) {
    return { delivered: false, reason: "pushover_disabled" };
  }
  if (settings.notifications.delivery !== "live") {
    return { delivered: false, reason: "dry_run" };
  }

  const response = await postPushoverMessage(settings.notifications.pushover, request);
  if (!response.ok) {
    throw new Error(`Pushover returned ${response.status}`);
  }
  return { delivered: true };
}

export async function postPushoverMessage(
  pushover: AppSettings["notifications"]["pushover"],
  request: NotificationRequest,
  fetcher: NotificationFetch = fetch,
): Promise<Response> {
  const body = new URLSearchParams({
    token: pushover.apiToken.trim(),
    user: pushover.userKey.trim(),
    title: trimTo(request.title, 250),
    message: trimTo(request.message, 1024),
  });

  const signal = AbortSignal.timeout(PUSHOVER_TIMEOUT_MS);

  try {
    return await fetcher("https://api.pushover.net/1/messages.json", {
      method: "POST",
      redirect: "error",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body,
      signal,
    });
  } catch (error) {
    if (signal.aborted) {
      throw new Error(`Pushover request timed out after ${PUSHOVER_TIMEOUT_MS}ms`);
    }
    throw error;
  }
}
