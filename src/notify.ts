import type { AppSettings } from "./app-types";

type NotificationRequest = {
  title: string;
  message: string;
};

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

  const pushover = settings.notifications.pushover;
  const body = new URLSearchParams({
    token: pushover.apiToken.trim(),
    user: pushover.userKey.trim(),
    title: trimTo(request.title, 250),
    message: trimTo(request.message, 1024),
  });

  const signal = AbortSignal.timeout(PUSHOVER_TIMEOUT_MS);

  let response: Response;
  try {
    response = await fetch("https://api.pushover.net/1/messages.json", {
      method: "POST",
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

  if (!response.ok) {
    throw new Error(`Pushover returned ${response.status}`);
  }

  return { delivered: true };
}
