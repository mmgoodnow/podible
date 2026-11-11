import { postPushoverMessage, type NotificationFetch } from "../notify";
import { emailTarget, pushoverTarget, type DeliveryClaim, type DeliveryOutcome, type NotificationOutbox } from "./outbox";
import { recipientAddresses } from "./settings";

const backoffMs = [30_000, 120_000, 600_000, 1_800_000, 7_200_000];

function retry(claim: DeliveryClaim, now: number, reason: string, response?: Response): DeliveryOutcome {
  const header = response?.headers.get("Retry-After");
  const retryAt = header && /^\d+$/.test(header) ? now + Number(header) * 1000 : Date.parse(header ?? "");
  const reset = response?.headers.get("X-Limit-App-Remaining") === "0"
    ? Number(response.headers.get("X-Limit-App-Reset")) * 1000 : 0;
  return { state: "retry", reason, at: Math.max(now + (backoffMs[claim.delivery.attempts - 1] ?? backoffMs.at(-1)!), retryAt || 0, reset || 0) };
}

async function deliver(outbox: NotificationOutbox, claim: DeliveryClaim, fetcher: NotificationFetch, now: () => number): Promise<DeliveryOutcome> {
  const settings = outbox.settings(), { delivery } = claim;
  if (settings.delivery !== "live") return { state: "skipped", reason: "dry_run" };

  if (delivery.channel === "pushover") {
    if (pushoverTarget(settings)?.binding !== delivery.binding) return { state: "skipped", reason: "destination_changed" };
    try {
      const response = await postPushoverMessage(settings.pushover, delivery.message, fetcher);
      if (response.status === 429) return retry(claim, now(), "http_429", response);
      if (response.status >= 400 && response.status < 500 && response.status !== 408)
        return { state: "failed", reason: `http_${response.status}` };
      if (response.ok) {
        const payload = await response.json() as { status?: unknown } | null;
        if (payload?.status === 1) return { state: "accepted" };
        if (payload?.status === 0) return { state: "failed", reason: "rejected" };
      }
    } catch { /* No client idempotency key; a lost reply might follow an accepted send. */ }
    return { state: "unknown", reason: "unconfirmed_push" };
  }

  const to = recipientAddresses(settings.email.recipients)?.get(delivery.userId);
  const target = emailTarget(settings, to);
  if (!target || target.binding !== delivery.binding || !outbox.hasUser(delivery.userId))
    return { state: "skipped", reason: "destination_changed" };
  try {
    const response = await fetcher("https://api.resend.com/emails", {
      method: "POST",
      redirect: "error",
      headers: { Authorization: `Bearer ${target.apiKey}`, "Content-Type": "application/json", "Idempotency-Key": delivery.idempotencyKey },
      body: JSON.stringify({ from: target.from, to: [target.to], subject: delivery.message.title, text: delivery.message.message }),
      signal: AbortSignal.timeout(3_000),
    });
    if (response.ok) {
      const payload = await response.json() as { id?: unknown } | null;
      return typeof payload?.id === "string" && payload.id ? { state: "accepted" } : retry(claim, now(), "invalid_response");
    }
    if (response.status === 409) {
      const payload = await response.json().catch(() => null) as { name?: string } | null;
      if (payload?.name === "concurrent_idempotent_requests") return retry(claim, now(), "concurrent_request", response);
    }
    if (response.status === 408 || response.status === 429 || response.status >= 500)
      return retry(claim, now(), `http_${response.status}`, response);
    return { state: "failed", reason: `http_${response.status}` };
  } catch {
    return retry(claim, now(), "unconfirmed_email");
  }
}

export async function deliverNextNotification(outbox: NotificationOutbox, { fetcher = fetch, now = Date.now }: {
  fetcher?: NotificationFetch; now?: () => number;
} = {}) {
  const claim = outbox.claim(now());
  if (!claim) return null;
  const outcome = await deliver(outbox, claim, fetcher, now);
  return outbox.settle(claim, outcome, now());
}

export function startNotificationDelivery(outbox: NotificationOutbox, log: (message: string) => void) {
  let active: Promise<void> | undefined;
  const timer = setInterval(() => {
    if (active) return;
    active = deliverNextNotification(outbox).then(outcome => {
      if (outcome && (outcome.state === "failed" || outcome.state === "unknown"))
        log(`[notifications] delivery ${outcome.state}: ${outcome.reason}`);
    }).catch(() => log("[notifications] could not reconcile delivery outbox"))
      .finally(() => { active = undefined; });
  }, 1_000);
  return async () => {
    clearInterval(timer);
    await active;
  };
}
