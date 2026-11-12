import { postPushoverMessage, type NotificationFetch } from "../notify";
import { emailTarget, pushoverTarget, type DeliveryClaim, type DeliveryOutcome, type NotificationOutbox } from "./outbox";
import { recipientAddresses } from "./settings";
import { sendSmtp, type SendSmtp } from "./smtp";

const backoffMs = [30_000, 120_000, 600_000, 1_800_000, 7_200_000];

function retry(claim: DeliveryClaim, now: number, reason: string, response?: Response): DeliveryOutcome {
  const header = response?.headers.get("Retry-After");
  const retryAt = header && /^\d+$/.test(header) ? now + Number(header) * 1000 : Date.parse(header ?? "");
  const reset = response?.headers.get("X-Limit-App-Remaining") === "0"
    ? Number(response.headers.get("X-Limit-App-Reset")) * 1000 : 0;
  return { state: "retry", reason, at: Math.max(now + (backoffMs[claim.delivery.attempts - 1] ?? backoffMs.at(-1)!), retryAt || 0, reset || 0) };
}

async function deliver(outbox: NotificationOutbox, claim: DeliveryClaim, fetcher: NotificationFetch, smtp: SendSmtp, now: () => number): Promise<DeliveryOutcome> {
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
    const accepted = await smtp(target, { id: claim.id, subject: delivery.message.title, text: delivery.message.message });
    if (accepted) return { state: "accepted" };
  } catch (error) {
    const failure = error as { responseCode?: number; command?: string; code?: string } | null;
    const code = failure?.responseCode;
    // Retry only definite pre-delivery failures or explicit SMTP rejection, never a lost DATA acknowledgement.
    if (failure?.code === "EDNS" || failure?.command === "CONN" && code === undefined)
      return retry(claim, now(), "smtp_connection");
    if (typeof code === "number" && Number.isInteger(code) &&
        /^(?:CONN|EHLO|HELO|STARTTLS|AUTH\b.*|MAIL FROM|RCPT TO|DATA)$/.test(failure?.command ?? "")) {
      if (code >= 400 && code < 500) return retry(claim, now(), `smtp_${code}`);
      if (code >= 500 && code < 600) return { state: "failed", reason: `smtp_${code}` };
    }
  }
  return { state: "unknown", reason: "unconfirmed_email" };
}

export async function deliverNextNotification(outbox: NotificationOutbox, { fetcher = fetch, smtp = sendSmtp, now = Date.now }: {
  fetcher?: NotificationFetch; smtp?: SendSmtp; now?: () => number;
} = {}) {
  const claim = outbox.claim(now());
  if (!claim) return null;
  const outcome = await deliver(outbox, claim, fetcher, smtp, now);
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
