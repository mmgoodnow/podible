import { z } from "zod";
import type { AppSettings } from "../app-types";

const address = z.email();

export function recipientAddresses(value: string): Map<number, string> | null {
  const addresses = new Map<number, string>();
  for (const line of value.split(/\r?\n/)) {
    if (!line.trim()) continue;
    const match = /^([1-9]\d*)\s*=\s*(\S+)$/.exec(line.trim());
    const id = Number(match?.[1]), email = match?.[2];
    if (!Number.isSafeInteger(id) || !email || !address.safeParse(email).success || addresses.has(id)) return null;
    addresses.set(id, email);
  }
  return addresses;
}

export const emailSettingsSchema = z.object({
  apiKey: z.string(),
  from: z.string().refine(value => !value.trim() || address.safeParse(value.trim()).success, "Enter a sender email address."),
  recipients: z.string().refine(value => recipientAddresses(value) !== null, "Use one unique Podible user ID = email address per line."),
});

export function parseNotificationSettings(value: unknown): AppSettings["notifications"] {
  const input = (value && typeof value === "object" ? value : {}) as Partial<AppSettings["notifications"]>;
  const email = emailSettingsSchema.safeParse(input.email);
  return {
    delivery: input.delivery === "live" ? "live" : "dry-run",
    email: email.success ? email.data : { apiKey: "", from: "", recipients: "" },
    pushover: {
      enabled: input.pushover?.enabled === true,
      lifecycleEnabled: input.pushover?.lifecycleEnabled === true,
      apiToken: typeof input.pushover?.apiToken === "string" ? input.pushover.apiToken : "",
      userKey: typeof input.pushover?.userKey === "string" ? input.pushover.userKey : "",
    },
  };
}
