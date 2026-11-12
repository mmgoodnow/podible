import nodemailer from "nodemailer";
import type { emailTarget } from "./outbox";

export type SmtpTarget = NonNullable<ReturnType<typeof emailTarget>>;
type SmtpMessage = { id: string; subject: string; text: string };
export type SendSmtp = (target: SmtpTarget, message: SmtpMessage) => Promise<boolean>;

export function smtpTransport(target: Pick<SmtpTarget, "host" | "port" | "security" | "username" | "password">) {
  return nodemailer.createTransport({
    host: target.host,
    port: target.port,
    secure: target.security === "tls",
    requireTLS: target.security === "starttls",
    auth: target.username ? { user: target.username, pass: target.password } : undefined,
    connectionTimeout: 5_000,
    greetingTimeout: 5_000,
    socketTimeout: 10_000,
    dnsTimeout: 5_000,
    disableFileAccess: true,
    disableUrlAccess: true,
  });
}

export const sendSmtp: SendSmtp = async (target, message) => {
  const result = await smtpTransport(target).sendMail({
    from: target.from, to: target.to, subject: message.subject, text: message.text,
    messageId: `<podible.${message.id}@${target.from.split("@").at(-1)}>`,
  });
  return result.accepted.length === 1 && result.rejected.length === 0;
};
