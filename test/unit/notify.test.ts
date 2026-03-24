import { describe, expect, test } from "bun:test";

import { sendPushoverNotification } from "../../src/books/notify";
import { defaultSettings } from "../../src/books/settings";

describe("pushover notifications", () => {
  test("times out hung requests", async () => {
    const settings = defaultSettings({
      notifications: {
        pushover: {
          enabled: true,
          apiToken: "token",
          userKey: "user",
        },
      },
    });

    const originalFetch = globalThis.fetch;
    globalThis.fetch = ((async (_input: unknown, init?: RequestInit) => {
      const signal = init?.signal;
      return await new Promise<Response>((_resolve, reject) => {
        if (!signal) {
          reject(new Error("missing abort signal"));
          return;
        }
        signal.addEventListener(
          "abort",
          () => reject(new Error("aborted")),
          { once: true }
        );
      });
    }) as unknown) as typeof fetch;

    try {
      await expect(
        sendPushoverNotification(settings, {
          title: "Podible recovery failed",
          message: "Auto-acquire found no usable release",
        })
      ).rejects.toThrow("Pushover request timed out after 3000ms");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});
