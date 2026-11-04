import { expect, test } from "bun:test";
import { capturePage } from "../browser/capture";

const transient = () => new Error("screenshot: Protocol error (Page.captureScreenshot): Unable to capture screenshot");
function fixture(failures: Error[]) {
  let calls = 0, waits = 0;
  return {
    page: {
      async bringToFront() {},
      async screenshot() { const error = failures[calls++]; if (error) throw error; return Buffer.from("frame"); },
      async waitForTimeout() { waits++; },
    },
    state: () => ({ calls, waits }),
  };
}
test("capture retries only transient compositor failures and returns a real frame", async () => {
  const f = fixture([transient(), transient()]);
  expect((await capturePage(f.page, "unused.png")).toString()).toBe("frame");
  expect(f.state()).toEqual({ calls: 3, waits: 2 });
});
test("persistent capture errors still fail after bounded retries", async () => {
  const f = fixture([transient(), transient(), transient()]);
  await expect(capturePage(f.page, "unused.png")).rejects.toThrow("Unable to capture screenshot");
  expect(f.state()).toEqual({ calls: 3, waits: 2 });
});
test("navigation or closed-target failures are not hidden by screenshot retries", async () => {
  const f = fixture([new Error("Target closed")]);
  await expect(capturePage(f.page, "unused.png")).rejects.toThrow("Target closed");
  expect(f.state()).toEqual({ calls: 1, waits: 0 });
});
