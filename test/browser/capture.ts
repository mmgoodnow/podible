import type { Page } from "playwright";

type CapturePage = Pick<Page, "bringToFront" | "screenshot" | "waitForTimeout">;

/** Chromium can reject a capture before its compositor has a frame ready. */
export async function capturePage(page: CapturePage, path: string) {
  for (let attempt = 0; attempt < 3; attempt++) {
    await page.bringToFront();
    try {
      return await page.screenshot({ path, animations: "disabled" });
    } catch (error) {
      if (attempt === 2 || !(error instanceof Error) ||
          !error.message.includes("Protocol error (Page.captureScreenshot): Unable to capture screenshot")) throw error;
      await page.waitForTimeout(100);
    }
  }
  throw new Error("Screenshot capture attempts exhausted");
}
