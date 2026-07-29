import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";

async function keepDemoFixture(page: Page) {
  await page.addInitScript(() => {
    class OfflineWebSocket extends EventTarget {
      static readonly CLOSED = 3;
      readonly readyState = OfflineWebSocket.CLOSED;
      close() {}
    }
    Object.defineProperty(window, "WebSocket", {
      configurable: true,
      value: OfflineWebSocket,
    });
  });
  await page.route("http://127.0.0.1:8787/**", (route) => route.abort());
}

test("desktop renders forecast contract and has no serious accessibility violations", async ({
  page,
}) => {
  await keepDemoFixture(page);
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto("/");

  await expect(page.getByText("DEMO DATA", { exact: true })).toBeVisible();
  await expect(page.getByText("FORECAST DEMO", { exact: true })).toBeVisible();
  await expect(page.getByText("50% interval", { exact: true })).toBeVisible();
  await expect(page.getByText("TP 3333.66", { exact: true })).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Setuju tidak entry" }),
  ).toBeDisabled();

  const accessibility = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa"])
    .analyze();
  expect(
    accessibility.violations.filter((violation) =>
      ["serious", "critical"].includes(violation.impact ?? ""),
    ),
  ).toEqual([]);
  await expect(page).toHaveScreenshot("xpde-desktop.png", {
    animations: "disabled",
    caret: "hide",
    fullPage: true,
    maxDiffPixelRatio: 0.02,
  });
});

test("mobile keeps decision proposal before the forecast chart", async ({ page }) => {
  await keepDemoFixture(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");

  const decision = page
    .locator(".mobile-decision")
    .getByText("Decision proposal", { exact: true });
  const chart = page.getByText("Market + forecast envelope", { exact: true });
  await expect(decision).toBeVisible();
  await expect(chart).toBeVisible();
  const decisionBox = await decision.boundingBox();
  const chartBox = await chart.boundingBox();
  expect(decisionBox).not.toBeNull();
  expect(chartBox).not.toBeNull();
  expect(decisionBox!.y).toBeLessThan(chartBox!.y);
  await expect(page).toHaveScreenshot("xpde-mobile.png", {
    animations: "disabled",
    caret: "hide",
    fullPage: true,
    maxDiffPixelRatio: 0.02,
  });
});
