const { test, expect } = require("@playwright/test");

test("shows quickstart page when no invocations exist", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("Quickstart")).toBeVisible();
});

test("shows invocation not found message for unknown invocation", async ({ page }) => {
  await page.goto("/invocation/does-not-exist");
  await expect(page.getByText("Invocation not found!")).toBeVisible();
});
