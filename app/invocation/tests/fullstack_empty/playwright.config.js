const { defineConfig } = require("@playwright/test");
const path = require("path");
const { HTTP_PORT, HEALTH_URL } = require("./test_environment");

const startScript = path.join(__dirname, "start_buildbuddy.js");

module.exports = defineConfig({
  testDir: __dirname,
  retries: 0,
  use: {
    baseURL: `http://127.0.0.1:${HTTP_PORT}`,
    headless: true,
    trace: "retain-on-failure",
  },
  webServer: {
    command: `node ${startScript}`,
    url: HEALTH_URL,
    stdout: "pipe",
    stderr: "pipe",
    reuseExistingServer: false,
    timeout: 120 * 1000,
  },
});
