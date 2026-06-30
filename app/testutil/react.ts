import { cleanup, fireEvent, render, screen as testingLibraryScreen, waitFor, within } from "@testing-library/react";
import globalJsdom from "global-jsdom";

export { fireEvent, render, waitFor };

/**
 * Testing Library's global query helper.
 *
 * Tests can call screen.getByText(...) instead of threading around the return
 * value from render(...). The proxy delays binding to document.body until after
 * setup() installs the jsdom document.
 */
export const screen: typeof testingLibraryScreen = new Proxy({} as typeof testingLibraryScreen, {
  get(_target, property: keyof typeof testingLibraryScreen) {
    const queries = within(document.body) as Partial<typeof testingLibraryScreen>;
    return queries[property] ?? testingLibraryScreen[property];
  },
});

/**
 * HTML common to all tests using this library.
 *
 * Note that <div id="app"> matches the production app root element.
 */
const TEST_HTML = '<!doctype html><html><body><div id="app"></div></body></html>';
const TEST_URL = "https://app.buildbuddy.test/";

/** Sets up DOM globals and Testing Library cleanup for a React Jasmine suite. */
export function reactSetup() {
  let cleanupGlobals: () => void;

  beforeAll(() => {
    // Install jsdom-backed browser globals once for this suite.
    cleanupGlobals = globalJsdom(TEST_HTML, { url: TEST_URL });
  });

  afterEach(() => {
    // Unmount React trees and clear Testing Library state between specs.
    cleanup();
  });

  afterAll(() => {
    // Remove globals installed by global-jsdom after the suite finishes.
    cleanupGlobals();
  });
}
