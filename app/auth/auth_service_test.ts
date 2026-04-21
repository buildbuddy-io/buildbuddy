declare function require(path: string): any;

class FakeStorage {
  private values = new Map<string, string>();

  get length() {
    return this.values.size;
  }

  clear() {
    this.values.clear();
  }

  getItem(key: string) {
    return this.values.get(key) ?? null;
  }

  key(index: number) {
    return Array.from(this.values.keys())[index] ?? null;
  }

  removeItem(key: string) {
    this.values.delete(key);
  }

  setItem(key: string, value: string) {
    this.values.set(key, value);
  }
}

const testGlobal = globalThis as typeof globalThis & {
  localStorage: FakeStorage;
  sessionStorage: FakeStorage;
  window: {
    buildbuddyConfig: Record<string, unknown>;
    location: {
      href: string;
      origin: string;
      host: string;
      pathname: string;
      search: string;
      hash: string;
    };
    opener?: { postMessage: jasmine.Spy };
  };
};

testGlobal.window = {
  buildbuddyConfig: { disableOidcLogin: true },
  location: {
    href: "https://app.example.com/invocation/123",
    origin: "https://app.example.com",
    host: "app.example.com",
    pathname: "/invocation/123",
    search: "",
    hash: "",
  } as Location & {
    href: string;
    origin: string;
    host: string;
    pathname: string;
    search: string;
    hash: string;
  },
};
testGlobal.localStorage = new FakeStorage() as Storage & FakeStorage;
testGlobal.sessionStorage = new FakeStorage() as Storage & FakeStorage;

const AuthService = require("./auth_service").AuthService as typeof import("./auth_service").AuthService;

describe("AuthService.handleTokenRefreshError", () => {
  beforeEach(() => {
    testGlobal.localStorage.clear();
    testGlobal.window.location.href = "https://app.example.com/invocation/123";
  });

  it("uses the SPA login page when OIDC login is disabled", () => {
    const authService = new AuthService();
    authService.handleTokenRefreshError();

    expect(testGlobal.localStorage.getItem("auto_login_attempted")).toBe("true");
    expect(testGlobal.window.location.href).toBe(
      `/?${new URLSearchParams({
        redirect_url: "https://app.example.com/invocation/123",
      })}`
    );
  });
});
