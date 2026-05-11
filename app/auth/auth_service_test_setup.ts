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

export const testGlobal = globalThis as unknown as {
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
    opener?: { postMessage: unknown };
  };
};

testGlobal.window = {
  buildbuddyConfig: { authEnabled: true, configuredIssuers: [] },
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
