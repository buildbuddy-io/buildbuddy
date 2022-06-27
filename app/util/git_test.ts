import { normalizeRepoURL } from "./git";

describe("normalize repo URL", () => {
  it("should normalize as expected", () => {
    expect(normalizeRepoURL("file:///path/to/repo")).toBe("file:///path/to/repo");
    expect(normalizeRepoURL("http://localhost:8080/local/repo.git")).toBe("http://localhost:8080/local/repo");
    expect(normalizeRepoURL("  https://github.com/foo/bar  ")).toBe("https://github.com/foo/bar");
    expect(normalizeRepoURL("ssh://git@github.com:foo/bar.git")).toBe("https://github.com/foo/bar");
    expect(normalizeRepoURL("git@github.com:foo/bar")).toBe("https://github.com/foo/bar");
    expect(normalizeRepoURL("http://github.com/foo/bar")).toBe("https://github.com/foo/bar");
  });
});
