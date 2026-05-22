import { sanitizeLinkHref } from "./link";

describe("sanitizeLinkHref", () => {
  it("allows app relative links and supported absolute links", () => {
    expect(sanitizeLinkHref("/invocation/abc")).toBe("/invocation/abc");
    expect(sanitizeLinkHref("#execution")).toBe("#execution");
    expect(sanitizeLinkHref("?tag=ci")).toBe("?tag=ci");
    expect(sanitizeLinkHref("relative/path")).toBe("relative/path");
    expect(sanitizeLinkHref("blob:https://app.buildbuddy.io/12345678-1234-1234-1234-123456789abc")).toBe(
      "blob:https://app.buildbuddy.io/12345678-1234-1234-1234-123456789abc"
    );
    expect(sanitizeLinkHref(" https://github.com/buildbuddy-io/buildbuddy ")).toBe(
      "https://github.com/buildbuddy-io/buildbuddy"
    );
  });

  it("rejects hrefs outside the link policy", () => {
    expect(sanitizeLinkHref("mailto:hello@example.com")).toBeUndefined();
    expect(sanitizeLinkHref("ftp://example.com/file")).toBeUndefined();
    expect(sanitizeLinkHref("data:text/html,hi")).toBeUndefined();
    expect(sanitizeLinkHref("javascript:alert(1)")).toBeUndefined();
    expect(sanitizeLinkHref("java\nscript:alert(1)")).toBeUndefined();
    expect(sanitizeLinkHref("//example.com/path")).toBeUndefined();
    expect(sanitizeLinkHref("https://example.com/\npath")).toBeUndefined();
  });
});
