import { Completion, apply, ghostFor, tokenAt } from "./completion";

const c = (text: string, extra: Partial<Completion> = {}): Completion => ({ text, ...extra });

describe("tokenAt", () => {
  it("finds the token around the caret and the part before it", () => {
    expect(tokenAt("web ki", 6)).toEqual({ token: { start: 4, end: 6 }, typed: "ki" });
    expect(tokenAt("kind:pod web", 3)).toEqual({ token: { start: 0, end: 8 }, typed: "kin" });
  });
  it("is empty after a space or in an empty value", () => {
    expect(tokenAt("web ", 4)).toEqual({ token: { start: 4, end: 4 }, typed: "" });
    expect(tokenAt("", 0)).toEqual({ token: { start: 0, end: 0 }, typed: "" });
  });
});

describe("apply", () => {
  it("inserts a key as is, with the caret at its end", () => {
    expect(apply("web ki", tokenAt("web ki", 6).token, c("kind:"))).toEqual({ value: "web kind:", caret: 9 });
  });
  it("follows a finished value with a space", () => {
    expect(apply("web kind:de", tokenAt("web kind:de", 11).token, c("kind:deployment"))).toEqual({
      value: "web kind:deployment ",
      caret: 20,
    });
  });
  it("reuses a space already there", () => {
    expect(apply("kind:po ns:prod", tokenAt("kind:po ns:prod", 7).token, c("kind:pod"))).toEqual({
      value: "kind:pod ns:prod",
      caret: 9,
    });
  });
  it("leaves a partial completion open", () => {
    expect(apply("label:ap", tokenAt("label:ap", 8).token, c("label:app", { partial: true }))).toEqual({
      value: "label:app",
      caret: 9,
    });
  });
  it("replaces the whole token", () => {
    expect(apply("kin web", tokenAt("kin web", 3).token, c("kind:"))).toEqual({ value: "kind: web", caret: 5 });
  });
});

describe("ghostFor", () => {
  it("is what the first extending completion adds, matched case-insensitively", () => {
    expect(ghostFor("ki", [c("kind:")])).toEqual({ completion: c("kind:"), rest: "nd:" });
    expect(ghostFor("kind:P", [c("kind:pod"), c("kind:podtemplate")])).toEqual({
      completion: c("kind:pod"),
      rest: "od",
    });
  });
  it("offers a whole key's first value", () => {
    const completions = [c("label:app=web"), c("label:app.kubernetes.io/name", { partial: true })];
    expect(ghostFor("label:app", completions)?.rest).toEqual("=web");
  });
  it("is nothing when nothing extends what was typed", () => {
    expect(ghostFor("kind:pod", [c("kind:pod")])).toBeUndefined();
    expect(ghostFor("", [c("kind:")])).toBeUndefined();
    expect(ghostFor("ns:x", [c("ns:prod")])).toBeUndefined();
  });
});
