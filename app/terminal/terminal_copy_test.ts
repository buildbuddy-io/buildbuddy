import { copyTerminalText } from "./copy";

describe("copyTerminalText", () => {
  it("strips ANSI sequences before copying", () => {
    const copier = jasmine.createSpy("copier");
    copyTerminalText("\x1b[32mHello\x1b[0m\nWorld", copier);
    expect(copier).toHaveBeenCalledWith("Hello\nWorld");
  });

  it("falls back to empty string when log is undefined", () => {
    const copier = jasmine.createSpy("copier");
    copyTerminalText(undefined, copier);
    expect(copier).toHaveBeenCalledWith("");
  });
});
