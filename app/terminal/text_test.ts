import { getContent, normalizeSpace } from "./text";

describe("normalizeSpace", () => {
  it("should handle a single tabstop in a single line of plaintext", () => {
    expect(normalizeSpace("\t1234")).toEqual("        1234");
    expect(normalizeSpace("1234\t")).toEqual("1234    ");
    expect(normalizeSpace("1234\t1234")).toEqual("1234    1234");
    expect(normalizeSpace("1234567\t1234")).toEqual("1234567 1234");
    expect(normalizeSpace("12345678\t1234")).toEqual("12345678        1234");
  });

  it("should handle multiple tabstops in a single line of plaintext", () => {
    expect(normalizeSpace("1234\t\t1234")).toEqual("1234            1234");
    expect(normalizeSpace("\t1234\t1234\t")).toEqual("        1234    1234    ");
  });

  it("should handle multiple tabstops across multiple lines of plaintext", () => {
    expect(normalizeSpace("1234567\t123\nab\tcdefg")).toEqual("1234567 123\nab      cdefg");
    expect(normalizeSpace("\n\t1234567\t123\nab\tcdefg")).toEqual("\n        1234567 123\nab      cdefg");
  });

  it("should handle tabstops in lines containing ANSI codes", () => {
    expect(normalizeSpace("\x1b[33m\t12345\x1b[0m\t12345")).toEqual("\x1b[33m        12345\x1b[0m   12345");
  });
});

describe("getContent", () => {
  it("should handle ANSI SGR state that persists across lines", () => {
    expect(getContent("\x1b[32mMulti-line\nColor\x1b[m\nReset", "", 0).rows).toEqual([
      {
        plaintext: "Multi-line",
        matchStartIndex: null,
        wrapOffset: 0,
        tags: [
          {
            length: 10,
            style: { foreground: "green" },
          },
        ],
      },
      {
        plaintext: "Color",
        matchStartIndex: null,
        wrapOffset: 0,
        tags: [
          {
            length: 5,
            style: { foreground: "green" },
          },
        ],
      },
      {
        plaintext: "Reset",
        matchStartIndex: null,
        wrapOffset: 0,
        tags: [
          {
            length: 5,
            style: {},
          },
        ],
      },
    ]);

    expect(getContent("\x1b[32mMulti-line\n\x1b[32mColor\x1b[m\nReset", "", 0).rows).toEqual([
      {
        plaintext: "Multi-line",
        matchStartIndex: null,
        wrapOffset: 0,
        tags: [
          {
            length: 10,
            style: { foreground: "green" },
          },
        ],
      },
      {
        plaintext: "Color",
        matchStartIndex: null,
        wrapOffset: 0,
        tags: [
          {
            length: 5,
            style: { foreground: "green" },
          },
        ],
      },
      {
        plaintext: "Reset",
        matchStartIndex: null,
        wrapOffset: 0,
        tags: [
          {
            length: 5,
            style: {},
          },
        ],
      },
    ]);
  });
});
