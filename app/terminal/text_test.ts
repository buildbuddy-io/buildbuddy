import { normalizeSpace } from "./text";

describe("normalizeSpace", () => {
  it("should apply tab stops", () => {
    // Single tabstop
    expect(normalizeSpace("\t1234")).toEqual("        1234");
    expect(normalizeSpace("1234\t")).toEqual("1234    ");
    expect(normalizeSpace("1234\t1234")).toEqual("1234    1234");
    expect(normalizeSpace("1234567\t1234")).toEqual("1234567 1234");
    expect(normalizeSpace("12345678\t1234")).toEqual("12345678        1234");

    // Multiple tabstops
    expect(normalizeSpace("1234\t\t1234")).toEqual("1234            1234");
    expect(normalizeSpace("\t1234\t1234\t")).toEqual("        1234    1234    ");

    // Multiple tabstops with newlines
    expect(normalizeSpace("1234567\t123\nab\tcdefg")).toEqual("1234567 123\nab      cdefg");
    expect(normalizeSpace("\n\t1234567\t123\nab\tcdefg")).toEqual("\n        1234567 123\nab      cdefg");
  });
});
