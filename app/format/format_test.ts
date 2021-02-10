import Long from "long";
import format from "./format";

describe("percent", () => {
  it("should handle zero", () => {
    expect(format.percent(0)).toEqual("0");
  });
  it("should handle fractions", () => {
    expect(format.percent(0.5)).toEqual("50");
  });
  it("should truncate", () => {
    expect(format.percent(0.3333333)).toEqual("33");
  });
  it("should handle one hundred", () => {
    expect(format.percent(1)).toEqual("100");
  });
  it("should handle over 100 percent", () => {
    expect(format.percent(5)).toEqual("500");
  });
  it("should handle longs", () => {
    expect(format.percent(new Long(5, 0))).toEqual("500");
  });
});

describe("sentenceCase", () => {
  it("should handle empty string", () => {
    expect(format.sentenceCase("")).toEqual("");
  });
  it("should handle lowercase", () => {
    expect(format.sentenceCase("foo")).toEqual("Foo");
  });
  it("should handle multi word", () => {
    expect(format.sentenceCase("foo bar")).toEqual("Foo bar");
  });
  it("should handle caps", () => {
    expect(format.sentenceCase("Foo bar")).toEqual("Foo bar");
  });
});
