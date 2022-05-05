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

describe("durationSec", () => {
  it("should format 0 as 0s", () => {
    expect(format.durationSec(null)).toEqual("0s");
    expect(format.durationSec(0)).toEqual("0s");
  });
  it("should format < 1 ms as microseconds", () => {
    expect(format.durationSec(0.000001)).toEqual("1.00µs");
    expect(format.durationSec(0.0001)).toEqual("100µs");
    expect(format.durationSec(0.000999)).toEqual("999µs");
  });
  it("should format < 1 second as ms", () => {
    expect(format.durationSec(0.001)).toEqual("1.00ms");
    expect(format.durationSec(0.999)).toEqual("999ms");
  });
  it("should format < 1 minute as fractional seconds", () => {
    expect(format.durationSec(7.42)).toEqual("7.42s");
    expect(format.durationSec(59.94999)).toEqual("59.9s");
    expect(format.durationSec(59.95)).toEqual("60.0s");
    expect(format.durationSec(59.99999)).toEqual("60.0s");
  });

  // Convenience to-second conversions to make the following tests more readable.
  const second = 1;
  const minute = 60;
  const hour = 60 * 60;
  const day = 60 * 60 * 24;

  it("should format < 1 hour as m/s", () => {
    expect(format.durationSec(1 * minute)).toEqual("1m 0s");
    expect(format.durationSec(59 * minute + 59.499 * second)).toEqual("59m 59s");
    expect(format.durationSec(59 * minute + 59.5 * second)).toEqual("59m 59s");
    expect(format.durationSec(59 * minute + 59.999 * second)).toEqual("59m 59s");
  });
  it("should format < 1 day as h/m", () => {
    expect(format.durationSec(1 * hour)).toEqual("1h 0m");
    expect(format.durationSec(23 * hour + 59 * minute)).toEqual("23h 59m");
    expect(format.durationSec(23 * hour + 59 * minute + 29.999 * second)).toEqual("23h 59m");
    expect(format.durationSec(23 * hour + 59 * minute + 30 * second)).toEqual("23h 59m");
    expect(format.durationSec(23 * hour + 59 * minute + 59.999 * second)).toEqual("23h 59m");
  });
  it("should format < 1 week as d/h", () => {
    expect(format.durationSec(1 * day)).toEqual("1d 0h");
    expect(format.durationSec(6 * day + 23 * hour)).toEqual("6d 23h");
    expect(format.durationSec(6 * day + 23 * hour + 29 * minute + 59.999 * second)).toEqual("6d 23h");
    expect(format.durationSec(6 * day + 23 * hour + 30 * minute)).toEqual("6d 23h");
    expect(format.durationSec(6 * day + 23 * hour + 59 * minute + 59.999 * second)).toEqual("6d 23h");
  });
  it("should format larger timespans with decimals", () => {
    expect(format.durationSec(14 * day)).toEqual("2.00 weeks");
    expect(format.durationSec(30 * day)).toEqual("1.00 months");
    expect(format.durationSec(365 * day)).toEqual("1.00 years");
  });
});

describe("formatWithCommas", () => {
  it("should handle 0", () => {
    expect(format.formatWithCommas(new Long(0, 0))).toEqual("0");
  });
  it("should handle fewer than 4 digits", () => {
    expect(format.formatWithCommas(new Long(123, 0))).toEqual("123");
  });
  it("should handle 4 digits", () => {
    expect(format.formatWithCommas(new Long(1234, 0))).toEqual("1,234");
  });
  it("should handle 7 digits", () => {
    expect(format.formatWithCommas(new Long(1234567, 0))).toEqual("1,234,567");
  });
});

describe("bytes", () => {
  it("should abbreviate large numbers", () => {
    expect(format.bytes(0)).toEqual("0B");
    expect(format.bytes(99)).toEqual("99B");
    expect(format.bytes(100)).toEqual("0.1KB");
    expect(format.bytes(1020)).toEqual("1.02KB");
    expect(format.bytes(1023)).toEqual("1.023KB");
    expect(format.bytes(1e6 - 1)).toEqual("1000KB");
    expect(format.bytes(1e6)).toEqual("1MB");
    expect(format.bytes(1e9 - 1)).toEqual("1000MB");
    expect(format.bytes(1e9)).toEqual("1GB");
    expect(format.bytes(1e12 - 1)).toEqual("1000GB");
    expect(format.bytes(1e12)).toEqual("1TB");
    expect(format.bytes(1e15 - 1)).toEqual("1000TB");
    expect(format.bytes(1e15)).toEqual("1PB");
  });
});

describe("count", () => {
  it("should abbreviate large numbers", () => {
    expect(format.count(0)).toEqual("0");
    expect(format.count(99)).toEqual("99");
    expect(format.count(100)).toEqual("100");
    expect(format.count(1020)).toEqual("1.02K");
    expect(format.count(1023)).toEqual("1.023K");
    expect(format.count(1e6 - 1)).toEqual("1000K");
    expect(format.count(1e6)).toEqual("1M");
    expect(format.count(1.5e6)).toEqual("1.5M");
    expect(format.count(1e9 - 1)).toEqual("1000M");
    expect(format.count(1e9)).toEqual("1B");
  });
});
