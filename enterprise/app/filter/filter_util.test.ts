import "./test_setup";
import { stat_filter } from "../../../proto/stat_filter_ts_proto";
import { getFiltersFromDimensionParam } from "./filter_util";

describe("getFiltersFromDimensionParam", () => {
  it("parses single execution dimension filter", () => {
    const result = getFiltersFromDimensionParam("e1|6|abcdef");
    expect(result.length).toBe(1);
    expect(result[0]!.dimension!.execution).toBe(stat_filter.ExecutionDimensionType.WORKER_EXECUTION_DIMENSION);
    expect(result[0]!.value).toBe("abcdef");
  });

  it("parses single invocation dimension filter", () => {
    const result = getFiltersFromDimensionParam("i1|4|main");
    expect(result.length).toBe(1);
    expect(result[0]!.dimension!.invocation).toBe(stat_filter.InvocationDimensionType.BRANCH_INVOCATION_DIMENSION);
    expect(result[0]!.value).toBe("main");
  });

  it("parses multiple filters", () => {
    const result = getFiltersFromDimensionParam("e1|6|abcdef|i1|4|main");
    expect(result.length).toBe(2);
    expect(result[0]!.dimension!.execution).toBe(stat_filter.ExecutionDimensionType.WORKER_EXECUTION_DIMENSION);
    expect(result[0]!.value).toBe("abcdef");
    expect(result[1]!.dimension!.invocation).toBe(stat_filter.InvocationDimensionType.BRANCH_INVOCATION_DIMENSION);
    expect(result[1]!.value).toBe("main");
  });

  it("parses filter with empty value", () => {
    const result = getFiltersFromDimensionParam("e1|0|");
    expect(result.length).toBe(1);
    expect(result[0]!.dimension!.execution).toBe(stat_filter.ExecutionDimensionType.WORKER_EXECUTION_DIMENSION);
    expect(result[0]!.value).toBe("");
  });

  it("returns empty array for empty string", () => {
    const result = getFiltersFromDimensionParam("");
    expect(result.length).toBe(0);
  });

  it("returns empty array for malformed input", () => {
    const result = getFiltersFromDimensionParam("invalid");
    expect(result.length).toBe(0);
  });

  it("handles incomplete filter - missing value length", () => {
    const result = getFiltersFromDimensionParam("e1|");
    expect(result.length).toBe(0);
  });

  it("handles incomplete filter - missing value", () => {
    const result = getFiltersFromDimensionParam("e1|5|");
    expect(result.length).toBe(0);
  });

  it("handles value length mismatch", () => {
    const result = getFiltersFromDimensionParam("e1|10|short");
    expect(result.length).toBe(0);
  });
});
