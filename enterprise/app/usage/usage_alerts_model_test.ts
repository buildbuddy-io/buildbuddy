import Long from "long";
import { usage } from "../../../proto/usage_ts_proto";
import {
  USAGE_ALERTING_METRICS,
  usageAlertingAbsoluteThresholdFromInput,
  usageAlertingDurationThresholdToMinutes,
  usageAlertingMetricLabel,
} from "./usage_alerts_model";

const UsageAlertingMetric = usage.UsageAlertingMetric.Value;
type UsageAlertingMetric = usage.UsageAlertingMetric.Value;

function usageAlertingMetricValues(): UsageAlertingMetric[] {
  return Object.values(UsageAlertingMetric)
    .filter((value): value is UsageAlertingMetric => typeof value === "number")
    .filter((value) => value !== UsageAlertingMetric.UNKNOWN);
}

function sortedMetrics(metrics: UsageAlertingMetric[]): UsageAlertingMetric[] {
  return [...metrics].sort((a, b) => a - b);
}

describe("USAGE_ALERTING_METRICS", () => {
  it("has metadata for every non-unknown enum value", () => {
    expect(sortedMetrics(USAGE_ALERTING_METRICS.map(({ metric }) => metric))).toEqual(
      sortedMetrics(usageAlertingMetricValues())
    );
  });

  it("does not define duplicate metadata entries", () => {
    const metrics = USAGE_ALERTING_METRICS.map(({ metric }) => metric);
    expect(new Set(metrics).size).toBe(metrics.length);
  });

  it("defines labels and units for every metric", () => {
    for (const metadata of USAGE_ALERTING_METRICS) {
      expect(metadata.label).toMatch(/\S/);
      expect(["count", "bytes", "duration_usec", "duration_nanos"]).toContain(metadata.unit);
    }
  });

  it("returns a fallback label for unknown metrics", () => {
    expect(usageAlertingMetricLabel(UsageAlertingMetric.UNKNOWN)).toBe("Unknown");
  });

  it("stores usec-backed duration threshold inputs as microseconds", () => {
    const twoMinutes = Long.fromNumber(2);

    for (const metric of [
      UsageAlertingMetric.TOTAL_CACHED_ACTION_EXEC_USEC,
      UsageAlertingMetric.LINUX_EXECUTION_DURATION_USEC,
      UsageAlertingMetric.CLOUD_RBE_LINUX_EXECUTION_DURATION_USEC,
      UsageAlertingMetric.CLOUD_WORKFLOW_LINUX_EXECUTION_DURATION_USEC,
    ]) {
      expect(usageAlertingAbsoluteThresholdFromInput(metric, twoMinutes).toString()).toBe("120000000");
    }
  });

  it("stores nanos-backed duration threshold inputs as nanoseconds", () => {
    const twoMinutes = Long.fromNumber(2);

    expect(usageAlertingAbsoluteThresholdFromInput(UsageAlertingMetric.CLOUD_CPU_NANOS, twoMinutes).toString()).toBe(
      "120000000000"
    );
  });

  it("leaves count and byte threshold inputs unchanged", () => {
    const threshold = Long.fromNumber(123);

    expect(usageAlertingAbsoluteThresholdFromInput(UsageAlertingMetric.INVOCATIONS, threshold).toString()).toBe("123");
    expect(
      usageAlertingAbsoluteThresholdFromInput(UsageAlertingMetric.TOTAL_DOWNLOAD_SIZE_BYTES, threshold).toString()
    ).toBe("123");
  });

  it("formats stored duration thresholds as minutes", () => {
    for (const metric of [
      UsageAlertingMetric.TOTAL_CACHED_ACTION_EXEC_USEC,
      UsageAlertingMetric.LINUX_EXECUTION_DURATION_USEC,
      UsageAlertingMetric.CLOUD_RBE_LINUX_EXECUTION_DURATION_USEC,
      UsageAlertingMetric.CLOUD_WORKFLOW_LINUX_EXECUTION_DURATION_USEC,
    ]) {
      expect(usageAlertingDurationThresholdToMinutes(metric, Long.fromNumber(120e6)).toString()).toBe("2");
    }
    expect(
      usageAlertingDurationThresholdToMinutes(UsageAlertingMetric.CLOUD_CPU_NANOS, Long.fromNumber(120e9)).toString()
    ).toBe("2");
  });
});
