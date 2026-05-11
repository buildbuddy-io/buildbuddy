import { usage } from "../../../proto/usage_ts_proto";
import { USAGE_ALERTING_METRICS, usageAlertingMetricLabel } from "./usage_alerts_model";

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
      expect(["count", "bytes", "duration_nanos"]).toContain(metadata.unit);
    }
  });

  it("returns a fallback label for unknown metrics", () => {
    expect(usageAlertingMetricLabel(UsageAlertingMetric.UNKNOWN)).toBe("Unknown");
  });
});
