import Long from "long";
import { usage } from "../../../proto/usage_ts_proto";

export type UsageAlertingMetric = usage.UsageAlertingMetric.Value;
export type UsageAlertingMetricUnit = "count" | "bytes" | "duration_usec" | "duration_nanos";

export type UsageAlertingMetricMetadata = {
  /** Usage alerting metric enum value represented by this metadata. */
  metric: UsageAlertingMetric;

  /** Human-readable label shown in the usage alerting UI. */
  label: string;

  /** Unit category used to format thresholds for this metric. */
  unit: UsageAlertingMetricUnit;
};

const UsageAlertingMetric = usage.UsageAlertingMetric.Value;

export const USAGE_ALERTING_METRICS: UsageAlertingMetricMetadata[] = [
  {
    metric: UsageAlertingMetric.INVOCATIONS,
    label: "Invocations",
    unit: "count",
  },
  {
    metric: UsageAlertingMetric.ACTION_CACHE_HITS,
    label: "Action cache hits",
    unit: "count",
  },
  {
    metric: UsageAlertingMetric.TOTAL_CACHED_ACTION_EXEC_USEC,
    label: "Cached build minutes",
    unit: "duration_usec",
  },
  {
    metric: UsageAlertingMetric.CAS_CACHE_HITS,
    label: "Content addressable storage cache hits",
    unit: "count",
  },
  {
    metric: UsageAlertingMetric.TOTAL_DOWNLOAD_SIZE_BYTES,
    label: "Total bytes downloaded from cache (All)",
    unit: "bytes",
  },
  {
    metric: UsageAlertingMetric.TOTAL_EXTERNAL_DOWNLOAD_SIZE_BYTES,
    label: "Total bytes downloaded from cache (External)",
    unit: "bytes",
  },
  {
    metric: UsageAlertingMetric.TOTAL_INTERNAL_DOWNLOAD_SIZE_BYTES,
    label: "Total bytes downloaded from cache (Internal)",
    unit: "bytes",
  },
  {
    metric: UsageAlertingMetric.TOTAL_WORKFLOW_DOWNLOAD_SIZE_BYTES,
    label: "Total bytes downloaded from cache (Workflows)",
    unit: "bytes",
  },
  {
    metric: UsageAlertingMetric.TOTAL_UPLOAD_SIZE_BYTES,
    label: "Total bytes uploaded to cache (All)",
    unit: "bytes",
  },
  {
    metric: UsageAlertingMetric.TOTAL_EXTERNAL_UPLOAD_SIZE_BYTES,
    label: "Total bytes uploaded to cache (External)",
    unit: "bytes",
  },
  {
    metric: UsageAlertingMetric.TOTAL_INTERNAL_UPLOAD_SIZE_BYTES,
    label: "Total bytes uploaded to cache (Internal)",
    unit: "bytes",
  },
  {
    metric: UsageAlertingMetric.TOTAL_WORKFLOW_UPLOAD_SIZE_BYTES,
    label: "Total bytes uploaded to cache (Workflows)",
    unit: "bytes",
  },
  {
    metric: UsageAlertingMetric.LINUX_EXECUTION_DURATION_USEC,
    label: "Linux remote execution duration (All)",
    unit: "duration_usec",
  },
  {
    metric: UsageAlertingMetric.CLOUD_RBE_LINUX_EXECUTION_DURATION_USEC,
    label: "Linux remote execution duration (Cloud RBE)",
    unit: "duration_usec",
  },
  {
    metric: UsageAlertingMetric.CLOUD_WORKFLOW_LINUX_EXECUTION_DURATION_USEC,
    label: "Linux remote execution duration (Workflows)",
    unit: "duration_usec",
  },
  {
    metric: UsageAlertingMetric.CLOUD_CPU_NANOS,
    label: "Linux CPU duration (All)",
    unit: "duration_nanos",
  },
  {
    metric: UsageAlertingMetric.CLOUD_RBE_CPU_NANOS,
    label: "Linux CPU duration (Cloud RBE)",
    unit: "duration_nanos",
  },
  {
    metric: UsageAlertingMetric.CLOUD_WORKFLOW_CPU_NANOS,
    label: "Linux CPU duration (Workflows)",
    unit: "duration_nanos",
  },
];

export function usageAlertingMetricMetadata(metric?: UsageAlertingMetric): UsageAlertingMetricMetadata | undefined {
  return USAGE_ALERTING_METRICS.find((option) => option.metric === metric);
}

export function usageAlertingMetricLabel(metric?: UsageAlertingMetric): string {
  return usageAlertingMetricMetadata(metric)?.label ?? "Unknown";
}

export function usageAlertingMetricUnit(metric?: UsageAlertingMetric): UsageAlertingMetricUnit {
  return usageAlertingMetricMetadata(metric)?.unit ?? "count";
}

export function usageAlertingAbsoluteThresholdFromInput(metric: UsageAlertingMetric, threshold: Long): Long {
  switch (usageAlertingMetricUnit(metric)) {
    case "duration_usec":
      return threshold.multiply(60e6);
    case "duration_nanos":
      return threshold.multiply(60e9);
    default:
      return threshold;
  }
}

export function usageAlertingDurationThresholdToMinutes(
  metric: UsageAlertingMetric | undefined,
  threshold: Long
): Long {
  switch (usageAlertingMetricUnit(metric)) {
    case "duration_usec":
      return roundDivide(threshold, 60e6);
    case "duration_nanos":
      return roundDivide(threshold, 60e9);
    default:
      return threshold;
  }
}

function roundDivide(value: Long, divisor: number): Long {
  const d = Long.fromNumber(divisor);
  return value.add(d.divide(2)).divide(d);
}
