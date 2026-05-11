import { usage } from "../../../proto/usage_ts_proto";

export type UsageAlertingMetric = usage.UsageAlertingMetric.Value;
export type UsageAlertingMetricUnit = "count" | "bytes" | "duration_nanos";

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
  { metric: UsageAlertingMetric.CACHED_BUILD_DURATION, label: "Cached build minutes", unit: "duration_nanos" },
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
    metric: UsageAlertingMetric.EXTERNAL_DOWNLOAD_SIZE_BYTES,
    label: "Total bytes downloaded from cache (External)",
    unit: "bytes",
  },
  {
    metric: UsageAlertingMetric.INTERNAL_DOWNLOAD_SIZE_BYTES,
    label: "Total bytes downloaded from cache (Internal)",
    unit: "bytes",
  },
  {
    metric: UsageAlertingMetric.WORKFLOW_DOWNLOAD_SIZE_BYTES,
    label: "Total bytes downloaded from cache (Workflows)",
    unit: "bytes",
  },
  {
    metric: UsageAlertingMetric.TOTAL_UPLOAD_SIZE_BYTES,
    label: "Total bytes uploaded to cache (All)",
    unit: "bytes",
  },
  {
    metric: UsageAlertingMetric.EXTERNAL_UPLOAD_SIZE_BYTES,
    label: "Total bytes uploaded to cache (External)",
    unit: "bytes",
  },
  {
    metric: UsageAlertingMetric.INTERNAL_UPLOAD_SIZE_BYTES,
    label: "Total bytes uploaded to cache (Internal)",
    unit: "bytes",
  },
  {
    metric: UsageAlertingMetric.WORKFLOW_UPLOAD_SIZE_BYTES,
    label: "Total bytes uploaded to cache (Workflows)",
    unit: "bytes",
  },
  {
    metric: UsageAlertingMetric.LINUX_EXECUTION_DURATION,
    label: "Linux remote execution duration (All)",
    unit: "duration_nanos",
  },
  {
    metric: UsageAlertingMetric.CLOUD_RBE_LINUX_EXECUTION_DURATION,
    label: "Linux remote execution duration (Cloud RBE)",
    unit: "duration_nanos",
  },
  {
    metric: UsageAlertingMetric.CLOUD_WORKFLOW_LINUX_EXECUTION_DURATION,
    label: "Linux remote execution duration (Workflows)",
    unit: "duration_nanos",
  },
  {
    metric: UsageAlertingMetric.LINUX_CPU_DURATION,
    label: "Linux CPU duration (All)",
    unit: "duration_nanos",
  },
  {
    metric: UsageAlertingMetric.CLOUD_RBE_LINUX_CPU_DURATION,
    label: "Linux CPU duration (Cloud RBE)",
    unit: "duration_nanos",
  },
  {
    metric: UsageAlertingMetric.CLOUD_WORKFLOW_LINUX_CPU_DURATION,
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
