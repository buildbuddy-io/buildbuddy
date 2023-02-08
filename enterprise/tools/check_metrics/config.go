package main

type Config struct {
	// If using port-forwarding to access the prometheus server, this should look something like "http://localhost:<from_port>"
	// with command: `kubectl --context="" --namespace=<NAMESPACE> port-forward "<POD_NAME>" --address 0.0.0.0 <FROM_PORT>:<TO_PORT>`
	PrometheusAddress string `yaml:"prometheus_address"`
	// The total duration to poll metrics, after which we should continue with the rollout.
	MonitoringTimeframeSeconds int `yaml:"monitoring_timeframe_seconds"`
	// The default frequency with which we should poll each metric
	PollingIntervalSeconds int `yaml:"polling_interval_seconds"`
	// The default max number of times the metric can consecutively report as unhealthy before we should rollback
	MaxUnhealthyCount int                `yaml:"max_unhealthy_count"`
	PrometheusMetrics []PrometheusMetric `yaml:"prometheus_metrics"`
}

type PrometheusMetric struct {
	Name  string `yaml:"name"`
	Query string `yaml:"query"`
	// Set to override the default polling interval
	PollingIntervalSeconds int `yaml:"polling_interval_seconds"`
	// Set to override the default unhealthy count
	MaxUnhealthyCount int `yaml:"max_unhealthy_count"`
	// If the metric does not meet this health threshold, it is considered unhealthy
	HealthThreshold HealthThreshold `yaml:"health_threshold"`
	// Set if it's valid for the metric value to be 0 or missing
	// Ex. For error log count or invocation failure rate, at any given time the value can be 0
	IsMissingDataValid bool `yaml:"is_missing_data_valid"`
}

// Exactly one field should be set in the HealthThreshold
type HealthThreshold struct {
	AbsoluteRange *AbsoluteRange `yaml:"absolute"`
	RelativeRange *RelativeRange `yaml:"relative"`
}

// AbsoluteRange is used to ensure a metric's value is within a range specified by Max and Min
// For example, you would set Min=0.8 to ensure the app health check never dips below 80%
// If `Max` or `Min` is omitted, it will be ignored
type AbsoluteRange struct {
	Max *float64 `yaml:"max"`
	Min *float64 `yaml:"min"`
}

// RelativeRange is used to compare a metric against another and ensure their values are similar
// For example, you might use this to compare metrics for a canary to the baseline
// Exactly one `Within` field should be set
type RelativeRange struct {
	ComparisonQuery string `yaml:"comparison_query"`

	// Within is used to specify an absolute value the metric should not differ from the comparison
	// For example, if within=100, if the metric is not within 100 of the comparison, it will be considered unhealthy
	Within *Within `yaml:"within"`

	// WithinPercentage is used to specify that the metric should not differ from the comparison by this percentage
	// For example, if withinPercentage=0.1, if the metric is not within 10% of the comparison, it will be considered unhealthy
	// The percentage is taken relative to the comparison (i.e. the comparison value is the denominator when calculating
	// the percentage)
	WithinPercentage *Within `yaml:"within_percentage"`
}

type Within struct {
	Value float64

	// Set GreaterBy=true if you expect a healthy metric to be less than or equal to the comparison
	// If it's greater than the comparison by Value, the metric is considered unhealthy
	// For example, if GreaterBy=true and Value=100, metric=500 and comparison=300, the metric is greater by 200
	// That is not within the value of 100, so the metric is unhealthy
	GreaterBy *bool `yaml:"greater_by"`
	// Set LessBy=true if you expect a healthy metric to be greater than or equal to the comparison
	// If it's less than the comparison by Value, the metric is considered unhealthy
	// For example, if LessBy=true and Value=100, metric=300 and comparison=500, the metric is less by 200
	// That is not within the value of 100, so the metric is unhealthy
	LessBy *bool `yaml:"less_by"`
}
