package main

type Config struct {
	// If using port-forwarding to access the prometheus server, this should look something like "http://localhost:<from_port>"
	// with command: `kubectl --context="" --namespace=<NAMESPACE> port-forward "<POD_NAME>" --address 0.0.0.0 <FROM_PORT>:<TO_PORT>`
	PrometheusAddress string `yaml:"prometheus_address"`
	// The total duration to poll metrics, after which we should continue with the rollout.
	MonitoringTimeframeSeconds int                `yaml:"monitoring_timeframe_seconds"`
	PrometheusMetrics          []PrometheusMetric `yaml:"prometheus_metrics"`
}

type PrometheusMetric struct {
	Name          string `yaml:"name"`
	CanaryQuery   string `yaml:"canary_query"`
	BaselineQuery string `yaml:"baseline_query"`
	// The frequency with which we should poll this metric
	PollingIntervalSeconds int `yaml:"polling_interval_seconds"`
	// The max number of times the canary can consecutively report as unhealthy before we should rollback.
	MaxUnhealthyCount int `yaml:"max_unhealthy_count"`
	// If the canary's success rate is lower than the other apps by this threshold, it is considered unhealthy.
	HealthThreshold float64 `yaml:"health_threshold"`
}
