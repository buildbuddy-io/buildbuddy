package main

type Config struct {
	// If using port-forwarding to access the prometheus server, this should look something like "http://localhost:<from_port>"
	// with command: `kubectl --context="" --namespace=<NAMESPACE> port-forward "<POD_NAME>" --address 0.0.0.0 <FROM_PORT>:<TO_PORT>`
	PrometheusAddress string `yaml:"prometheus_address"`
	// The total duration to poll metrics, after which we should continue with the rollout.
	MonitoringTimeframeSeconds int `yaml:"monitoring_timeframe_seconds"`
	// The frequency with which we should poll metrics.
	MonitoringTickerSeconds int `yaml:"monitoring_ticker_seconds"`
	// If we detect a metric is unhealthy, the duration we should keep polling that metric for recovery before we should rollback.
	UnhealthyMonitoringTimeframeSeconds int                `yaml:"unhealthy_monitoring_timeframe_seconds"`
	PrometheusMetrics                   []PrometheusMetric `yaml:"prometheus_metrics"`
}

type PrometheusMetric struct {
	Name          string `yaml:"name"`
	CanaryQuery   string `yaml:"canary_query"`
	BaselineQuery string `yaml:"baseline_query"`
	// If the canary's success rate is lower than the other apps by this threshold, we should rollback.
	CanaryHealthThreshold float64 `yaml:"canary_health_threshold"`
}
