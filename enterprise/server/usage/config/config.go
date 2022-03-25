package config

import "flag"

var usageTrackingEnabled = flag.Bool("app.usage_tracking_enabled", false, "If set, enable usage data collection.")

func UsageTrackingEnabled() bool {
	return *usageTrackingEnabled
}
