package config

import "flag"

var trendsHeatmapEnabled = flag.Bool("app.trends_heatmap_enabled", false, "If set, enable a fancy heatmap UI for exploring build trends.")
var statFiltersEnabled = flag.Bool("app.stat_filters_enabled", false, "If set, parse and use new filters for invocations and executions.")

func TrendsHeatmapEnabled() bool {
	return *trendsHeatmapEnabled
}

func StatFiltersEnabled() bool {
	return *statFiltersEnabled
}
