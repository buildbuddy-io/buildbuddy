package config

import "flag"

var trendsHeatmapEnabled = flag.Bool("app.trends_heatmap_enabled", false, "If set, enable a fancy heatmap UI for exploring build trends.")

func TrendsHeatmapEnabled() bool {
	return *trendsHeatmapEnabled
}
