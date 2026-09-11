// Generates the Grafana dashboard for Raft metrics.
package main

import (
	"github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/dash"
	"github.com/grafana/grafana-foundation-sdk/go/cog"
	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/heatmap"
	"github.com/grafana/grafana-foundation-sdk/go/prometheus"
	"github.com/grafana/grafana-foundation-sdk/go/table"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
)

func row(title string) *dashboard.RowBuilder {
	return dashboard.NewRowBuilder(title).Collapsed(true)
}

func q(expr, legend string) *prometheus.DataqueryBuilder {
	return prometheus.NewDataqueryBuilder().
		Expr(expr).
		EditorMode(prometheus.QueryEditorModeCode).
		LegendFormat(legend).
		Range()
}

func ts(title, unit string) *timeseries.PanelBuilder {
	panel := timeseries.NewPanelBuilder().
		Title(title).
		Datasource(dash.Prometheus()).
		ColorScheme(dashboard.NewFieldColorBuilder().Mode(dashboard.FieldColorModeIdPaletteClassic)).
		LineWidth(1).
		FillOpacity(0).
		GradientMode(common.GraphGradientModeNone).
		ShowPoints(common.VisibilityModeAuto).
		Legend(common.NewVizLegendOptionsBuilder().
			DisplayMode(common.LegendDisplayModeList).
			Placement(common.LegendPlacementBottom).
			ShowLegend(true)).
		Tooltip(common.NewVizTooltipOptionsBuilder().
			Mode(common.TooltipDisplayModeSingle).
			Sort(common.SortOrderNone)).
		Height(8).
		Span(12)
	if unit != "" {
		panel.Unit(unit)
	}
	return panel
}

func metaCacheGetMultiPanel() *timeseries.PanelBuilder {
	return ts("/GetMulti", dash.UnitMicroseconds).
		Description("").
		Legend(common.NewVizLegendOptionsBuilder().
			DisplayMode(common.LegendDisplayModeList).
			Placement(common.LegendPlacementRight).
			ShowLegend(true)).
		Tooltip(common.NewVizTooltipOptionsBuilder().
			Mode(common.TooltipDisplayModeMulti).
			Sort(common.SortOrderDescending)).
		WithOverride(dashboard.MatcherConfig{Id: "byName", Options: "QPS"}, []dashboard.DynamicConfigValue{dashboard.DynamicConfigValue{Id: "custom.axisPlacement", Value: "right"}, dashboard.DynamicConfigValue{Id: "unit", Value: "reqps"}}).
		WithTarget(q("histogram_quantile(0.99, sum(rate(buildbuddy_remote_cache_method_handling_usec_bucket{region=\"${region}\", job=\"buildbuddy-app\", cache_method=\"GetMulti\"}[${window}])) by (le)\n)", "P99").
			Interval("").
			QueryType("randomWalk").
			RefId("A")).
		WithTarget(q("histogram_quantile(0.95, sum(rate(buildbuddy_remote_cache_method_handling_usec_bucket{region=\"${region}\", job=\"buildbuddy-app\", cache_method=\"GetMulti\"}[${window}])) by (le)\n)", "P95").
			Interval("").
			RefId("B")).
		WithTarget(q("histogram_quantile(0.5, sum(rate(buildbuddy_remote_cache_method_handling_usec_bucket{region=\"${region}\", job=\"buildbuddy-app\", cache_method=\"GetMulti\"}[${window}])) by (le)\n)", "P50").
			Interval("").
			RefId("C")).
		WithTarget(q("sum(rate(buildbuddy_remote_cache_method_handled_total{region=\"${region}\", job=\"buildbuddy-app\", cache_method=\"GetMulti\"}[${window}]))", "QPS").
			Interval("").
			RefId("D"))
}

func metaCacheGetPanel() *timeseries.PanelBuilder {
	return ts("/Get", dash.UnitMicroseconds).
		Description("").
		Legend(common.NewVizLegendOptionsBuilder().
			DisplayMode(common.LegendDisplayModeList).
			Placement(common.LegendPlacementRight).
			ShowLegend(true)).
		Tooltip(common.NewVizTooltipOptionsBuilder().
			Mode(common.TooltipDisplayModeMulti).
			Sort(common.SortOrderDescending)).
		WithOverride(dashboard.MatcherConfig{Id: "byName", Options: "QPS"}, []dashboard.DynamicConfigValue{dashboard.DynamicConfigValue{Id: "custom.axisPlacement", Value: "right"}, dashboard.DynamicConfigValue{Id: "unit", Value: "reqps"}}).
		WithTarget(q("histogram_quantile(0.99, sum(rate(buildbuddy_remote_cache_method_handling_usec_bucket{region=\"${region}\", job=\"buildbuddy-app\", cache_method=\"Get\"}[${window}])) by (le)\n)", "P99").
			Interval("").
			QueryType("randomWalk").
			RefId("A")).
		WithTarget(q("histogram_quantile(0.95, sum(rate(buildbuddy_remote_cache_method_handling_usec_bucket{region=\"${region}\", job=\"buildbuddy-app\", cache_method=\"Get\"}[${window}])) by (le)\n)", "P95").
			Interval("").
			RefId("B")).
		WithTarget(q("histogram_quantile(0.5, sum(rate(buildbuddy_remote_cache_method_handling_usec_bucket{region=\"${region}\", job=\"buildbuddy-app\", cache_method=\"Get\"}[${window}])) by (le)\n)", "P50").
			Interval("").
			RefId("C")).
		WithTarget(q("sum(rate(buildbuddy_remote_cache_method_handled_total{region=\"${region}\", job=\"buildbuddy-app\", cache_method=\"Get\"}[${window}]))", "QPS").
			Interval("").
			RefId("D"))
}

func metaCacheSetMultiPanel() *timeseries.PanelBuilder {
	return ts("/SetMulti", dash.UnitMicroseconds).
		Description("").
		Legend(common.NewVizLegendOptionsBuilder().
			DisplayMode(common.LegendDisplayModeList).
			Placement(common.LegendPlacementRight).
			ShowLegend(true)).
		Tooltip(common.NewVizTooltipOptionsBuilder().
			Mode(common.TooltipDisplayModeMulti).
			Sort(common.SortOrderDescending)).
		WithOverride(dashboard.MatcherConfig{Id: "byName", Options: "QPS"}, []dashboard.DynamicConfigValue{dashboard.DynamicConfigValue{Id: "custom.axisPlacement", Value: "right"}, dashboard.DynamicConfigValue{Id: "unit", Value: "reqps"}}).
		WithTarget(q("histogram_quantile(0.99, sum(rate(buildbuddy_remote_cache_method_handling_usec_bucket{region=\"${region}\", job=\"buildbuddy-app\", cache_method=\"SetMulti\"}[${window}])) by (le)\n)", "P99").
			Interval("").
			QueryType("randomWalk").
			RefId("A")).
		WithTarget(q("histogram_quantile(0.95, sum(rate(buildbuddy_remote_cache_method_handling_usec_bucket{region=\"${region}\", job=\"buildbuddy-app\", cache_method=\"SetMulti\"}[${window}])) by (le)\n)", "P95").
			Interval("").
			RefId("B")).
		WithTarget(q("histogram_quantile(0.5, sum(rate(buildbuddy_remote_cache_method_handling_usec_bucket{region=\"${region}\", job=\"buildbuddy-app\", cache_method=\"SetMulti\"}[${window}])) by (le)\n)", "P50").
			Interval("").
			RefId("C")).
		WithTarget(q("sum(rate(buildbuddy_remote_cache_method_handled_total{region=\"${region}\", job=\"buildbuddy-app\", cache_method=\"SetMulti\"}[${window}]))", "QPS").
			Interval("").
			RefId("D"))
}

func metaCacheFindMissingPanel() *timeseries.PanelBuilder {
	return ts("/FindMissing", dash.UnitMicroseconds).
		Description("").
		Legend(common.NewVizLegendOptionsBuilder().
			DisplayMode(common.LegendDisplayModeList).
			Placement(common.LegendPlacementRight).
			ShowLegend(true)).
		Tooltip(common.NewVizTooltipOptionsBuilder().
			Mode(common.TooltipDisplayModeMulti).
			Sort(common.SortOrderDescending)).
		WithOverride(dashboard.MatcherConfig{Id: "byName", Options: "QPS"}, []dashboard.DynamicConfigValue{dashboard.DynamicConfigValue{Id: "custom.axisPlacement", Value: "right"}, dashboard.DynamicConfigValue{Id: "unit", Value: "reqps"}}).
		WithTarget(q("histogram_quantile(0.99, sum(rate(buildbuddy_remote_cache_method_handling_usec_bucket{region=\"${region}\", job=\"buildbuddy-app\", cache_method=\"FindMissing\"}[${window}])) by (le)\n)", "P99").
			Interval("").
			QueryType("randomWalk").
			RefId("A")).
		WithTarget(q("histogram_quantile(0.95, sum(rate(buildbuddy_remote_cache_method_handling_usec_bucket{region=\"${region}\", job=\"buildbuddy-app\", cache_method=\"FindMissing\"}[${window}])) by (le)\n)", "P95").
			Interval("").
			RefId("B")).
		WithTarget(q("histogram_quantile(0.5, sum(rate(buildbuddy_remote_cache_method_handling_usec_bucket{region=\"${region}\", job=\"buildbuddy-app\", cache_method=\"FindMissing\"}[${window}])) by (le)\n)", "P50").
			Interval("").
			RefId("C")).
		WithTarget(q("sum(rate(buildbuddy_remote_cache_method_handled_total{region=\"${region}\", job=\"buildbuddy-app\", cache_method=\"FindMissing\"}[${window}]))", "QPS").
			Interval("").
			RefId("D"))
}

func metadataServerOverallPodNhidGkeNodePanel() *table.PanelBuilder {
	return table.NewPanelBuilder().
		Title("pod  <> nhid <>  gke node").
		Datasource(dash.Prometheus()).
		Height(8).
		Span(12).
		WithTarget(q("avg(buildbuddy_raft_ranges{region=\"${region}\", namespace=\"${namespace}\"}) by (node_host_id, pod_name)\n  * on(pod_name) group_left(node)\n  label_replace(\n    kube_pod_info{namespace=\"${namespace}\", pod=~\"metadata-server-.*\"},\n    \"pod_name\", \"$1\", \"pod\", \"(.*)\"\n  )", "__auto").
			Instant().
			Format(prometheus.PromQueryFormatTable).
			RefId("A")).
		WithTransformation(dashboard.DataTransformerConfig{Id: "organize", Options: map[string]any{"excludeByName": map[string]any{"Time": true, "Value": true}, "includeByName": map[string]any{}, "indexByName": map[string]any{"Time": 0, "Value": 4, "node": 3, "node_host_id": 2, "pod_name": 1}, "renameByName": map[string]any{}}}).
		WithOverride(dashboard.MatcherConfig{Id: "byName", Options: "node"}, []dashboard.DynamicConfigValue{dashboard.DynamicConfigValue{Id: "custom.width", Value: 327}}).
		WithOverride(dashboard.MatcherConfig{Id: "byName", Options: "node_host_id"}, []dashboard.DynamicConfigValue{dashboard.DynamicConfigValue{Id: "custom.width", Value: 319}}).
		WithOverride(dashboard.MatcherConfig{Id: "byName", Options: "pod_name"}, []dashboard.DynamicConfigValue{dashboard.DynamicConfigValue{Id: "custom.width", Value: 160}}).
		SortBy([]cog.Builder[common.TableSortByFieldState]{common.NewTableSortByFieldStateBuilder().DisplayName("pod_name").Desc(true)})
}

func metadataServerOverallSetPanel() *timeseries.PanelBuilder {
	return ts("/Set", dash.UnitSeconds).
		Description("").
		Legend(common.NewVizLegendOptionsBuilder().
			DisplayMode(common.LegendDisplayModeList).
			Placement(common.LegendPlacementRight).
			ShowLegend(true)).
		Tooltip(common.NewVizTooltipOptionsBuilder().
			Mode(common.TooltipDisplayModeMulti).
			Sort(common.SortOrderNone)).
		FillOpacity(10).
		ShowPoints(common.VisibilityModeNever).
		WithOverride(dashboard.MatcherConfig{Id: "byName", Options: "QPS"}, []dashboard.DynamicConfigValue{dashboard.DynamicConfigValue{Id: "unit", Value: "reqps"}, dashboard.DynamicConfigValue{Id: "custom.axisPlacement", Value: "right"}}).
		WithTarget(q("histogram_quantile(0.99, sum(rate(grpc_server_handling_seconds_bucket{region=\"${region}\", namespace=\"${namespace}\", grpc_service=\"metadata.service.MetadataService\", grpc_method=\"Set\"}[${window}])) by (le)\n)", "P99").
			Exemplar(true).
			Interval("").
			QueryType("randomWalk").
			RefId("A")).
		WithTarget(q("histogram_quantile(0.95, sum(rate(grpc_server_handling_seconds_bucket{region=\"${region}\", namespace=\"${namespace}\", grpc_service=\"metadata.service.MetadataService\", grpc_method=\"Set\"}[${window}])) by (le)\n)", "P95").
			Exemplar(true).
			Interval("").
			RefId("B")).
		WithTarget(q("histogram_quantile(0.50, sum(rate(grpc_server_handling_seconds_bucket{region=\"${region}\", namespace=\"${namespace}\", grpc_service=\"metadata.service.MetadataService\", grpc_method=\"Set\"}[${window}])) by (le)\n)", "P50").
			Exemplar(true).
			Interval("").
			RefId("C")).
		WithTarget(q("sum(rate(grpc_server_handled_total{region=\"${region}\", namespace=\"${namespace}\", grpc_service=\"metadata.service.MetadataService\", grpc_method=\"Set\"}[${window}]))", "QPS").
			Exemplar(true).
			Interval("").
			RefId("D"))
}

func metadataServerOverallGetPanel() *timeseries.PanelBuilder {
	return ts("/Get", dash.UnitSeconds).
		Description("").
		Legend(common.NewVizLegendOptionsBuilder().
			DisplayMode(common.LegendDisplayModeList).
			Placement(common.LegendPlacementRight).
			ShowLegend(true)).
		Tooltip(common.NewVizTooltipOptionsBuilder().
			Mode(common.TooltipDisplayModeMulti).
			Sort(common.SortOrderNone)).
		FillOpacity(10).
		ShowPoints(common.VisibilityModeNever).
		WithOverride(dashboard.MatcherConfig{Id: "byName", Options: "QPS"}, []dashboard.DynamicConfigValue{dashboard.DynamicConfigValue{Id: "unit", Value: "reqps"}, dashboard.DynamicConfigValue{Id: "custom.axisPlacement", Value: "right"}}).
		WithTarget(q("histogram_quantile(0.99, sum(rate(grpc_server_handling_seconds_bucket{region=\"${region}\", namespace=\"${namespace}\", grpc_service=\"metadata.service.MetadataService\", grpc_method=\"Get\"}[${window}])) by (le)\n)", "P99").
			Exemplar(true).
			Interval("").
			QueryType("randomWalk").
			RefId("A")).
		WithTarget(q("histogram_quantile(0.95, sum(rate(grpc_server_handling_seconds_bucket{region=\"${region}\", namespace=\"${namespace}\", grpc_service=\"metadata.service.MetadataService\", grpc_method=\"Get\"}[${window}])) by (le)\n)", "P95").
			Exemplar(true).
			Interval("").
			RefId("B")).
		WithTarget(q("histogram_quantile(0.50, sum(rate(grpc_server_handling_seconds_bucket{region=\"${region}\", namespace=\"${namespace}\", grpc_service=\"metadata.service.MetadataService\", grpc_method=\"Get\"}[${window}])) by (le)\n)", "P50").
			Exemplar(true).
			Interval("").
			RefId("C")).
		WithTarget(q("sum(rate(grpc_server_handled_total{region=\"${region}\", namespace=\"${namespace}\", grpc_service=\"metadata.service.MetadataService\", grpc_method=\"Get\"}[${window}])) ", "QPS").
			Exemplar(true).
			Interval("").
			RefId("D"))
}

func splitsMovesSplitDurationPanel() *timeseries.PanelBuilder {
	return ts("Split Duration", dash.UnitMicroseconds).
		WithOverride(dashboard.MatcherConfig{Id: "byNames", Options: map[string]any{"mode": "exclude", "names": []any{"Value"}, "prefix": "All except:", "readOnly": true}}, []dashboard.DynamicConfigValue{dashboard.DynamicConfigValue{Id: "custom.hideFrom", Value: map[string]any{"legend": false, "tooltip": false, "viz": true}}}).
		WithTarget(q("histogram_quantile(0.5, sum(rate(buildbuddy_raft_split_duration_usec_bucket{region=\"${region}\", namespace=\"${namespace}\"}[${window}])) by (le))", "p50").
			Exemplar(true).
			Interval("").
			RefId("A")).
		WithTarget(q("histogram_quantile(0.9, sum(rate(buildbuddy_raft_split_duration_usec_bucket{region=\"${region}\", namespace=\"${namespace}\"}[${window}])) by (le))", "p90").
			Exemplar(true).
			Interval("").
			RefId("B")).
		WithTarget(q("histogram_quantile(0.99, sum(rate(buildbuddy_raft_split_duration_usec_bucket{region=\"${region}\", namespace=\"${namespace}\"}[${window}])) by (le))", "p99").
			Exemplar(true).
			Interval("").
			RefId("C"))
}

func pebbleBlockCacheHitsAndMissesPanel() *timeseries.PanelBuilder {
	return ts("block cache hits and misses", dash.UnitRequestsPerSec).
		Description("").
		Legend(common.NewVizLegendOptionsBuilder().
			DisplayMode(common.LegendDisplayModeList).
			Placement(common.LegendPlacementRight).
			ShowLegend(true)).
		Tooltip(common.NewVizTooltipOptionsBuilder().
			Mode(common.TooltipDisplayModeMulti).
			Sort(common.SortOrderDescending)).
		AxisPlacement(common.AxisPlacementLeft).
		WithOverride(dashboard.MatcherConfig{Id: "byName", Options: "hit_ratio"}, []dashboard.DynamicConfigValue{dashboard.DynamicConfigValue{Id: "custom.axisPlacement", Value: "right"}, dashboard.DynamicConfigValue{Id: "unit", Value: "percentunit"}}).
		WithTarget(q("sum by (cache_status) (rate(buildbuddy_remote_cache_pebble_cache_pebble_block_cache_requests_count{region=\"${region}\", job=\"metadata-server\"}[${window}]))", "{{cache_status}}").
			Interval("").
			QueryType("randomWalk").
			RefId("A")).
		WithTarget(q("sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_block_cache_requests_count{region=\"${region}\", job=\"metadata-server\", cache_status=\"hit\"}[${window}])) / sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_block_cache_requests_count{region=\"${region}\", job=\"metadata-server\"}[${window}]))", "hit_ratio").
			Interval("").
			QueryType("randomWalk").
			RefId("B"))
}

func pebbleCompactionStatePanel() *timeseries.PanelBuilder {
	return ts("Compaction state", dash.UnitBytes).
		WithOverride(dashboard.MatcherConfig{Id: "byName", Options: "in progress (count)"}, []dashboard.DynamicConfigValue{dashboard.DynamicConfigValue{Id: "custom.axisPlacement", Value: "right"}, dashboard.DynamicConfigValue{Id: "unit", Value: "none"}}).
		WithOverride(dashboard.MatcherConfig{Id: "byName", Options: "marked files"}, []dashboard.DynamicConfigValue{dashboard.DynamicConfigValue{Id: "custom.axisPlacement", Value: "right"}, dashboard.DynamicConfigValue{Id: "unit", Value: "none"}}).
		WithTarget(q("sum(buildbuddy_remote_cache_pebble_cache_pebble_compact_in_progress_bytes{region=\"${region}\",cache_name=\"raft\", namespace=\"${namespace}\"})", "in progress (bytes)").
			RefId("B")).
		WithTarget(q("sum(buildbuddy_remote_cache_pebble_cache_pebble_compact_in_progress{region=\"${region}\",cache_name=\"raft\", namespace=\"${namespace}\"})", "in progress (count)").
			RefId("C")).
		WithTarget(q("sum(buildbuddy_remote_cache_pebble_cache_pebble_compact_marked_files{region=\"${region}\",cache_name=\"raft\", namespace=\"${namespace}\"})", "marked files").
			RefId("D"))
}

func pebbleCompactionEstimatedDebtPanel() *timeseries.PanelBuilder {
	return ts("Compaction estimated debt", dash.UnitBytes).
		WithOverride(dashboard.MatcherConfig{Id: "byName", Options: "in progress (count)"}, []dashboard.DynamicConfigValue{dashboard.DynamicConfigValue{Id: "custom.axisPlacement", Value: "right"}, dashboard.DynamicConfigValue{Id: "unit", Value: "none"}}).
		WithOverride(dashboard.MatcherConfig{Id: "byName", Options: "marked files"}, []dashboard.DynamicConfigValue{dashboard.DynamicConfigValue{Id: "custom.axisPlacement", Value: "right"}, dashboard.DynamicConfigValue{Id: "unit", Value: "none"}}).
		WithTarget(q("sum(buildbuddy_remote_cache_pebble_cache_pebble_compact_estimated_debt_bytes{region=\"${region}\", cache_name=\"raft\", namespace=\"${namespace}\"}) by (pod_name)", "{{pod_name}}").
			RefId("A"))
}

func evictionDiskCachePartitionUsagePanel() *timeseries.PanelBuilder {
	return ts("Disk Cache Partition Usage ", dash.UnitPercentUnit).
		Height(11).
		AxisPlacement(common.AxisPlacementLeft).
		Min(0).
		WithOverride(dashboard.MatcherConfig{Id: "byFrameRefID", Options: "B"}, []dashboard.DynamicConfigValue{dashboard.DynamicConfigValue{Id: "unit", Value: "bytes"}, dashboard.DynamicConfigValue{Id: "custom.axisPlacement", Value: "right"}}).
		WithTarget(q("sum(buildbuddy_remote_cache_disk_cache_partition_size_bytes{region=\"${region}\",cache_name=\"raft\", namespace=\"${namespace}\"})by (partition_id)/max(buildbuddy_remote_cache_disk_cache_partition_capacity_bytes{region=\"${region}\", cache_name=\"raft\", namespace=\"${namespace}\"}) by (partition_id)", " {{partition_id}}").
			Exemplar(true).
			Interval("").
			RefId("A")).
		WithTarget(q("sum(buildbuddy_remote_cache_disk_cache_partition_size_bytes{region=\"${region}\",cache_name=\"raft\", namespace=\"${namespace}\"}) by (pod_name, partition_id)", "{{pod_name}} {{partition_id}}").
			RefId("B"))
}

func evictionEvictionAgePartitionIdPanel() *heatmap.PanelBuilder {
	return heatmap.NewPanelBuilder().
		Title("Eviction Age (${partition_id})").
		Datasource(dash.Prometheus()).
		MaxDataPoints(25).
		Calculate(false).
		CellGap(0).
		CellRadius(2).
		Color(heatmap.NewHeatmapColorOptionsBuilder().
			Mode(heatmap.HeatmapColorModeOpacity).
			Scheme("Oranges").
			Fill("#3274D9").
			Scale(heatmap.HeatmapColorScaleExponential).
			Exponent(0.5).
			Steps(128).
			Reverse(false)).
		FilterValues(heatmap.NewFilterValueRangeBuilder().Le(1e-9)).
		RowsFrame(heatmap.NewRowsHeatmapOptionsBuilder().Layout(common.HeatmapCellLayoutAuto)).
		ShowValue(common.VisibilityModeNever).
		Tooltip(heatmap.NewHeatmapTooltipBuilder().
			Mode(common.TooltipDisplayModeSingle).
			ShowColorScale(false).
			YHistogram(true)).
		YAxis(heatmap.NewYAxisConfigBuilder().
			AxisPlacement(common.AxisPlacementLeft).
			Reverse(false).
			Decimals(0).
			Unit(dash.UnitDurationMs).
			ScaleDistribution(common.NewScaleDistributionConfigBuilder().
				Type(common.ScaleDistributionLog).
				Log(2))).
		ExemplarsColor("rgba(255,0,255,0.7)").
		HideLegend().
		ScaleDistribution(common.NewScaleDistributionConfigBuilder().
			Type(common.ScaleDistributionLog).
			Log(2)).
		Height(10).
		Span(12).
		WithTarget(q("sum(increase(buildbuddy_remote_cache_disk_cache_eviction_age_msec_bucket{region=\"${region}\", partition_id=\"${partition_id}\", cache_name=\"raft\", namespace=\"${namespace}\"}[$__interval])) by (le)", "{{le}}").
			Exemplar(true).
			Interval("").
			Format(prometheus.PromQueryFormatHeatmap).
			RefId("A"))
}

func metaCacheRow() *dashboard.RowBuilder {
	return row("Meta Cache").
		WithPanel(ts("Request Mix", "").
			Description("").
			WithTarget(q("sum(rate(buildbuddy_remote_cache_method_handled_total{region=\"${region}\", job=\"buildbuddy-app\"}[${window}])) by (cache_method)\n", "{{cache_method}}").
				Interval("").
				QueryType("randomWalk").
				RefId("A"))).
		WithPanel(metaCacheGetMultiPanel()).
		WithPanel(metaCacheGetPanel()).
		WithPanel(metaCacheSetMultiPanel()).
		WithPanel(metaCacheFindMissingPanel())
}

func mdloadRow() *dashboard.RowBuilder {
	return row("mdload").
		WithPanel(ts("Final Error Count", "").
			WithTarget(q("sum(increase(buildbuddy_mdload_final_error_count{region=\"${region}\"}[${window}])) by (method)", "{{method}}").
				RefId("A"))).
		WithPanel(ts("Total Error Count (including retries)", "").
			WithTarget(q("sum(increase(buildbuddy_mdload_total_error_count{region=\"${region}\"}[${window}])) by (method)", "{{method}}").
				RefId("A")))
}

func metadataServerOverallRow() *dashboard.RowBuilder {
	return row("Metadata Server Overall").
		WithPanel(ts("CPU", dash.UnitPercentUnit).
			Max(1).
			WithTarget(q("1 - (avg by(mode,nodename) ((rate(node_cpu_seconds_total{mode=\"idle\"}[1m])) * on(instance) group_left(nodename) (node_uname_info{region=\"${region}\", nodename=~\"^gke-.*-mds-.*-([0-9a-f]{8})-(grp-)?[0-9a-z]{4}$\"})))", "{{nodename}}").
				RefId("A"))).
		WithPanel(metadataServerOverallPodNhidGkeNodePanel()).
		WithPanel(ts("instances", dash.UnitShort).
			ShowPoints(common.VisibilityModeNever).
			Decimals(0).
			Min(0).
			WithTarget(q("sum(up{region=\"${region}\", job=\"metadata-server\", namespace=\"${namespace}\"})", "Up").
				Interval("").
				QueryType("randomWalk").
				RefId("A")).
			WithTarget(q("sum(kube_pod_status_ready{region=\"${region}\", pod=~\"metadata-server-([0-9a-f]{8,10}-.*|[0-9]+)$\", namespace=\"${namespace}\"})", "Ready").
				Exemplar(true).
				Interval("").
				RefId("B"))).
		WithPanel(metadataServerOverallSetPanel()).
		WithPanel(metadataServerOverallGetPanel()).
		WithPanel(ts("Raft Proposals", dash.UnitRequestsPerSec).
			WithTarget(q("sum(rate(buildbuddy_raft_proposals{region=\"${region}\",  namespace=\"${namespace}\"}[${window}])) by (pod_name)", "{{pod_name}}").
				Exemplar(true).
				Interval("").
				RefId("A"))).
		WithPanel(ts("RangeCache Hit Rate", dash.UnitPercentUnit).
			Decimals(3).
			WithTarget(q("sum(rate(buildbuddy_raft_rangecache_lookups{rangecache_event_type=\"hit\", region=\"${region}\", namespace=\"${namespace}\"}[${window}]))/sum(rate(buildbuddy_raft_rangecache_lookups{region=\"${region}\",  namespace=\"${namespace}\"}[${window}]))", "").
				Exemplar(true).
				Interval("").
				RefId("A"))).
		WithPanel(ts("Metarange RangeLease", "").
			WithTarget(q("max by (pod_name)(buildbuddy_raft_leases{region=\"${region}\", namespace=\"${namespace}\", range_id=\"1\"})", "__auto").
				RefId("A"))).
		WithPanel(ts("Metarange Leader", "").
			WithTarget(q("max by (pod_name, replicaid)(dragonboat_raftnode_has_leader{namespace=\"${namespace}\",region=\"${region}\", shardid=\"1\"})", "__auto").
				RefId("A")))
}

func grpcMetadataServiceRow() *dashboard.RowBuilder {
	return row("gRPC (MetadataService)").
		WithPanel(ts("gRPC server handling duration, q=${quantile}", dash.UnitSeconds).
			Legend(common.NewVizLegendOptionsBuilder().
				DisplayMode(common.LegendDisplayModeTable).
				Placement(common.LegendPlacementBottom).
				ShowLegend(true).
				Calcs([]string{"lastNotNull"}).
				SortBy("Last *").
				SortDesc(true)).
			Tooltip(common.NewVizTooltipOptionsBuilder().
				Mode(common.TooltipDisplayModeMulti).
				Sort(common.SortOrderNone)).
			WithTarget(q("histogram_quantile(${quantile}, sum by (le, grpc_service, grpc_method) (rate(grpc_server_handling_seconds_bucket{region=\"${region}\", job=\"metadata-server\", grpc_service=\"metadata.service.MetadataService\", namespace=\"${namespace}\"}[${window}])))", "{{grpc_method}}").
				Exemplar(true).
				Interval("").
				RefId("A"))).
		WithPanel(ts("Handled gRPC requests per second by method", "").
			Legend(common.NewVizLegendOptionsBuilder().
				DisplayMode(common.LegendDisplayModeTable).
				Placement(common.LegendPlacementBottom).
				ShowLegend(true)).
			WithTarget(q("sum by (grpc_service, grpc_method) (rate(grpc_server_handled_total{job=\"metadata-server\", grpc_service=\"metadata.service.MetadataService\", namespace=\"${namespace}\"}[${window}]))", "{{grpc_service}}.{{grpc_method}}").
				RefId("A"))).
		WithPanel(ts("Handled gRPC requests per second by status", dash.UnitOps).
			WithTarget(q("sum by (grpc_code) (rate(grpc_server_handled_total{region=\"${region}\", grpc_service=\"metadata.service.MetadataService\", namespace=\"${namespace}\"}[${window}]))", "__auto").
				RefId("A")))
}

func grpcRaftServiceRow() *dashboard.RowBuilder {
	return row("gRPC (RaftService)").
		WithPanel(ts("gRPC server handling duration, q=${quantile}", dash.UnitSeconds).
			Legend(common.NewVizLegendOptionsBuilder().
				DisplayMode(common.LegendDisplayModeTable).
				Placement(common.LegendPlacementBottom).
				ShowLegend(true).
				Calcs([]string{"lastNotNull"}).
				SortBy("Last *").
				SortDesc(true)).
			Tooltip(common.NewVizTooltipOptionsBuilder().
				Mode(common.TooltipDisplayModeMulti).
				Sort(common.SortOrderNone)).
			WithTarget(q("histogram_quantile(${quantile}, sum by (le, grpc_service, grpc_method) (rate(grpc_server_handling_seconds_bucket{region=\"${region}\", job=\"metadata-server\", grpc_service=\"raft.service.Api\", namespace=\"${namespace}\"}[${window}])))", "{{grpc_method}}").
				Exemplar(true).
				Interval("").
				RefId("A"))).
		WithPanel(ts("Handled gRPC requests per second by method", "").
			Legend(common.NewVizLegendOptionsBuilder().
				DisplayMode(common.LegendDisplayModeTable).
				Placement(common.LegendPlacementBottom).
				ShowLegend(true)).
			WithTarget(q("sum by (grpc_service, grpc_method, pod_name) (rate(grpc_server_handled_total{job=\"metadata-server\", grpc_service=\"raft.service.Api\", namespace=\"${namespace}\"}[${window}]))", "{{grpc_service}}.{{grpc_method}} {{pod_name}}").
				RefId("A"))).
		WithPanel(ts("Handled gRPC requests per second by status", dash.UnitOps).
			WithTarget(q("sum by (grpc_code) (rate(grpc_server_handled_total{region=\"${region}\", grpc_service=\"raft.service.Api\", namespace=\"${namespace}\"}[${window}]))", "__auto").
				RefId("A")))
}

func latencyRow() *dashboard.RowBuilder {
	return row("Latency").
		WithPanel(ts("replica.Update latency (q=${quantile})", dash.UnitMicroseconds).
			Description("").
			WithTarget(q("histogram_quantile(${quantile}, sum(rate(buildbuddy_raft_replica_update_duration_usec_bucket{region=\"${region}\", namespace=\"${namespace}\"}[${window}])) by (le))", "__auto").
				RefId("A"))).
		WithPanel(ts("range lock latency (q=${quantile})", dash.UnitMilliseconds).
			Description("").
			WithTarget(q("histogram_quantile(${quantile}, sum(rate(buildbuddy_raft_range_lock_duration_msec_bucket{region=\"${region}\", namespace=\"${namespace}\"}[${window}])) by (pod_name, le))", "{{pod_name}}").
				RefId("A"))).
		WithPanel(ts("nodehost.SyncPropose latency (q=${quantile})", dash.UnitMicroseconds).
			Description("").
			WithTarget(q("histogram_quantile(${quantile}, sum(rate(buildbuddy_raft_nodehost_method_usec_bucket{region=\"${region}\", nodehost_method=\"SyncPropose\", namespace=\"${namespace}\"}[${window}])) by (pod_name, le))", "__auto").
				RefId("A"))).
		WithPanel(ts("nodehost.SyncPropose requests per sec by pod", "").
			Legend(common.NewVizLegendOptionsBuilder().
				DisplayMode(common.LegendDisplayModeTable).
				Placement(common.LegendPlacementBottom).
				ShowLegend(true)).
			WithTarget(q("sum by (pod_name) (rate(buildbuddy_raft_nodehost_method_usec_count{job=\"metadata-server\",nodehost_method=\"SyncPropose\", namespace=\"${namespace}\"}[${window}]))", "{{pod_name}}").
				RefId("A"))).
		WithPanel(ts("nodehost.SyncPropose errors", "").
			WithTarget(q("sum(increase(buildbuddy_raft_nodehost_method_errors{region=\"${region}\", job=\"metadata-server\", nodehost_method=\"SyncPropose\", namespace=\"${namespace}\"}[${window}])) by (dragonboat_error)", "__auto").
				RefId("A"))).
		WithPanel(ts("nodehost.SyncPropose requests per sec by range", "").
			Legend(common.NewVizLegendOptionsBuilder().
				DisplayMode(common.LegendDisplayModeTable).
				Placement(common.LegendPlacementBottom).
				ShowLegend(true)).
			WithTarget(q("avg(sum by (range_id) (rate(buildbuddy_raft_nodehost_method_usec_count{job=\"metadata-server\",nodehost_method=\"SyncPropose\", namespace=\"${namespace}\", range_id!=\"1\"}[${window}])))", "avg").
				RefId("A")).
			WithTarget(q("max(sum by (range_id) (rate(buildbuddy_raft_nodehost_method_usec_count{job=\"metadata-server\",nodehost_method=\"SyncPropose\", namespace=\"${namespace}\", range_id!=\"1\"}[${window}])))", "max").
				RefId("B")))
}

func partitionsRow() *dashboard.RowBuilder {
	return row("Partitions").
		WithPanel(ts("Partition Operations By Status", "").
			WithTarget(q("sum(increase(buildbuddy_raft_partition_operations{region=\"${region}\", namespace=\"${namespace}\"}[${window}]))by(op, status)", "{{op}}, {{status}}").
				RefId("A"))).
		WithPanel(ts("Number of Ranges Per Partition", "").
			WithTarget(q("count by (partition_id) (count by (partition_id, range_id) (buildbuddy_raft_range_replica{region=\"${region}\", namespace=\"${namespace}\"}))", "{{partition_id}}").
				RefId("A")))
}

func transactionsRow() *dashboard.RowBuilder {
	return row("Transactions").
		WithPanel(ts("Transaction Records", "").
			WithTarget(q("sum (increase(buildbuddy_raft_txn_record_process_count{region=\"${region}\", namespace=\"${namespace}\"}[${window}])) by (txn_record_process_status)", "__auto").
				RefId("A")))
}

func rangeAndLeasesRow() *dashboard.RowBuilder {
	return row("Range and Leases").
		WithPanel(ts("Raft Ranges", "").
			WithTarget(q("count(count by (range_id)(buildbuddy_raft_bytes{region=\"${region}\", namespace=\"${namespace}\"}) )", "total").
				RefId("B")).
			WithTarget(q("avg(buildbuddy_raft_ranges{region=\"${region}\", namespace=\"${namespace}\"}) by (node_host_id, pod_name)", "{{pod_name}}").
				Exemplar(true).
				Interval("").
				RefId("A"))).
		WithPanel(ts("Lease Count", "").
			WithTarget(q("sum(buildbuddy_raft_leases{region=\"${region}\", namespace=\"${namespace}\"}) by (node_host_id, pod_name)", "{{pod_name}} - {{node_host_id}}").
				RefId("A"))).
		WithPanel(ts("Number of ranges not in 3 zones", "").
			WithTarget(q("count(count by (range_id) (count by (range_id, zone) (buildbuddy_raft_range_replica{region=\"${region}\", namespace=\"${namespace}\"})) != 3) or vector(0)", "__auto").
				RefId("A"))).
		WithPanel(ts("Number of ranges w/o leases", "").
			WithTarget(q("sum (count_eq_over_time(max by (range_id) (buildbuddy_raft_leases{region=\"${region}\", namespace=\"${namespace}\"}), 0))", "__auto").
				RefId("A"))).
		WithPanel(ts("Range Sizes", "decmbytes").
			WithTarget(q("avg(buildbuddy_raft_bytes{region=\"${region}\", namespace=\"${namespace}\"})/1e6", "avg").
				Exemplar(true).
				Interval("").
				RefId("A")).
			WithTarget(q("max(buildbuddy_raft_bytes{region=\"${region}\", namespace=\"${namespace}\"})/1e6", "max").
				RefId("C")).
			WithTarget(q("avg(buildbuddy_raft_bytes{region=\"${region}\", namespace=\"${namespace}\"}/1e6) by (pod_name)", "{{pod_name}}").
				Exemplar(true).
				Interval("").
				RefId("B"))).
		WithPanel(ts("Lease Action Error", "").
			WithTarget(q("sum(increase(buildbuddy_raft_lease_action_count{region=\"${region}\", status!=\"OK\"}[${window}])) by (pod_name,lease_action, range_id)", "{{pod_name}} - {{lease_action}} - {{range_id}}").
				RefId("A"))).
		WithPanel(ts("Lease Acquisition latency (q=${quantile})", dash.UnitMilliseconds).
			Description("").
			WithTarget(q("histogram_quantile(${quantile}, sum(rate(buildbuddy_raft_lease_action_duration_msec_bucket{region=\"${region}\", namespace=\"${namespace}\", lease_action=\"Acquire\"}[${window}])) by (pod_name, le))", "{{pod_name}}").
				RefId("A")))
}

func splitsMovesRow() *dashboard.RowBuilder {
	return row("Splits & Moves").
		WithPanel(ts("Driver Actions", "").
			WithTarget(q("sum(increase(buildbuddy_raft_driver_actions{region=\"${region}\", namespace=\"${namespace}\"}[${window}])) by (driver_action)", "__auto").
				RefId("A"))).
		WithPanel(ts("Raft Moves", dash.UnitRequestsPerSec).
			WithTarget(q("sum(increase(buildbuddy_raft_moves{region=\"${region}\", namespace=\"${namespace}\"}[${window}])) by (move_type, status)", "{{move_type}}: {{status}}").
				Exemplar(true).
				Interval("").
				RefId("A"))).
		WithPanel(ts("Raft Splits", dash.UnitRequestsPerSec).
			WithTarget(q("sum (increase(buildbuddy_raft_splits{region=\"${region}\", namespace=\"${namespace}\"}[${window}])) by (pod_name, status)", "{{status}}: {{pod_name}}").
				Exemplar(true).
				Interval("").
				RefId("A"))).
		WithPanel(splitsMovesSplitDurationPanel()).
		WithPanel(ts("Intermediate Replicas", "").
			WithTarget(q("sum(buildbuddy_raft_intermediate_replicas_count{region=\"${region}\", namespace=\"${namespace}\"}) by (replica_state, range_id)", "__auto").
				RefId("A")))
}

func eventsRow() *dashboard.RowBuilder {
	return row("Events").
		WithPanel(ts("Raft Store Events Chan Size", "").
			WithTarget(q("sum by (pod_name) (buildbuddy_raft_store_events{region=\"${region}\", namespace=\"${namespace}\"})", "__auto").
				RefId("A"))).
		WithPanel(ts("Raft Store Broadcast Events Dropped ", "").
			WithTarget(q("sum by (pod_name, raft_event, event_broadcast_source) (buildbuddy_raft_store_event_broadcast_dropped{region=\"${region}\", namespace=\"${namespace}\"})", "__auto").
				RefId("A"))).
		WithPanel(ts("Raft Listener Events Dropped", "").
			WithTarget(q("sum by (listener_event, listener_id) (buildbuddy_raft_listener_events_dropped{region=\"${region}\", namespace=\"${namespace}\"})", "__auto").
				RefId("A"))).
		WithPanel(ts("Raft Store Listener Events Dropped ", "").
			WithTarget(q("sum by (raft_event, listener_id) (buildbuddy_raft_store_event_listener_dropped{region=\"${region}\", namespace=\"${namespace}\"})", "{{listener_id}}: {{raft_event}}").
				RefId("A")))
}

func zombiesRow() *dashboard.RowBuilder {
	return row("Zombies").
		WithPanel(ts("Number of Zombies", "").
			WithTarget(q("sum (buildbuddy_raft_zombie_cleanup_tasks{region=\"${region}\", namespace=\"${namespace}\"}) by (pod_name)", "__auto").
				RefId("A"))).
		WithPanel(ts("Zombie Cleanup Errors by Pod", "").
			WithTarget(q("sum(increase(buildbuddy_raft_zombie_cleanup{region=\"${region}\", namespace=\"${namespace}\"}[${window}])) by (pod_name,status)", "{{pod_name}} - {{status}}").
				RefId("A")))
}

func golangRow() *dashboard.RowBuilder {
	return row("Golang").
		WithPanel(ts("Heap size", dash.UnitBytes).
			WithTarget(q("sum (go_memstats_heap_alloc_bytes{region=\"${region}\", job=\"metadata-server\", namespace=\"${namespace}\"}) by (job, pod_name, namespace)", "{{pod_name}}").
				RefId("A"))).
		WithPanel(ts("Next GC heap size", dash.UnitDecimalBytes).
			Description("Size of the heap when the next GC will start").
			WithTarget(q("sum (go_memstats_next_gc_bytes{region=\"${region}\", job=\"metadata-server\"}) by (pod_name)", "{{pod_name}}").
				Interval("").
				QueryType("randomWalk").
				RefId("A"))).
		WithPanel(ts("Time since last GC", dash.UnitSeconds).
			Description("Time passed since the last GC finished. Smaller times indicate that the GC is running more frequently.").
			WithTarget(q("avg_over_time((time() - sum by (pod_name)(go_memstats_last_gc_time_seconds{region=\"${region}\", job=\"metadata-server\"}))[$__rate_interval:])", "{{job}} @ {{pod_name}}, {{namespace}}").
				Interval("").
				QueryType("randomWalk").
				RefId("A"))).
		WithPanel(ts("Median GC duration", dash.UnitSeconds).
			WithTarget(q("sum (go_gc_duration_seconds{region=\"${region}\", quantile=\"0.5\",job=\"metadata-server\"}) by (pod_name)", "{{pod_name}}").
				Interval("").
				QueryType("randomWalk").
				RefId("A"))).
		WithPanel(ts("goroutines", "").
			WithTarget(q("sum (go_goroutines{region=\"${region}\", job=\"metadata-server\"} ) by (pod_name)", "{{job}} @ {{pod_name}}, {{namespace}}").
				Interval("").
				QueryType("randomWalk").
				RefId("A"))).
		WithPanel(ts("OS threads", "").
			WithTarget(q("sum (go_threads{region=\"${region}\", job=\"metadata-server\"} ) by (pod_name)", "{{pod_name}}").
				Interval("").
				QueryType("randomWalk").
				RefId("A")))
}

func dragonboatRow() *dashboard.RowBuilder {
	return row("Dragonboat").
		WithPanel(ts("Num of Shards", "").
			WithTarget(q("count(max by (shardid)(dragonboat_raftnode_has_leader{namespace=\"${namespace}\",region=\"${region}\"}))", "Total number of shards w/ a leader").
				RefId("A"))).
		WithPanel(ts("Num of Shards w/o Leader", "").
			WithTarget(q("sum(count_eq_over_time(max by (shardid)(dragonboat_raftnode_has_leader{namespace=\"${namespace}\",region=\"${region}\"}), 0))", "Total number of shards w/o a leader").
				RefId("A"))).
		WithPanel(ts("Proposal Dropped", "").
			WithTarget(q("sum by (pod_name) (increase(dragonboat_raftnode_proposal_dropped_total{namespace=\"${namespace}\", region=\"${region}\"}[${window}]))", "__auto").
				RefId("A"))).
		WithPanel(ts("Number of shards with less than 3 shards", "").
			WithTarget(q("min_over_time(count( count by (shardid)(dragonboat_raftnode_has_leader{namespace=\"${namespace}\",region=\"${region}\"}) < 3)[5m:])", "__auto").
				RefId("A")))
}

func dragonboatTransportRow() *dashboard.RowBuilder {
	return row("Dragonboat Transport").
		WithPanel(ts("Failed Message Connections", dash.UnitShort).
			WithTarget(q("sum by (pod_name)(increase(dragonboat_transport_failed_message_connection_attempt_total{namespace=\"${namespace}\", region=\"${region}\"}[${window}]))", "{{pod_name}}").
				RefId("A"))).
		WithPanel(ts("Received Message Dropped", "").
			WithTarget(q("sum by (pod_name)(increase(dragonboat_transport_received_message_dropped_total{namespace=\"${namespace}\", region=\"${region}\"}[${window}]))", "{{pod_name}}").
				RefId("A"))).
		WithPanel(ts("Message Send Failure Rate", dash.UnitPercentUnit).
			WithTarget(q("sum by (pod_name)(increase(dragonboat_transport_message_send_failure_total{namespace=\"${namespace}\", region=\"${region}\"}[${window}]) / (increase(dragonboat_transport_message_send_failure_total{namespace=\"${namespace}\", region=\"${region}\"}[${window}]) + increase(dragonboat_transport_message_send_success_total{namespace=\"${namespace}\", region=\"${region}\"}[${window}])))", "{{pod_name}}").
				RefId("A"))).
		WithPanel(ts("Received Message", dash.UnitShort).
			WithTarget(q("sum by (pod_name)(increase(dragonboat_transport_received_message_total{namespace=\"${namespace}\", region=\"${region}\"}[${window}]))", "{{pod_name}}").
				RefId("A")))
}

func pebbleRow() *dashboard.RowBuilder {
	return row("Pebble").
		WithPanel(ts("block cache size", dash.UnitBytes).
			WithTarget(q("sum by (pod_name) (buildbuddy_remote_cache_pebble_cache_pebble_block_cache_size_bytes{region=\"${region}\", job=\"metadata-server\"})", "{{pod_name}}").
				RefId("A"))).
		WithPanel(pebbleBlockCacheHitsAndMissesPanel()).
		WithPanel(ts("Compaction rate  (by type)", "").
			WithTarget(q("sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_compact_count{region=\"${region}\", cache_name=\"raft\", namespace=\"${namespace}\"}[1m])) by (compaction_type)\n", "__auto").
				RefId("A"))).
		WithPanel(pebbleCompactionStatePanel()).
		WithPanel(pebbleCompactionEstimatedDebtPanel()).
		WithPanel(ts("Op Rate", dash.UnitOps).
			WithTarget(q("sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_op_count{region=\"${region}\", pebble_id=\"raft_store\", namespace=\"${namespace}\"}[1m])) by (pebble_op)", "__auto").
				RefId("A"))).
		WithPanel(ts("Op p50 Latency", dash.UnitMicroseconds).
			Height(9).
			WithTarget(q("histogram_quantile(0.50, sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_op_latency_usec_bucket{region=\"${region}\", pebble_id=\"raft_store\", namespace=\"${namespace}\"}[1m])) by (le,pebble_op))", "__auto").
				RefId("A"))).
		WithPanel(ts("Op p95 Latency", dash.UnitMicroseconds).
			Height(9).
			WithTarget(q("histogram_quantile(0.95, sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_op_latency_usec_bucket{region=\"${region}\", pebble_id=\"raft_store\", namespace=\"${namespace}\"}[1m])) by (le,pebble_op))", "__auto").
				RefId("A"))).
		WithPanel(ts("Op p99 Latency", dash.UnitMicroseconds).
			Height(9).
			WithTarget(q("histogram_quantile(0.99, sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_op_latency_usec_bucket{region=\"${region}\", pebble_id=\"raft_store\", namespace=\"${namespace}\"}[1m])) by (le,pebble_op))", "__auto").
				RefId("A"))).
		WithPanel(ts("Zombie Table Size", dash.UnitDecimalBytes).
			Height(9).
			WithTarget(q("sum(buildbuddy_remote_cache_pebble_cache_zombie_table_size_bytes{region=\"${region}\", namespace=\"${namespace}\"}) ", "__auto").
				RefId("A"))).
		WithPanel(ts("Zombie Table Count", "").
			Height(9).
			WithTarget(q("sum(buildbuddy_remote_cache_pebble_cache_zombie_table_count{region=\"${region}\", namespace=\"${namespace}\"}) ", "__auto").
				RefId("A")))
}

func pebbleLevelsRow() *dashboard.RowBuilder {
	return row("Pebble Levels").
		WithPanel(ts("Number files (by level)", "").
			WithTarget(q("sum(buildbuddy_remote_cache_pebble_cache_pebble_level_num_files{region=\"${region}\", cache_name=\"raft\", namespace=\"${namespace}\"}) by (level)", "{{level}}").
				RefId("A"))).
		WithPanel(ts("Size (by level)", dash.UnitBytes).
			WithTarget(q("sum(buildbuddy_remote_cache_pebble_cache_pebble_level_size_bytes{region=\"${region}\", cache_name=\"raft\", namespace=\"${namespace}\"}) by (level)", "{{level}}").
				RefId("A"))).
		WithPanel(ts("Compaction score (by level)", dash.UnitNone).
			WithTarget(q("sum(buildbuddy_remote_cache_pebble_cache_pebble_level_score{region=\"${region}\", cache_name=\"raft\", namespace=\"${namespace}\"}) by (level)", "{{level}}").
				RefId("A"))).
		WithPanel(ts("Bytes in (by level)", dash.UnitBytes).
			WithTarget(q("sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_level_bytes_in_count{region=\"${region}\", cache_name=\"raft\", namespace=\"${namespace}\"}[1m])) by (level)", "{{level}}").
				RefId("A"))).
		WithPanel(ts("Bytes ingested (by level)", dash.UnitBytes).
			WithTarget(q("sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_level_bytes_ingested_count{region=\"${region}\", cache_name=\"raft\", namespace=\"${namespace}\"}[1m])) by (level)", "{{level}}").
				RefId("A"))).
		WithPanel(ts("Bytes moved (by level)", dash.UnitBytes).
			WithTarget(q("sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_level_bytes_moved_count{region=\"${region}\", cache_name=\"raft\", namespace=\"${namespace}\"}[1m])) by (level)", "{{level}}").
				RefId("A"))).
		WithPanel(ts("Bytes read (by level)", dash.UnitBytes).
			WithTarget(q("sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_level_bytes_read_count{region=\"${region}\", cache_name=\"raft\", namespace=\"${namespace}\"}[1m])) by (level)", "{{level}}").
				RefId("A"))).
		WithPanel(ts("Bytes compacted (by level)", dash.UnitBytes).
			WithTarget(q("sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_level_bytes_compacted_count{region=\"${region}\", cache_name=\"raft\"}[1m])) by (level)", "{{level}}").
				RefId("A"))).
		WithPanel(ts("Bytes flushed (by level)", dash.UnitBytes).
			WithTarget(q("sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_level_bytes_flushed_count{region=\"${region}\", cache_name=\"raft\"}[1m])) by (level)", "{{level}}").
				RefId("A"))).
		WithPanel(ts("Tables compacted (by level)", dash.UnitNone).
			WithTarget(q("sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_level_tables_compacted_count{region=\"${region}\", cache_name=\"raft\", namespace=\"${namespace}\"}[1m])) by (level)", "{{level}}").
				RefId("A"))).
		WithPanel(ts("Tables flushed (by level)", dash.UnitNone).
			WithTarget(q("sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_level_tables_flushed_count{region=\"${region}\", cache_name=\"raft\", namespace=\"${namespace}\"}[1m])) by (level)", "{{level}}").
				RefId("A"))).
		WithPanel(ts("Tables ingested (by level)", dash.UnitNone).
			WithTarget(q("sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_level_tables_ingested_count{region=\"${region}\", cache_name=\"raft\", namespace=\"${namespace}\"}[1m])) by (level)", "{{level}}").
				RefId("A"))).
		WithPanel(ts("Tables moved (by level)", dash.UnitNone).
			WithTarget(q("sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_level_tables_moved_count{region=\"${region}\", cache_name=\"raft\", namespace=\"${namespace}\"}[1m])) by (level)", "{{level}}").
				RefId("A")))
}

func evictionRow() *dashboard.RowBuilder {
	return row("Eviction").
		WithPanel(ts("Disk Cache Filesystem Usage ", dash.UnitPercentUnit).
			Height(11).
			Legend(common.NewVizLegendOptionsBuilder().
				DisplayMode(common.LegendDisplayModeTable).
				Placement(common.LegendPlacementBottom).
				ShowLegend(true).
				Calcs([]string{"lastNotNull"}).
				SortBy("Last *").
				SortDesc(false)).
			Min(0).
			Max(1).
			WithTarget(q("max((buildbuddy_remote_cache_disk_cache_filesystem_total_bytes{region=\"${region}\", cache_name=\"raft\", namespace=\"${namespace}\"}-buildbuddy_remote_cache_disk_cache_filesystem_avail_bytes{region=\"${region}\",cache_name=\"raft\", namespace=\"${namespace}\"})/buildbuddy_remote_cache_disk_cache_filesystem_total_bytes{region=\"${region}\", cache_name=\"raft\", namespace=\"${namespace}\"}) by (pod_name)", "{{pod_name}}").
				Exemplar(true).
				Interval("").
				RefId("A"))).
		WithPanel(evictionDiskCachePartitionUsagePanel()).
		WithPanel(ts("Raft Eviction errors", "").
			Height(9).
			Legend(common.NewVizLegendOptionsBuilder().
				DisplayMode(common.LegendDisplayModeList).
				Placement(common.LegendPlacementBottom).
				ShowLegend(false)).
			WithTarget(q("sum(increase(buildbuddy_raft_eviction_errors{region=\"${region}\", namespace=\"${namespace}\"}[${window}]))", "__auto").
				RefId("A"))).
		WithPanel(ts("Eviction sample queue length", dash.UnitShort).
			Height(9).
			WithTarget(q("sum by (partition_id) (avg_over_time(buildbuddy_raft_eviction_samples_chan_size{region=\"${region}\", job=\"metadata-server\"}[${window}]))", "__auto").
				RefId("A"))).
		WithPanel(ts("Eviction resample latency", dash.UnitMicroseconds).
			Height(10).
			WithTarget(q("histogram_quantile(${quantile}, sum(rate(buildbuddy_remote_cache_pebble_cache_eviction_resample_latency_usec_bucket{region=\"${region}\", cache_name=\"raft\", namespace=\"${namespace}\"}[${window}])) by (le, partition_id))", "__auto").
				RefId("A"))).
		WithPanel(ts("Eviction evict latency", dash.UnitMicroseconds).
			Height(10).
			WithTarget(q("histogram_quantile(${quantile}, sum(rate(buildbuddy_remote_cache_pebble_cache_eviction_evict_latency_usec_bucket{region=\"${region}\", cache_name=\"raft\", namespace=\"${namespace}\"}[${window}])) by (le, partition_id))", "__auto").
				RefId("A"))).
		WithPanel(ts("Disk Cache Avg Last Evicted Age ", dash.UnitSeconds).
			Description("Avg age of last item evicted").
			Height(10).
			Tooltip(common.NewVizTooltipOptionsBuilder().
				Mode(common.TooltipDisplayModeMulti).
				Sort(common.SortOrderNone)).
			FillOpacity(10).
			ShowPoints(common.VisibilityModeNever).
			WithTarget(q("avg(buildbuddy_remote_cache_disk_cache_last_eviction_age_usec{region=\"${region}\", cache_name=\"raft\", namespace=\"${namespace}\"}/1e6) by (partition_id)", "").
				Interval("").
				QueryType("randomWalk").
				RefId("A"))).
		WithPanel(evictionEvictionAgePartitionIdPanel()).
		WithPanel(ts("Disk Cache eviction rate", "").
			Height(9).
			WithTarget(q("max(rate(buildbuddy_remote_cache_disk_cache_num_evictions{region=\"${region}\",cache_name=\"raft\", namespace=\"${namespace}\"}[10m])) by (pod_name, partition_id)", "{{partition_id}} {{pod_name}}").
				RefId("A"))).
		WithPanel(ts("batch delete latency", dash.UnitMicroseconds).
			Height(9).
			Legend(common.NewVizLegendOptionsBuilder().
				DisplayMode(common.LegendDisplayModeTable).
				Placement(common.LegendPlacementBottom).
				ShowLegend(true).
				Calcs([]string{"lastNotNull"}).
				SortBy("Last *").
				SortDesc(true)).
			Tooltip(common.NewVizTooltipOptionsBuilder().
				Mode(common.TooltipDisplayModeMulti).
				Sort(common.SortOrderNone)).
			WithTarget(q("histogram_quantile(0.99, sum by (le) (rate(buildbuddy_raft_batch_delete_usec_bucket{region=\"${region}\", job=\"metadata-server\", namespace=\"${namespace}\"}[${window}])))", "p99").
				Exemplar(true).
				Interval("").
				RefId("A")).
			WithTarget(q("histogram_quantile(0.95, sum by (le) (rate(buildbuddy_raft_batch_delete_usec_bucket{region=\"${region}\", job=\"metadata-server\", namespace=\"${namespace}\"}[${window}])))", "p95").
				Exemplar(true).
				Interval("").
				RefId("B")).
			WithTarget(q("histogram_quantile(0.5, sum by (le) (rate(buildbuddy_raft_batch_delete_usec_bucket{region=\"${region}\", job=\"metadata-server\", namespace=\"${namespace}\"}[${window}])))", "p50").
				Exemplar(true).
				Interval("").
				RefId("C"))).
		WithPanel(ts("# gcs deletes dropped", "").
			WithTarget(q("sum by (partition_id)(buildbuddy_raft_gcs_delete_dropped{region=\"${region}\", namespace=\"${namespace}\"})", "{{partition_id}}").
				RefId("A"))).
		WithPanel(ts("GCS Eviction queue length", dash.UnitShort).
			WithTarget(q("sum by (partition_id) (avg_over_time(buildbuddy_raft_eviction_gcs_chan_size{region=\"${region}\", job=\"metadata-server\"}[${window}]))", "__auto").
				RefId("A"))).
		WithPanel(ts("# gcs delete", "").
			WithTarget(q("sum by (status)(rate(buildbuddy_raft_gcs_eviction_count{region=\"${region}\", namespace=\"${namespace}\"}[${window}]))", "{{status}}").
				RefId("A")))
}

func atimeUpdateRow() *dashboard.RowBuilder {
	return row("ATime Update").
		WithPanel(ts("# gcs atime update", "").
			WithTarget(q("sum by (status)(rate(buildbuddy_raft_atime_update_gcs_count{region=\"${region}\", namespace=\"${namespace}\"}[${window}]))", "{{status}}").
				RefId("A"))).
		WithPanel(ts("batch atime update latency", dash.UnitMicroseconds).
			Legend(common.NewVizLegendOptionsBuilder().
				DisplayMode(common.LegendDisplayModeTable).
				Placement(common.LegendPlacementBottom).
				ShowLegend(true).
				Calcs([]string{"lastNotNull"}).
				SortBy("Last *").
				SortDesc(true)).
			Tooltip(common.NewVizTooltipOptionsBuilder().
				Mode(common.TooltipDisplayModeMulti).
				Sort(common.SortOrderNone)).
			WithTarget(q("histogram_quantile(0.99, sum by (le) (rate(buildbuddy_raft_batch_atime_update_usec_bucket{region=\"${region}\", job=\"metadata-server\", namespace=\"${namespace}\"}[${window}])))", "p99").
				Exemplar(true).
				Interval("").
				RefId("A")).
			WithTarget(q("histogram_quantile(0.95, sum by (le) (rate(buildbuddy_raft_batch_atime_update_usec_bucket{region=\"${region}\", job=\"metadata-server\", namespace=\"${namespace}\"}[${window}])))", "p95").
				RefId("B")).
			WithTarget(q("histogram_quantile(0.5, sum by (le) (rate(buildbuddy_raft_batch_atime_update_usec_bucket{region=\"${region}\", job=\"metadata-server\", namespace=\"${namespace}\"}[${window}])))", "p50").
				RefId("C")))
}

func regionVariable() *dashboard.QueryVariableBuilder {
	return dashboard.NewQueryVariableBuilder("region").
		Datasource(dash.Prometheus()).
		Query(dashboard.StringOrMap{Map: map[string]any{"query": "label_values(up,region)", "refId": "PrometheusVariableQueryEditor-VariableQuery"}}).
		Current(dash.SelectedOption("us-west1", "us-west1")).
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		Definition("label_values(up,region)")
}

func windowVariable() *dashboard.CustomVariableBuilder {
	return dashboard.NewCustomVariableBuilder("window").
		Values(dashboard.StringOrMap{String: new("30s, 1m, 5m, 10m, 15m, 30m, 1h, 2h, 4h, 8h, 16h, 1d, 2d, 5d, 7d, 14d, 30d")}).
		Current(dash.SelectedOption("30s", "30s")).
		Label("Averaging Window")
}

func quantileVariable() *dashboard.CustomVariableBuilder {
	return dashboard.NewCustomVariableBuilder("quantile").
		Values(dashboard.StringOrMap{String: new("0.5,0.75,0.9,0.95,0.99")}).
		Current(dash.SelectedOption("0.5", "0.5"))
}

func partitionIdVariable() *dashboard.QueryVariableBuilder {
	return dashboard.NewQueryVariableBuilder("partition_id").
		Datasource(dash.Prometheus()).
		Query(dashboard.StringOrMap{Map: map[string]any{"query": "label_values(buildbuddy_remote_cache_disk_cache_partition_capacity_bytes{namespace=\"${namespace}\"},partition_id)", "refId": "PrometheusVariableQueryEditor-VariableQuery"}}).
		Current(dash.SelectedOption("default", "default")).
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		Definition("label_values(buildbuddy_remote_cache_disk_cache_partition_capacity_bytes{namespace=\"${namespace}\"},partition_id)")
}

func namespaceVariable() *dashboard.QueryVariableBuilder {
	return dashboard.NewQueryVariableBuilder("namespace").
		Datasource(dash.Prometheus()).
		Query(dashboard.StringOrMap{Map: map[string]any{"query": "label_values(buildbuddy_raft_ranges,namespace)", "refId": "PrometheusVariableQueryEditor-VariableQuery"}}).
		Current(dash.SelectedOption("raft-dev", "raft-dev")).
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		Definition("label_values(buildbuddy_raft_ranges,namespace)")
}

func build() (dashboard.Dashboard, error) {
	return dashboard.NewDashboardBuilder("Raft").
		Uid("dbbffdbe-e2d2-4297-bd36-2544b51850ef").
		Tags([]string{"generated", "file:raft.json"}).
		Editable().
		Timezone("").
		Refresh("1m").
		Time("now-6h", "now").
		Timepicker(dashboard.NewTimePickerBuilder()).
		WithVariable(regionVariable()).
		WithVariable(windowVariable()).
		WithVariable(quantileVariable()).
		WithVariable(partitionIdVariable()).
		WithVariable(namespaceVariable()).
		WithRow(metaCacheRow()).
		WithRow(mdloadRow()).
		WithRow(metadataServerOverallRow()).
		WithRow(grpcMetadataServiceRow()).
		WithRow(grpcRaftServiceRow()).
		WithRow(latencyRow()).
		WithRow(partitionsRow()).
		WithRow(transactionsRow()).
		WithRow(rangeAndLeasesRow()).
		WithRow(splitsMovesRow()).
		WithRow(eventsRow()).
		WithRow(zombiesRow()).
		WithRow(golangRow()).
		WithRow(dragonboatRow()).
		WithRow(dragonboatTransportRow()).
		WithRow(pebbleRow()).
		WithRow(pebbleLevelsRow()).
		WithRow(evictionRow()).
		WithRow(atimeUpdateRow()).
		Build()
}

func main() {
	dash.MustMarshal(build())
}
