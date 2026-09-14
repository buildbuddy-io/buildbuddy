// Generates the Grafana dashboard for remote cache metrics.
package main

import (
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/dash"
	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/heatmap"
	"github.com/grafana/grafana-foundation-sdk/go/prometheus"
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

func evictionAgePanel() *heatmap.PanelBuilder {
	return heatmap.NewPanelBuilder().
		Title("Eviction Age (${cache_name})").
		Datasource(dash.Prometheus()).
		Repeat("cache_name").
		RepeatDirection(dashboard.PanelRepeatDirectionH).
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
			YHistogram(true)).
		YAxis(heatmap.NewYAxisConfigBuilder().
			AxisPlacement(common.AxisPlacementLeft).
			Reverse(false).
			Decimals(0).
			Unit(dash.UnitDurationMs)).
		ExemplarsColor("rgba(255,0,255,0.7)").
		HideLegend().
		Height(8).
		Span(24).
		WithTarget(q(`sum(increase(buildbuddy_remote_cache_disk_cache_eviction_age_msec_bucket{region="${region}", partition_id="${partition_id}", cache_name="${cache_name}"}[$__interval])) by (le)`, "{{le}}").
			Exemplar(true).
			Interval("").
			Format(prometheus.PromQueryFormatHeatmap).
			RefId("A"))
}

func keyFreshnessPanel() *heatmap.PanelBuilder {
	return heatmap.NewPanelBuilder().
		Title("Internal Key Freshness").
		Datasource(dash.Prometheus()).
		Calculate(false).
		CellGap(1).
		Color(heatmap.NewHeatmapColorOptionsBuilder().
			Mode(heatmap.HeatmapColorModeScheme).
			Scheme("Oranges").
			Fill("dark-orange").
			Scale(heatmap.HeatmapColorScaleExponential).
			Exponent(0.5).
			Steps(64).
			Reverse(false)).
		FilterValues(heatmap.NewFilterValueRangeBuilder().Le(1e-9)).
		RowsFrame(heatmap.NewRowsHeatmapOptionsBuilder().Layout(common.HeatmapCellLayoutAuto)).
		Tooltip(heatmap.NewHeatmapTooltipBuilder().
			Mode(common.TooltipDisplayModeSingle).
			YHistogram(false)).
		YAxis(heatmap.NewYAxisConfigBuilder().
			AxisPlacement(common.AxisPlacementLeft).
			Reverse(false).
			Unit(dash.UnitDurationMs)).
		ExemplarsColor("rgba(255,0,255,0.7)").
		ShowLegend().
		Height(8).
		Span(12).
		WithTarget(q(`sum(increase(buildbuddy_encryption_key_last_encryption_age_msec_bucket{region="${region}"}[6h])) by (le)`, "__auto").
			Format(prometheus.PromQueryFormatHeatmap).
			RefId("A"))
}

// partitionRow repeats once per partition. Each panel inside repeats
// horizontally across cache names, so panels span the full row and Grafana
// divides that width between the copies.
func partitionRow() *dashboard.RowBuilder {
	return row("Remote cache (partition: ${partition_id})").
		Repeat("partition_id").
		WithPanel(ts("Usage (${cache_name})", dash.UnitPercentUnit).
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			Span(24).
			Min(0).
			Legend(common.NewVizLegendOptionsBuilder().
				DisplayMode(common.LegendDisplayModeTable).
				Placement(common.LegendPlacementBottom).
				ShowLegend(true).
				Calcs([]string{"lastNotNull"})).
			WithTarget(q(`max(buildbuddy_remote_cache_disk_cache_partition_size_bytes{region="${region}",job="buildbuddy-app",partition_id="${partition_id}", cache_name="${cache_name}"}/buildbuddy_remote_cache_disk_cache_partition_capacity_bytes{region="${region}",job="buildbuddy-app",partition_id="${partition_id}", cache_name="${cache_name}"}) by (pod_name)`, "{{pod_name}}").
				Exemplar(true).
				Interval("").
				RefId("A"))).
		WithPanel(evictionAgePanel()).
		WithPanel(ts("Bytes evicted rate (${cache_name})", dash.UnitBinaryBytesPerSec).
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			Span(24).
			WithTarget(q(`sum by(pod_name) (deriv(buildbuddy_remote_cache_disk_cache_partition_size_bytes{region="${region}", cache_name="${cache_name}", partition_id="${partition_id}"}[${window}]))`, "total partition size delta {{pod_name}}").
				RefId("A")).
			WithTarget(q(`sum by(pod_name) (rate(buildbuddy_remote_cache_disk_cache_partition_size_bytes_evicted{region="${region}", cache_name="${cache_name}", partition_id="${partition_id}"}[${window}]))`, "bytes evicted {{pod_name}}").
				RefId("B")))
}

func encryptionRow() *dashboard.RowBuilder {
	qps := func(metric string) string {
		return fmt.Sprintf(`sum(rate(buildbuddy_encryption_%s{region="${region}"}[${window}]))`, metric)
	}
	return row("Remote cache encryption").
		WithPanel(ts("Encryption (QPS)", "").
			WithTarget(q(qps("encrypted_block_count"), "Blocks").RefId("A")).
			WithTarget(q(qps("encrypted_blob_count"), "Blobs").RefId("B"))).
		WithPanel(ts("Decryption (QPS)", "").
			WithTarget(q(qps("decrypted_block_count"), "Blocks").RefId("A")).
			WithTarget(q(qps("decrypted_blob_count"), "Blobs").RefId("B")).
			WithTarget(q(qps("decryption_error_count"), "Errors").RefId("C"))).
		WithPanel(ts("Key Refreshes (QPS)", "").
			WithTarget(q(qps("key_refresh_count"), "Attempts").RefId("A")).
			WithTarget(q(qps("key_refresh_failure_count"), "Errors").RefId("B"))).
		WithPanel(keyFreshnessPanel())
}

func migrationRow() *dashboard.RowBuilder {
	// migrationRate plots a migration counter's rate grouped by one label,
	// with the legend derived from that label so the two cannot drift apart.
	migrationRate := func(title, description, unit, metric, by string) *timeseries.PanelBuilder {
		return ts(title, unit).
			Description(description).
			Decimals(0).
			WithTarget(q(fmt.Sprintf(`sum by(%s)(rate(buildbuddy_remote_cache_migration_%s{region="${region}"}[${window}]))`, by, metric), "{{"+by+"}}").
				RefId("A"))
	}
	return row("Remote Cache Migration").
		WithPanel(migrationRate(
			"Bytes Copied Per Second",
			"Number of bytes copied from the source to destination cache during a cache migration.",
			dash.UnitBinaryBytesPerSec, "bytes_copied", "cache_type")).
		WithPanel(migrationRate(
			"Blobs Copied Per Second",
			"Number of blobs copied from the source to destination cache during a cache migration.",
			dash.UnitOps, "blobs_copied", "cache_type")).
		WithPanel(ts("Copy Queue Size", dash.UnitNone).
			Description("Number of digests queued to be copied during a cache migration.").
			Decimals(0).
			WithTarget(q(`sum by (pod_name) (buildbuddy_remote_cache_migration_copy_chan_size{region="${region}",namespace=~"buildbuddy-.*"})`, "{{pod_name}}").
				RefId("A"))).
		WithPanel(migrationRate(
			"Not Found Error Count Per Second",
			"Number of not found errors from the destination cache during a cache migration.",
			dash.UnitOps, "not_found_error_count", "type")).
		WithPanel(migrationRate(
			"Double Read Hit Count Per Second",
			"Number of double reads where the source and destination caches hold the same digests during a cache migration.",
			dash.UnitOps, "double_read_hit_count", "type")).
		WithPanel(ts("Percent of missing artifacts from the destination cache", dash.UnitPercentUnit).
			Description(`Percent of "important" artifacts (from reads) missing from the destination cache.`).
			Decimals(0).
			Max(1.1).
			Tooltip(common.NewVizTooltipOptionsBuilder().
				Mode(common.TooltipDisplayModeMulti).
				Sort(common.SortOrderDescending)).
			WithTarget(q(`sum by (type) (rate(buildbuddy_remote_cache_migration_not_found_error_count{region="${region}"}[${window}])) / (sum by (type) (rate(buildbuddy_remote_cache_migration_not_found_error_count{region="${region}"}[${window}])) + sum by (type) (rate(buildbuddy_remote_cache_migration_double_read_hit_count{region="${region}"}[${window}])))`, "{{type}}").
				RefId("A")))
}

func regionVariable() *dashboard.QueryVariableBuilder {
	query := `label_values(up, region)`
	return dash.QueryVar("region", query).
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		Current(dash.SelectedOption("us-west1", "us-west1")).
		Definition(query)
}

func windowVariable() *dashboard.CustomVariableBuilder {
	return dashboard.NewCustomVariableBuilder("window").
		Label("Averaging Window").
		Values(dashboard.StringOrMap{String: new("30s, 1m, 5m, 10m, 15m, 30m, 1h, 2h, 4h, 8h, 16h, 1d, 2d, 5d, 7d, 14d, 30d")}).
		Current(dash.SelectedOption("1m", "1m"))
}

// partitionLabelVariable returns an all-selectable variable over one label of
// the disk cache partition capacity metric.
func partitionLabelVariable(label string) *dashboard.QueryVariableBuilder {
	query := fmt.Sprintf(`label_values(buildbuddy_remote_cache_disk_cache_partition_capacity_bytes{region="${region}"}, %s)`, label)
	return dash.QueryVar(label, query).
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		IncludeAll(true).
		Sort(dashboard.VariableSortDisabled).
		Current(dash.SelectedOption("All", "$__all")).
		Definition(query)
}

func build() (dashboard.Dashboard, error) {
	return dashboard.NewDashboardBuilder("Remote Cache").
		Uid("9Zuc5O7Iz").
		Tags([]string{"generated", "file:cache.json"}).
		Editable().
		Timezone("").
		Refresh("1m").
		Time("now-3h", "now").
		Timepicker(dashboard.NewTimePickerBuilder()).
		WithVariable(regionVariable()).
		WithVariable(windowVariable()).
		WithVariable(partitionLabelVariable("partition_id")).
		WithVariable(partitionLabelVariable("cache_name")).
		WithRow(partitionRow()).
		WithRow(encryptionRow()).
		WithRow(migrationRow()).
		Build()
}

func main() {
	dash.MustMarshal(build())
}
