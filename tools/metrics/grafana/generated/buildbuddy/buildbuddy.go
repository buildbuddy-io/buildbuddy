// Generates the main "BuildBuddy Metrics" Grafana dashboard.
package main

import (
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/dash"
	"github.com/grafana/grafana-foundation-sdk/go/cog"
	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/heatmap"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
)

// row returns a collapsed row.
func row(title string) *dashboard.RowBuilder {
	return dashboard.NewRowBuilder(title).
		Collapsed(true)
}

// ts returns a timeseries panel with this dashboard's baseline style: thin
// lines with no fill, a list legend at the bottom, and a single-series
// tooltip. Panels default to half the grid wide and 8 units tall; override
// with Span/Height. An empty unit leaves the panel on Grafana's default unit.
func ts(title, unit string) *timeseries.PanelBuilder {
	p := timeseries.NewPanelBuilder().
		Title(title).
		Datasource(dash.Prometheus()).
		LineWidth(1).
		FillOpacity(0).
		GradientMode(common.GraphGradientModeNone).
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
		p.Unit(unit)
	}
	return p
}

// tableLegend returns a legend shown as a table at the bottom with the given
// summary calculations.
func tableLegend(calcs ...string) *common.VizLegendOptionsBuilder {
	return common.NewVizLegendOptionsBuilder().
		DisplayMode(common.LegendDisplayModeTable).
		Placement(common.LegendPlacementBottom).
		ShowLegend(true).
		Calcs(calcs)
}

func rightLegend() *common.VizLegendOptionsBuilder {
	return common.NewVizLegendOptionsBuilder().
		DisplayMode(common.LegendDisplayModeList).
		Placement(common.LegendPlacementRight).
		ShowLegend(true)
}

func hiddenLegend() *common.VizLegendOptionsBuilder {
	return common.NewVizLegendOptionsBuilder().
		DisplayMode(common.LegendDisplayModeList).
		Placement(common.LegendPlacementBottom).
		ShowLegend(false)
}

// multiTooltip returns a tooltip that lists every series under the cursor,
// sorted by value.
func multiTooltip() *common.VizTooltipOptionsBuilder {
	return common.NewVizTooltipOptionsBuilder().
		Mode(common.TooltipDisplayModeMulti).
		Sort(common.SortOrderDescending)
}

func multiTooltipUnsorted() *common.VizTooltipOptionsBuilder {
	return common.NewVizTooltipOptionsBuilder().
		Mode(common.TooltipDisplayModeMulti).
		Sort(common.SortOrderNone)
}

// colorProp pins a series to a fixed color; for use with OverrideByName.
func colorProp(color string) []dashboard.DynamicConfigValue {
	return []dashboard.DynamicConfigValue{
		{Id: "color", Value: map[string]any{"fixedColor": color, "mode": "fixed"}},
	}
}

// rightAxisProps moves a series to a separate right-hand axis with the given
// unit; for use with OverrideByName.
func rightAxisProps(unit string) []dashboard.DynamicConfigValue {
	return []dashboard.DynamicConfigValue{
		{Id: "custom.axisPlacement", Value: "right"},
		{Id: "unit", Value: unit},
	}
}

// yAxisLeft returns a left-placed heatmap y axis with the given unit. An
// empty unit leaves the axis on Grafana's default unit.
func yAxisLeft(unit string) *heatmap.YAxisConfigBuilder {
	b := heatmap.NewYAxisConfigBuilder().
		AxisPlacement(common.AxisPlacementLeft).
		Reverse(false)
	if unit != "" {
		b.Unit(unit)
	}
	return b
}

// schemeHeatmap returns a heatmap panel in the style shared by the PSI and
// task-size panels: bucket counts on an exponential "Oranges" color scheme.
// Callers set the y axis via YAxis(yAxisLeft(...)).
func schemeHeatmap(title string) *heatmap.PanelBuilder {
	return heatmap.NewPanelBuilder().
		Title(title).
		Datasource(dash.Prometheus()).
		Calculate(false).
		CellGap(1).
		Color(heatmap.NewHeatmapColorOptionsBuilder().
			Mode(heatmap.HeatmapColorModeScheme).
			Scheme("Oranges").
			Fill("dark-orange").
			Scale(heatmap.HeatmapColorScaleExponential).
			Exponent(0.5).
			Steps(64)).
		FilterValues(heatmap.NewFilterValueRangeBuilder().Le(1e-9)).
		RowsFrame(heatmap.NewRowsHeatmapOptionsBuilder().Layout(common.HeatmapCellLayoutAuto)).
		Tooltip(heatmap.NewHeatmapTooltipBuilder().
			Mode(common.TooltipDisplayModeSingle).
			ShowColorScale(false).
			YHistogram(false)).
		ExemplarsColor("rgba(255,0,255,0.7)").
		ShowLegend().
		Height(8).
		Span(12)
}

func systemStatusRow() *dashboard.RowBuilder {
	return row("System status").
		WithPanel(ts("${job} instances", "").
			Height(7).
			Span(24).
			Repeat("job").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			WithTarget(dash.PromQuery(`sum(up{region="${region}", job="${job}"})`, "Up").RefId("A")).
			WithTarget(dash.PromQuery(`sum(kube_pod_status_ready{region="${region}", pod=~"${job}-([0-9a-f]{8,10}-.*|[0-9]+)$"})`, "Ready").RefId("B"))).
		WithPanel(ts("${job} versions", "").
			Height(7).
			Span(24).
			Repeat("job").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			WithTarget(dash.PromQuery(`sum by (version, commit) (buildbuddy_version{region="${region}", job="${job}"})`, "{{version}} ({{commit}})"))).
		WithPanel(ts("Unexpected Restarts", "").
			WithTarget(dash.PromQuery(`sum(increase(kube_pod_container_status_restarts_total{region="${region}"}[1m])) by (pod, container, exported_namespace) > 0`, ""))).
		WithPanel(ts("Failing Health Checks", "").
			AxisSoftMax(0).
			WithTarget(dash.PromQuery(`sum(1 - (buildbuddy_health_check_status{region="${region}"} == 0)) by (pod_name, health_check_name)`, "")))
}

func probersRow() *dashboard.RowBuilder {
	return row("Probers").
		WithPanel(ts("Success Ratio", dash.UnitPercentUnit).
			Thresholds(dashboard.NewThresholdsConfigBuilder().
				Mode(dashboard.ThresholdsModeAbsolute).
				Steps([]dashboard.Threshold{{Color: "green"}, {Color: "red", Value: new(0.9)}})).
			AxisSoftMax(1).
			AxisSoftMin(0).
			ThresholdsStyle(common.NewGraphThresholdsStyleConfigBuilder().Mode(common.GraphThresholdsStyleModeDashed)).
			Legend(tableLegend("min").
				SortBy("Min")).
			WithTarget(dash.PromQuery(`sum(increase(cloudprober_success{region="${region}"}[10m])) by (probe) / sum(increase(cloudprober_total{region="${region}"}[10m])) by (probe)`, "{{probe}}"))).
		WithPanel(ts("RBE Prober Latency", dash.UnitMicroseconds).
			WithTarget(dash.PromQuery(`histogram_quantile(
  0.99,
  sum by (le, stage)
    (rate(buildbuddy_remote_execution_executed_action_metadata_durations_usec_bucket{region="${region}", job=~"executor.*", stage!="worker", group_id=~"GR16310856858823099217|GR13567963851860390298|GR11301939955034629488"}[1m]))
)`, ""))).
		WithPanel(ts("Prober op failures ($prober)", dash.UnitNone).
			Height(10).
			Span(24).
			Repeat("prober").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			Legend(hiddenLegend()).
			WithTarget(dash.PromQuery(`sum(rate(cloudprober_op_failure_count{region="${region}", probe="${prober}"}[$__rate_interval])) by (op, compressor)`, ""))).
		WithPanel(ts("Prober op p95 latency ($prober)", dash.UnitMicroseconds).
			Height(10).
			Span(24).
			Repeat("prober").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			Legend(hiddenLegend()).
			WithTarget(dash.PromQuery(`histogram_quantile(0.95, sum(rate(cloudprober_op_latency_usec_bucket{region="${region}", probe="${prober}"}[$__rate_interval])) by (le, op, compressor))`, "")))
}

func invocationsRow() *dashboard.RowBuilder {
	return row("Invocations").
		WithPanel(ts("Invocations per second (by status)", dash.UnitRequestsPerSec).
			WithTarget(dash.PromQuery(`sum by (invocation_status) (rate(buildbuddy_invocation_count{region="${region}"}[${window}]))`, "{{invocation_status}}"))).
		WithPanel(ts("Median invocation duration", dash.UnitMicroseconds).
			WithTarget(dash.PromQuery(`histogram_quantile(0.5, sum(rate(buildbuddy_invocation_duration_usec_bucket{region="${region}"}[${window}])) by (le))`, ""))).
		WithPanel(ts("Invocations per second (by bazel exit code)", dash.UnitRequestsPerSec).
			WithTarget(dash.PromQuery(`sum by (bazel_exit_code) (rate(buildbuddy_invocation_count{region="${region}"}[${window}]))`, "{{bazel_exit_code}}"))).
		WithPanel(ts("Build events uploaded per second", dash.UnitRequestsPerSec).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_invocation_build_event_count{region="${region}"}[${window}]))`, ""))).
		WithPanel(ts("Invocations per second (by Bazel version)", dash.UnitRequestsPerSec).
			WithTarget(dash.PromQuery(`sum by (bazel_version) (rate(buildbuddy_invocation_bazel_version{region="${region}"}[${window}]))`, "{{bazel_version}}")))
}

func invocationFinalizersRow() *dashboard.RowBuilder {
	return row("Invocation finalizers").
		WithPanel(ts("Stats recorder workers", "").
			Min(0).
			WithTarget(dash.PromQuery(`buildbuddy_invocation_stats_recorder_workers{region="${region}"}`, ""))).
		WithPanel(ts("Median stats recorder duration", dash.UnitMicroseconds).
			Min(0).
			Legend(hiddenLegend()).
			WithTarget(dash.PromQuery(`histogram_quantile(
    0.5,
    sum(rate(buildbuddy_invocation_stats_recorder_duration_usec_bucket{region="${region}"}[${window}])) by (le)
)`, ""))).
		WithPanel(ts("Webhook invocation lookup workers", "").
			Min(0).
			WithTarget(dash.PromQuery(`buildbuddy_invocation_webhook_invocation_lookup_workers{region="${region}"}`, ""))).
		WithPanel(ts("Median webhook invocation lookup duration", dash.UnitMicroseconds).
			Min(0).
			Legend(hiddenLegend()).
			WithTarget(dash.PromQuery(`histogram_quantile(
    0.5,
    sum(rate(buildbuddy_invocation_webhook_invocation_lookup_duration_usec_bucket{region="${region}"}[${window}])) by (le)
)`, ""))).
		WithPanel(ts("Webhook notification workers", "").
			Min(0).
			WithTarget(dash.PromQuery(`buildbuddy_invocation_webhook_notify_workers{region="${region}"}`, ""))).
		WithPanel(ts("Median webhook notify duration", dash.UnitMicroseconds).
			Min(0).
			Legend(hiddenLegend()).
			WithTarget(dash.PromQuery(`histogram_quantile(
    0.5,
    sum(rate(buildbuddy_invocation_webhook_notify_duration_usec_bucket{region="${region}"}[${window}])) by (le)
)`, "")))
}

func workflowsRow() *dashboard.RowBuilder {
	return row("Workflows").
		WithPanel(ts("Workflows started", dash.UnitRequestsPerSec).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_webhook_handler_workflows_started{region="${region}"}[${window}]))`, ""))).
		WithPanel(ts("Workflows started by trigger event", dash.UnitRequestsPerSec).
			WithTarget(dash.PromQuery(`sum by (event) (rate(buildbuddy_webhook_handler_workflows_started{region="${region}", event!=""}[${window}]))`, "{{event}}").RefId("A")).
			WithTarget(dash.PromQuery(`sum (rate(buildbuddy_webhook_handler_workflows_started{region="${region}", event=""}[${window}]))`, "rerun_button").RefId("B"))).
		WithPanel(ts("Webhook events by response code", dash.UnitRequestsPerSec).
			WithTarget(dash.PromQuery(`sum by (code) (rate(buildbuddy_http_request_handler_duration_usec_count{region="${region}",route="/webhooks/workflow/:id"}[${window}]))`, "{{code}}")))
}

func distributedCacheRow() *dashboard.RowBuilder {
	methodPanel := func(method string) *timeseries.PanelBuilder {
		filters := fmt.Sprintf(`region="${region}", job="buildbuddy-app", grpc_service="distributed_cache.DistributedCache", grpc_method="%s"`, method)
		return ts("/"+method, dash.UnitSeconds).
			AxisPlacement(common.AxisPlacementLeft).
			Legend(rightLegend()).
			Tooltip(multiTooltip()).
			OverrideByName("QPS", rightAxisProps(dash.UnitRequestsPerSec)).
			WithTarget(dash.PromQuery(`histogram_quantile(0.99, sum(rate(grpc_server_handling_seconds_bucket{`+filters+`}[${window}])) by (le))`, "P99").RefId("A")).
			WithTarget(dash.PromQuery(`histogram_quantile(0.95, sum(rate(grpc_server_handling_seconds_bucket{`+filters+`}[${window}])) by (le))`, "P95").RefId("B")).
			WithTarget(dash.PromQuery(`histogram_quantile(0.50, sum(rate(grpc_server_handling_seconds_bucket{`+filters+`}[${window}])) by (le))`, "P50").RefId("C")).
			WithTarget(dash.PromQuery(`sum(rate(grpc_server_handled_total{`+filters+`}[${window}])) by (grpc_service)`, "QPS").RefId("D"))
	}
	// transmissionPanel plots op rate against throughput, both broken down by
	// whether the payload moved as a reference to shared storage or as inline
	// bytes. Rates go on the left axis, bytes/sec on the right.
	transmissionPanel := func(title, description, typeLabel, countMetric, sizeMetric string) *timeseries.PanelBuilder {
		filters := `region="${region}", job="buildbuddy-app"`
		return ts(title, dash.UnitRequestsPerSec).
			Description(description).
			AxisPlacement(common.AxisPlacementLeft).
			Tooltip(multiTooltip()).
			OverrideByQuery("B", rightAxisProps(dash.UnitBytesPerSec)).
			WithTarget(dash.PromQuery(fmt.Sprintf(`sum by (%s) (rate(%s{%s}[${window}]))`, typeLabel, countMetric, filters), fmt.Sprintf("{{%s}} requests", typeLabel)).RefId("A")).
			WithTarget(dash.PromQuery(fmt.Sprintf(`sum by (%s) (rate(%s{%s}[${window}]))`, typeLabel, sizeMetric, filters), fmt.Sprintf("{{%s}} throughput", typeLabel)).RefId("B"))
	}
	return row("Distributed Cache").
		WithPanel(ts("Request Mix", "").
			WithTarget(dash.PromQuery(`sum(rate(grpc_server_started_total{region="${region}", job="buildbuddy-app",grpc_service="distributed_cache.DistributedCache"}[${window}])) by (grpc_method)`, "{{grpc_method}}"))).
		WithPanel(methodPanel("Metadata")).
		WithPanel(methodPanel("GetWithMetadata")).
		WithPanel(methodPanel("GetMulti")).
		WithPanel(methodPanel("FindMissing")).
		WithPanel(methodPanel("Write")).
		WithPanel(methodPanel("Read")).
		WithPanel(transmissionPanel(
			"Reads by Transmission Mechanism",
			"Distributed cache peer reads, split by whether the payload came back as a reference to shared storage or as inline bytes.",
			"response_type",
			"buildbuddy_remote_cache_distributed_cache_read_response_count",
			"buildbuddy_remote_cache_distributed_cache_read_response_size_bytes")).
		WithPanel(transmissionPanel(
			"Writes by Transmission Mechanism",
			"Distributed cache peer writes, split by whether the payload was sent as a reference to shared storage or as inline bytes.",
			"request_type",
			"buildbuddy_remote_cache_distributed_cache_write_request_count",
			"buildbuddy_remote_cache_distributed_cache_write_request_size_bytes")).
		WithPanel(ts("Read and Write Errors by Status", dash.UnitRequestsPerSec).
			Description("Distributed cache peer reads and writes that did not succeed. Writes deduped by the peer report \"AlreadyExists\" and are counted as successes, not errors.").
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`sum by (status, response_type) (rate(buildbuddy_remote_cache_distributed_cache_read_response_count{region="${region}", job="buildbuddy-app", status!="OK"}[${window}]))`, "read {{response_type}} {{status}}").RefId("A")).
			WithTarget(dash.PromQuery(`sum by (status, request_type) (rate(buildbuddy_remote_cache_distributed_cache_write_request_count{region="${region}", job="buildbuddy-app", status!~"OK|AlreadyExists"}[${window}]))`, "write {{request_type}} {{status}}").RefId("B"))).
		WithPanel(ts("Lookaside cache hits and misses", dash.UnitRequestsPerSec).
			AxisPlacement(common.AxisPlacementLeft).
			Legend(rightLegend()).
			Tooltip(multiTooltip()).
			OverrideByName("hit_ratio", rightAxisProps(dash.UnitPercentUnit)).
			WithTarget(dash.PromQuery(`sum by(status) (rate(buildbuddy_remote_cache_lookaside_cache_lookup_count{region="${region}", job="buildbuddy-app"}[${window}]))`, "").RefId("A")).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_cache_lookaside_cache_lookup_count{region="${region}", job="buildbuddy-app", status="hit"}[${window}])) / sum(rate(buildbuddy_remote_cache_lookaside_cache_lookup_count{region="${region}", job="buildbuddy-app"}[${window}]))`, "hit_ratio").RefId("B"))).
		WithPanel(ts("Lookaside cache eviction age by reason", dash.UnitMilliseconds).
			WithTarget(dash.PromQuery(`histogram_quantile(0.99, sum by (le, eviction_reason) (rate(buildbuddy_remote_cache_lookaside_cache_eviction_age_msec_bucket{region="${region}", job="buildbuddy-app"}[${window}])))`, "{{eviction_reason}} P99").RefId("A")).
			WithTarget(dash.PromQuery(`histogram_quantile(0.95, sum by (le, eviction_reason) (rate(buildbuddy_remote_cache_lookaside_cache_eviction_age_msec_bucket{region="${region}", job="buildbuddy-app"}[${window}])))`, "{{eviction_reason}} P95").RefId("B")).
			WithTarget(dash.PromQuery(`histogram_quantile(0.5, sum by (le, eviction_reason) (rate(buildbuddy_remote_cache_lookaside_cache_eviction_age_msec_bucket{region="${region}", job="buildbuddy-app"}[${window}])))`, "{{eviction_reason}} P50").RefId("C")).
			WithTarget(dash.PromQuery(`sum by(eviction_reason) (increase(buildbuddy_remote_cache_lookaside_cache_eviction_age_msec_sum{region="${region}", job="buildbuddy-app"}[${window}])) / sum by(eviction_reason) (increase(buildbuddy_remote_cache_lookaside_cache_eviction_age_msec_count{region="${region}", job="buildbuddy-app"}[${window}]))`, "{{eviction_reason}} avg").RefId("D"))).
		WithPanel(ts("Backfill count by status", dash.UnitRequestsPerSec).
			Description("The number of digests backfilled").
			AxisPlacement(common.AxisPlacementLeft).
			Legend(rightLegend()).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`sum by(status) (rate(buildbuddy_remote_cache_distributed_cache_backfill_latency_usec_count{region="${region}", job="buildbuddy-app"}[${window}]))`, ""))).
		WithPanel(ts("Successful backfill latency", dash.UnitMicroseconds).
			Thresholds(dashboard.NewThresholdsConfigBuilder().
				Mode(dashboard.ThresholdsModeAbsolute).
				Steps([]dashboard.Threshold{{Color: "green"}})).
			WithTarget(dash.PromQuery(`histogram_quantile(0.99, sum by (le, eviction_reason) (rate(buildbuddy_remote_cache_distributed_cache_backfill_latency_usec_bucket{region="${region}", job="buildbuddy-app", status="OK"}[${window}])))`, "P99").RefId("A")).
			WithTarget(dash.PromQuery(`histogram_quantile(0.95, sum by (le, eviction_reason) (rate(buildbuddy_remote_cache_distributed_cache_backfill_latency_usec_bucket{region="${region}", job="buildbuddy-app", status="OK"}[${window}])))`, "P95").RefId("B")).
			WithTarget(dash.PromQuery(`histogram_quantile(0.5, sum by (le, eviction_reason) (rate(buildbuddy_remote_cache_distributed_cache_backfill_latency_usec_bucket{region="${region}", job="buildbuddy-app", status="OK"}[${window}])))`, "P50").RefId("C")).
			WithTarget(dash.PromQuery(`sum (rate(buildbuddy_remote_cache_distributed_cache_backfill_latency_usec_sum{region="${region}", job="buildbuddy-app", status="OK"}[${window}])) / sum (rate(buildbuddy_remote_cache_distributed_cache_backfill_latency_usec_count{region="${region}", job="buildbuddy-app", status="OK"}[${window}]))`, "avg").RefId("D")))
}

func remoteCacheRow() *dashboard.RowBuilder {
	return row("Remote cache").
		WithPanel(ts("Download throughput", dash.UnitBinaryBytesPerSec).
			Description("Total number of bytes downloaded by consumers of the cache, per second. This does _not_ represent the average download speed across cache requests.").
			Min(0).
			FillOpacity(10).
			ShowPoints(common.VisibilityModeNever).
			Legend(hiddenLegend()).
			Tooltip(multiTooltipUnsorted()).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_cache_download_size_bytes_sum{region="${region}", job="buildbuddy-app"}[${window}]))`, ""))).
		WithPanel(ts("Upload throughput", dash.UnitBinaryBytesPerSec).
			Description("Total number of bytes uploaded by consumers of the cache, per second. This does _not_ represent the average upload speed across cache requests.").
			Min(0).
			FillOpacity(10).
			ShowPoints(common.VisibilityModeNever).
			Legend(hiddenLegend()).
			Tooltip(multiTooltipUnsorted()).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_cache_upload_size_bytes_sum{region="${region}", job="buildbuddy-app"}[${window}]))`, ""))).
		WithPanel(ts("Action cache", dash.UnitOps).
			Height(9).
			Min(0).
			ShowPoints(common.VisibilityModeNever).
			Legend(rightLegend()).
			Tooltip(multiTooltipUnsorted()).
			OverrideByName("Misses", colorProp("dark-red")).
			WithTarget(dash.PromQuery(`sum by (cache_event_type) (rate(buildbuddy_remote_cache_events{region="${region}", job="buildbuddy-app", cache_type="action_cache"}[1m]))`, "{{cache_event_type}}"))).
		WithPanel(ts("Content Addressable Store (CAS)", dash.UnitOps).
			Height(9).
			Min(0).
			ShowPoints(common.VisibilityModeNever).
			Legend(rightLegend()).
			Tooltip(multiTooltipUnsorted()).
			OverrideByName("Misses", colorProp("dark-red")).
			WithTarget(dash.PromQuery(`sum by (cache_event_type) (rate(buildbuddy_remote_cache_events{region="${region}", job="buildbuddy-app", cache_type="cas"}[${window}]))`, "{{cache_event_type}}"))).
		WithPanel(ts("Tree cache", dash.UnitOps).
			WithTarget(dash.PromQuery(`sum by (status) (rate(buildbuddy_remote_cache_tree_cache_lookup_count{region="${region}", job="buildbuddy-app"}[${window}]))`, ""))).
		WithPanel(ts("TreeCache by directory level", "").
			Legend(tableLegend("lastNotNull", "mean", "sum").
				SortBy("Total").
				SortDesc(true)).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`sum by (level, status) (rate(buildbuddy_remote_cache_tree_cache_lookup_count{region="${region}", job="buildbuddy-app"}[${window}]))`, "L{{level}}/{{status}}"))).
		WithPanel(ts("Cache Hit Rates", dash.UnitPercentUnit).
			Height(9).
			Span(24).
			OverrideByQuery("A", []dashboard.DynamicConfigValue{
				{Id: "displayName", Value: "TreeCache"},
			}).
			OverrideByQuery("B", []dashboard.DynamicConfigValue{
				{Id: "displayName", Value: "ActionCache"},
			}).
			OverrideByQuery("C", []dashboard.DynamicConfigValue{
				{Id: "displayName", Value: "CAS"},
			}).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_cache_tree_cache_lookup_count{region="${region}", job="buildbuddy-app", status="hit"}[${window}]))/sum(rate(buildbuddy_remote_cache_tree_cache_lookup_count{region="${region}", job="buildbuddy-app"}[${window}]))`, "").RefId("A")).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_cache_events{region="${region}", job="buildbuddy-app", cache_event_type="hit", cache_type="action_cache"}[${window}]))/sum(rate(buildbuddy_remote_cache_events{region="${region}", job="buildbuddy-app", cache_event_type=~"hit|miss", cache_type="action_cache"}[${window}]))`, "").RefId("B")).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_cache_events{region="${region}", job="buildbuddy-app", cache_event_type="hit", cache_type="cas"}[${window}]))/sum(rate(buildbuddy_remote_cache_events{region="${region}", job="buildbuddy-app", cache_event_type=~"hit|miss", cache_type="cas"}[${window}]))`, "").RefId("C"))).
		WithPanel(ts("Disk Cache Avg Last Evicted Age (${cache_name})", dash.UnitSeconds).
			Span(24).
			Description("Avg age of last item evicted by the disk cache").
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`avg(buildbuddy_remote_cache_disk_cache_last_eviction_age_usec{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}/1e6) by (partition_id)`, ""))).
		WithPanel(heatmap.NewPanelBuilder().
			Title("Files Added to Disk Cache by Size (${cache_name})").
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
				Steps(128)).
			FilterValues(heatmap.NewFilterValueRangeBuilder().Le(1e-9)).
			RowsFrame(heatmap.NewRowsHeatmapOptionsBuilder().Layout(common.HeatmapCellLayoutAuto)).
			ShowValue(common.VisibilityModeNever).
			Tooltip(heatmap.NewHeatmapTooltipBuilder().
				Mode(common.TooltipDisplayModeSingle).
				YHistogram(true)).
			YAxis(yAxisLeft(dash.UnitBytes).Decimals(0)).
			ExemplarsColor("rgba(255,0,255,0.7)").
			HideLegend().
			Height(8).
			Span(24).
			WithTarget(dash.PromHeatmapQuery(`sum(increase(buildbuddy_remote_cache_disk_cache_added_file_size_bytes_bucket{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[$__interval])) by (le)`))).
		WithPanel(ts("Disk Cache Filesystem Usage (${cache_name})", dash.UnitPercentUnit).
			Span(24).
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			Min(0).
			Max(1).
			Legend(tableLegend("lastNotNull").
				SortBy("Last *")).
			WithTarget(dash.PromQuery(`max((buildbuddy_remote_cache_disk_cache_filesystem_total_bytes{region="${region}",job="buildbuddy-app", cache_name="${cache_name}"}-buildbuddy_remote_cache_disk_cache_filesystem_avail_bytes{region="${region}",job="buildbuddy-app", cache_name="${cache_name}"})/buildbuddy_remote_cache_disk_cache_filesystem_total_bytes{region="${region}",job="buildbuddy-app", cache_name="${cache_name}"}) by (pod_name)`, "{{pod_name}}"))).
		WithPanel(ts("Disk Cache eviction rate  (${cache_name})", dash.UnitOps).
			Span(24).
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			WithTarget(dash.PromQuery(`max(rate(buildbuddy_remote_cache_disk_cache_num_evictions{region="${region}",job="buildbuddy-app", cache_name="${cache_name}"}[10m])) by (pod_name, partition_id)`, ""))).
		WithPanel(ts("Eviction resample latency (${cache_name})", dash.UnitMicroseconds).
			Span(24).
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			WithTarget(dash.PromQuery(`histogram_quantile(${quantile}, sum(rate(buildbuddy_remote_cache_pebble_cache_eviction_resample_latency_usec_bucket{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[${window}])) by (le, partition_id))`, ""))).
		WithPanel(ts("Eviction sample queue length (${cache_name})", dash.UnitShort).
			Span(24).
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_cache_pebble_cache_eviction_samples_chan_size{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[${window}]) by (partition_id)`, ""))).
		WithPanel(ts("Eviction samples by status (${cache_name})", dash.UnitOps).
			Span(24).
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_cache_pebble_cache_eviction_samples{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[${window}])) by (partition_id, status)`, ""))).
		WithPanel(ts("Eviction evict latency (${cache_name})", dash.UnitMicroseconds).
			Span(24).
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			WithTarget(dash.PromQuery(`histogram_quantile(${quantile}, sum(rate(buildbuddy_remote_cache_pebble_cache_eviction_evict_latency_usec_bucket{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[${window}])) by (le, partition_id))`, ""))).
		WithPanel(ts("Disk Cache Partition Usage (${cache_name})", dash.UnitPercentUnit).
			Span(24).
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			Min(0).
			Max(1).
			WithTarget(dash.PromQuery(`max(buildbuddy_remote_cache_disk_cache_partition_size_bytes{region="${region}",job="buildbuddy-app", cache_name="${cache_name}"}/buildbuddy_remote_cache_disk_cache_partition_capacity_bytes{region="${region}",job="buildbuddy-app", cache_name="${cache_name}"}) by (pod_name, partition_id)`, "{{pod_name}} {{partition_id}}")))
}

func pebbleRow() *dashboard.RowBuilder {
	return row("Remote cache pebble").
		WithPanel(ts("Compression Ratio", "").
			Span(24).
			WithTarget(dash.PromQuery(`1/histogram_quantile(0.1, sum(rate(buildbuddy_pebble_compression_ratio_bucket{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[10m])) by (le))`, "").RefId("A")).
			WithTarget(dash.PromQuery(`1/histogram_quantile(0.5, sum(rate(buildbuddy_pebble_compression_ratio_bucket{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[10m])) by (le))`, "").RefId("B")).
			WithTarget(dash.PromQuery(`1/histogram_quantile(0.99, sum(rate(buildbuddy_pebble_compression_ratio_bucket{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[10m])) by (le))`, "").RefId("C"))).
		WithPanel(ts("Compaction rate (${cache_name}) (by type)", "").
			Span(24).
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_compact_count{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[1m])) by (compaction_type)`, ""))).
		WithPanel(ts("Compaction state (${cache_name})", dash.UnitBytes).
			Span(24).
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			OverrideByName("in progress (count)", rightAxisProps(dash.UnitNone)).
			OverrideByName("marked files", rightAxisProps(dash.UnitNone)).
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_cache_pebble_cache_pebble_compact_in_progress_bytes{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"})`, "in progress (bytes)").RefId("B")).
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_cache_pebble_cache_pebble_compact_in_progress{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"})`, "in progress (count)").RefId("C")).
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_cache_pebble_cache_pebble_compact_marked_files{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"})`, "marked files").RefId("D"))).
		WithPanel(ts("Compaction estimated debt (${cache_name})", dash.UnitBytes).
			Span(24).
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			OverrideByName("in progress (count)", rightAxisProps(dash.UnitNone)).
			OverrideByName("marked files", rightAxisProps(dash.UnitNone)).
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_cache_pebble_cache_pebble_compact_estimated_debt_bytes{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}) by (pod_name)`, "{{pod_name}}"))).
		WithPanel(ts("Op Rate (${cache_name})", dash.UnitOps).
			Span(24).
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_op_count{region="${region}", job="buildbuddy-app", pebble_id="${cache_name}"}[1m])) by (pebble_op)`, ""))).
		WithPanel(ts("Op p50 Latency (${cache_name})", dash.UnitMicroseconds).
			Span(24).
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			WithTarget(dash.PromQuery(`histogram_quantile(0.50, sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_op_latency_usec_bucket{region="${region}", job="buildbuddy-app", pebble_id="${cache_name}"}[1m])) by (le,pebble_op))`, ""))).
		WithPanel(ts("Op p95 Latency (${cache_name})", dash.UnitMicroseconds).
			Span(24).
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			WithTarget(dash.PromQuery(`histogram_quantile(0.95, sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_op_latency_usec_bucket{region="${region}", job="buildbuddy-app", pebble_id="${cache_name}"}[1m])) by (le,pebble_op))`, ""))).
		WithPanel(ts("Op p99 Latency (${cache_name})", dash.UnitMicroseconds).
			Span(24).
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			WithTarget(dash.PromQuery(`histogram_quantile(0.99, sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_op_latency_usec_bucket{region="${region}", job="buildbuddy-app", pebble_id="${cache_name}"}[1m])) by (le,pebble_op))`, ""))).
		WithPanel(ts("block cache size (${cache_name})", dash.UnitBytes).
			Height(10).
			Span(24).
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			WithTarget(dash.PromQuery(`sum by (pod_name) (buildbuddy_remote_cache_pebble_cache_pebble_block_cache_size_bytes{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"})`, "{{pod_name}}"))).
		WithPanel(ts("block cache hits and misses (${cache_name})", dash.UnitRequestsPerSec).
			Height(10).
			Span(24).
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			AxisPlacement(common.AxisPlacementLeft).
			Legend(rightLegend()).
			Tooltip(multiTooltip()).
			OverrideByName("hit_ratio", rightAxisProps(dash.UnitPercentUnit)).
			WithTarget(dash.PromQuery(`sum by (cache_status) (rate(buildbuddy_remote_cache_pebble_cache_pebble_block_cache_requests_count{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[${window}]))`, "{{cache_status}}").RefId("A")).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_block_cache_requests_count{region="${region}", job="buildbuddy-app", cache_status="hit", cache_name="${cache_name}"}[${window}])) / sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_block_cache_requests_count{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[${window}]))`, "hit_ratio").RefId("B"))).
		WithPanel(ts("Zombie Table Count (${cache_name})", "").
			Height(10).
			Span(24).
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_cache_pebble_cache_zombie_table_count{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"})`, ""))).
		WithPanel(ts("Zombie Table Size ($cache_name)", dash.UnitDecimalBytes).
			Height(10).
			Span(24).
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_cache_pebble_cache_zombie_table_size_bytes{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"})`, "")))
}

func pebbleLevelsRow() *dashboard.RowBuilder {
	return row("Remote cache pebble levels (${cache_name})").
		Repeat("cache_name").
		WithPanel(ts("Number files (by level)", "").
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_cache_pebble_cache_pebble_level_num_files{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}) by (level)`, "{{level}}"))).
		WithPanel(ts("Size (by level)", dash.UnitBytes).
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_cache_pebble_cache_pebble_level_size_bytes{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}) by (level)`, "{{level}}"))).
		WithPanel(ts("Compaction score (by level)", dash.UnitNone).
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_cache_pebble_cache_pebble_level_score{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}) by (level)`, "{{level}}"))).
		WithPanel(ts("Bytes in (by level)", dash.UnitBytes).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_level_bytes_in_count{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[1m])) by (level)`, "{{level}}"))).
		WithPanel(ts("Bytes ingested (by level)", dash.UnitBytes).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_level_bytes_ingested_count{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[1m])) by (level)`, "{{level}}"))).
		WithPanel(ts("Bytes moved (by level)", dash.UnitBytes).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_level_bytes_moved_count{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[1m])) by (level)`, "{{level}}"))).
		WithPanel(ts("Bytes read (by level)", dash.UnitBytes).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_level_bytes_read_count{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[1m])) by (level)`, "{{level}}"))).
		WithPanel(ts("Bytes compacted (by level)", dash.UnitBytes).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_level_bytes_compacted_count{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[1m])) by (level)`, "{{level}}"))).
		WithPanel(ts("Bytes flushed (by level)", dash.UnitBytes).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_level_bytes_flushed_count{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[1m])) by (level)`, "{{level}}"))).
		WithPanel(ts("Tables compacted (by level)", dash.UnitNone).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_level_tables_compacted_count{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[1m])) by (level)`, "{{level}}"))).
		WithPanel(ts("Tables flushed (by level)", dash.UnitNone).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_level_tables_flushed_count{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[1m])) by (level)`, "{{level}}"))).
		WithPanel(ts("Tables ingested (by level)", dash.UnitNone).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_level_tables_ingested_count{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[1m])) by (level)`, "{{level}}"))).
		WithPanel(ts("Tables moved (by level)", dash.UnitNone).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_cache_pebble_cache_pebble_level_tables_moved_count{region="${region}", job="buildbuddy-app", cache_name="${cache_name}"}[1m])) by (level)`, "{{level}}")))
}

func sqlRow() *dashboard.RowBuilder {
	return row("SQL").
		WithPanel(ts("SQL queries per second (by query template)", dash.UnitOps).
			Height(13).
			Span(24).
			Min(0).
			FillOpacity(10).
			ShowPoints(common.VisibilityModeNever).
			Legend(tableLegend("lastNotNull").
				SortBy("Last *").
				SortDesc(true)).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`sum by (sql_query_template) (rate(buildbuddy_sql_query_count{region="${region}"}[${window}]))`, "{{sql_query_template}}"))).
		WithPanel(ts("SQL query duration by query template (q=${quantile})", dash.UnitMicroseconds).
			Height(14).
			Span(24).
			Min(0).
			ShowPoints(common.VisibilityModeNever).
			Legend(tableLegend("lastNotNull", "max", "mean").
				SortBy("Last *").
				SortDesc(true)).
			Tooltip(multiTooltip()).
			WithOverride(
				dashboard.MatcherConfig{Id: "byValue", Options: map[string]any{"op": "gte", "reducer": "allIsZero", "value": 0}},
				[]dashboard.DynamicConfigValue{
					{Id: "custom.hideFrom", Value: map[string]any{"legend": true, "tooltip": true, "viz": false}},
				},
			).
			WithOverride(
				dashboard.MatcherConfig{Id: "byValue", Options: map[string]any{"op": "gte", "reducer": "allIsNull", "value": 0}},
				[]dashboard.DynamicConfigValue{
					{Id: "custom.hideFrom", Value: map[string]any{"legend": true, "tooltip": true, "viz": false}},
				},
			).
			WithTarget(dash.PromQuery(`histogram_quantile(
  ${quantile},
  sum by (sql_query_template, le) (rate(buildbuddy_sql_query_duration_usec_bucket{region="${region}"}[${window}]))
) > 0`, "{{sql_query_template}}"))).
		WithPanel(ts("SQL queries per second", dash.UnitRequestsPerSec).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_sql_query_count{region="${region}"}[${window}]))`, ""))).
		WithPanel(ts("SQL error % (errors per second / queries per second)", dash.UnitPercentUnit).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_sql_error_count{region="${region}"}[${window}]))
/ (sum(rate(buildbuddy_sql_query_count{region="${region}"}[${window}])))`, ""))).
		WithPanel(ts("SQL errors per second", dash.UnitRequestsPerSec).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_sql_error_count{region="${region}"}[${window}]))`, "")))
}

func redisRow() *dashboard.RowBuilder {
	return row("Redis").
		WithPanel(ts("Memory usage", dash.UnitPercentUnit).
			Height(9).
			WithTarget(dash.PromQuery(`sum by(pod_name) (redis_memory_used_bytes{region="${region}"} / redis_memory_max_bytes{region="${region}"})`, "{{pod_name}}"))).
		WithPanel(ts("CPU Usage", dash.UnitPercentUnit).
			Height(9).
			WithTarget(dash.PromQuery(`sum by (pod_name) (rate(redis_cpu_user_seconds_total{region="${region}"} + redis_cpu_sys_seconds_total{region="${region}"}))`, "{{pod_name}}"))).
		WithPanel(ts("Total items", "").
			Height(9).
			WithTarget(dash.PromQuery(`sum by (pod_name) (redis_db_keys{region="${region}"})`, "{{pod_name}}"))).
		WithPanel(ts("Expiration Rate", "").
			Height(9).
			WithTarget(dash.PromQuery(`rate(sum by(pod_name) (redis_expired_keys_total{region="${region}"}))`, "{{pod_name}}"))).
		WithPanel(ts("Total number of clients", "").
			Height(9).
			Span(24).
			Min(0).
			Decimals(0).
			ShowPoints(common.VisibilityModeNever).
			Tooltip(multiTooltipUnsorted()).
			WithTarget(dash.PromQuery(`sum by(pod_name) (redis_connected_clients{region="${region}"})`, "{{pod_name}}"))).
		WithPanel(ts("Total commands per second", "").
			Height(10).
			Span(24).
			WithTarget(dash.PromQuery(`sum(rate(redis_commands_total{region="${region}"}[${window}])) by (cmd)`, "{{ cmd }}"))).
		WithPanel(ts("Average time spent by command per second", dash.UnitSeconds).
			Height(9).
			WithTarget(dash.PromQuery(`sum(irate(redis_commands_duration_seconds_total{region="${region}"}[${window}])) by (cmd)
  /
sum(irate(redis_commands_total{region="${region}"}[${window}])) by (cmd)`, "{{ cmd }}"))).
		WithPanel(ts("Total Time Spent by Command / sec", dash.UnitSeconds).
			Height(9).
			WithTarget(dash.PromQuery(`sum(irate(redis_commands_duration_seconds_total{region="${region}"}[${window}])) by (cmd) != 0`, "{{ cmd }}")))
}

func blobstoreRow() *dashboard.RowBuilder {
	return row("Blobstore").
		WithPanel(ts("Downloaded bytes per second", dash.UnitDecimalBytes).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_blobstore_read_size_bytes_sum{region="${region}"}[${window}]))`, ""))).
		WithPanel(ts("Median download duration", dash.UnitMicroseconds).
			WithTarget(dash.PromQuery(`histogram_quantile(0.5, sum(rate(buildbuddy_blobstore_read_duration_usec_bucket{region="${region}"}[${window}])) by (le))`, ""))).
		WithPanel(ts("Uploaded bytes per second", dash.UnitDecimalBytes).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_blobstore_write_size_bytes_sum{region="${region}"}[${window}]))`, ""))).
		WithPanel(ts("Median upload duration", dash.UnitMicroseconds).
			WithTarget(dash.PromQuery(`histogram_quantile(0.5, sum(rate(buildbuddy_blobstore_write_duration_usec_bucket{region="${region}"}[${window}])) by (le))`, "")))
}

func remoteExecutionRow() *dashboard.RowBuilder {
	return row("Remote execution").
		WithPanel(ts("Execution Request Rate", dash.UnitRequestsPerSec).
			FillOpacity(10).
			ShowPoints(common.VisibilityModeNever).
			Legend(tableLegend("lastNotNull", "mean").
				SortBy("Last *").
				SortDesc(true)).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_execution_requests{region="${region}"}[5m])) by (group_id, os, arch)`, "{{group_id}} ({{os}}_{{arch}})"))).
		WithPanel(ts("Num Execution Result Waiters (Sum Across Apps)", dash.UnitShort).
			FillOpacity(10).
			ShowPoints(common.VisibilityModeNever).
			Legend(tableLegend("lastNotNull", "mean").
				SortBy("Last *").
				SortDesc(true)).
			Tooltip(multiTooltipUnsorted()).
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_execution_waiting_execution_result{region="${region}"}) by (group_id)`, "{{group_id}}"))).
		WithPanel(ts("Num Execution Result Waiters (Max Across Apps)", dash.UnitShort).
			FillOpacity(10).
			ShowPoints(common.VisibilityModeNever).
			Legend(tableLegend("lastNotNull", "mean").
				SortBy("Last *").
				SortDesc(true)).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`max(sum(buildbuddy_remote_execution_waiting_execution_result{region="${region}"}) by (pod_name,group_id)) by (group_id)`, "{{group_id}}"))).
		WithPanel(ts("Remote execution SQL latency (q=${quantile})", dash.UnitMicroseconds).
			Legend(tableLegend("lastNotNull", "mean", "max").
				SortBy("Mean").
				SortDesc(true)).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`histogram_quantile(
    ${quantile},
    sum by (le, sql_query_template) (rate(buildbuddy_sql_query_duration_usec_bucket{region="${region}", sql_query_template=~"execution_server_.*"}[${window}]))
) > 0`, ""))).
		WithPanel(ts("Task sizer reads", dash.UnitRequestsPerSec).
			Min(0).
			Tooltip(multiTooltipUnsorted()).
			OverrideByName("hit", colorProp("green")).
			OverrideByName("error", colorProp("dark-red")).
			OverrideByName("miss", colorProp("dark-orange")).
			WithTarget(dash.PromQuery(`sum by (status) (rate(buildbuddy_remote_execution_task_size_read_requests{region="${region}"}[${window}]))`, "{{status}}"))).
		WithPanel(ts("Task sizer writes", dash.UnitRequestsPerSec).
			Min(0).
			Tooltip(multiTooltipUnsorted()).
			OverrideByName("hit", colorProp("green")).
			OverrideByName("error", colorProp("dark-red")).
			OverrideByName("miss", colorProp("dark-orange")).
			OverrideByName("ok", colorProp("green")).
			OverrideByName("missing_stats", colorProp("dark-orange")).
			WithTarget(dash.PromQuery(`sum by (status) (rate(buildbuddy_remote_execution_task_size_write_requests{region="${region}"}[${window}]))`, "{{status}}"))).
		WithPanel(schemeHeatmap("Enqueued task sizes (milli-CPU)").
			CellGap(0).
			CellValues(heatmap.NewCellValuesBuilder().Unit(" tasks enqueued")).
			YAxis(yAxisLeft("").Decimals(0)).
			WithTarget(dash.PromHeatmapQuery(`sum(increase(buildbuddy_remote_execution_enqueued_task_milli_cpu_bucket{region="${region}"}[${window}])) by (le)`))).
		WithPanel(schemeHeatmap("Enqueued task sizes (memory)").
			YAxis(yAxisLeft(dash.UnitBytes)).
			WithTarget(dash.PromHeatmapQuery(`sum(increase(buildbuddy_remote_execution_enqueued_task_memory_bytes_bucket{region="${region}"}[${window}])) by (le)`))).
		WithPanel(ts("Merged Actions", dash.UnitShort).
			FillOpacity(10).
			ShowPoints(common.VisibilityModeNever).
			Legend(tableLegend("lastNotNull", "mean").
				SortBy("Last *").
				SortDesc(true)).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_execution_merged_actions{region="${region}"}[${window}])) by (le)`, "Merged Actions per Second").RefId("A")).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_execution_hedged_actions{region="${region}"}[${window}])) by (le)`, "Hedged Executions per Second").RefId("B"))).
		WithPanel(ts("Action Merging Performance", dash.UnitShort).
			FillOpacity(10).
			ShowPoints(common.VisibilityModeNever).
			Legend(tableLegend("lastNotNull", "mean").
				SortBy("Last *").
				SortDesc(true)).
			Tooltip(multiTooltip()).
			OverrideByName("Merged Action Submit Time-Offset (p=0.50)", rightAxisProps(dash.UnitMicroseconds)).
			OverrideByName("Merged Action Submit Time-Offset (p=0.95)", rightAxisProps(dash.UnitMicroseconds)).
			OverrideByName("Merged Action Submit Time-Offset (p=0.99)", rightAxisProps(dash.UnitMicroseconds)).
			WithTarget(dash.PromQuery(`histogram_quantile(0.5, sum(rate(buildbuddy_remote_execution_merged_actions_per_execution_bucket{region="${region}"}[${window}])) by (le))`, "Merged Actions per Execution (p=0.50)").RefId("A")).
			WithTarget(dash.PromQuery(`histogram_quantile(0.95, sum(rate(buildbuddy_remote_execution_merged_actions_per_execution_bucket{region="${region}"}[${window}])) by (le))`, "Merged Actions per Execution (p=0.95)").RefId("B")).
			WithTarget(dash.PromQuery(`histogram_quantile(0.99, sum(rate(buildbuddy_remote_execution_merged_actions_per_execution_bucket{region="${region}"}[${window}])) by (le))`, "Merged Actions per Execution (p=0.99)").RefId("C")).
			WithTarget(dash.PromQuery(`histogram_quantile(0.5, sum(rate(buildbuddy_remote_execution_merged_action_submit_time_offset_usec_bucket{region="${region}"}[${window}])) by (le))`, "Merged Action Submit Time-Offset (p=0.50)").RefId("D")).
			WithTarget(dash.PromQuery(`histogram_quantile(0.95, sum(rate(buildbuddy_remote_execution_merged_action_submit_time_offset_usec_bucket{region="${region}"}[${window}])) by (le))`, "Merged Action Submit Time-Offset (p=0.95)").RefId("E")).
			WithTarget(dash.PromQuery(`histogram_quantile(0.99, sum(rate(buildbuddy_remote_execution_merged_action_submit_time_offset_usec_bucket{region="${region}"}[${window}])) by (le))`, "Merged Action Submit Time-Offset (p=0.99)").RefId("F")))
}

func httpRow() *dashboard.RowBuilder {
	return row("HTTP").
		WithPanel(ts("HTTP 5xx error ratio", "").
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_http_request_handler_duration_usec_count{region="${region}", code=~"5.."}[${window}]))
  /
sum(rate(buildbuddy_http_request_handler_duration_usec_count{region="${region}"}[${window}]))`, "Error ratio"))).
		WithPanel(ts("HTTP requests per second by route", "").
			WithTarget(dash.PromQuery(`sum by (route) (rate(buildbuddy_http_request_count{region="${region}"}[${window}]))`, "{{route}}"))).
		WithPanel(ts("HTTP requests per second by method", "").
			WithTarget(dash.PromQuery(`sum by (method) (rate(buildbuddy_http_request_count{region="${region}"}[${window}]))`, "{{method}}"))).
		WithPanel(ts("HTTP responses per second by status", "").
			WithTarget(dash.PromQuery(`sum by (code) (rate(buildbuddy_http_request_handler_duration_usec_count{region="${region}"}[${window}]))`, "{{code}}"))).
		WithPanel(ts("Median HTTP request handler duration (2xx responses only)", "").
			WithTarget(dash.PromQuery(`histogram_quantile(
  0.5,
  sum by (le, code) (rate(buildbuddy_http_request_handler_duration_usec_bucket{region="${region}", code=~"2.."}[${window}])))`, ""))).
		WithPanel(ts("Median HTTP response size", "").
			WithTarget(dash.PromQuery(`histogram_quantile(
  0.5,
  sum by (le) (rate(buildbuddy_http_response_size_bytes_bucket{region="${region}"}[${window}]))
)`, ""))).
		WithPanel(ts("HTTP outgoing requests per second", "").
			Legend(hiddenLegend()).
			WithTarget(dash.PromQuery(`sum (rate(buildbuddy_http_client_request_count{region="${region}"}[${window}]))`, "{{method}}"))).
		WithPanel(ts("HTTP outgoing bytes read", dash.UnitBytesPerSec).
			Legend(hiddenLegend()).
			WithTarget(dash.PromQuery(`sum (rate(buildbuddy_http_client_response_size_bytes_sum{region="${region}"}[${window}]))`, ""))).
		WithPanel(ts("HTTP outgoing requests per second by host", "").
			Legend(tableLegend("max").
				SortBy("Max").
				SortDesc(true)).
			WithTarget(dash.PromQuery(`sum by (host) (rate(buildbuddy_http_client_request_count{region="${region}"}[${window}]))`, "{{method}}"))).
		WithPanel(ts("HTTP outgoing bytes read by host", dash.UnitBytesPerSec).
			Legend(tableLegend("max").
				SortBy("Max").
				SortDesc(true)).
			WithTarget(dash.PromQuery(`sum by (host) (rate(buildbuddy_http_client_response_size_bytes_sum{region="${region}"}[${window}]))`, ""))).
		WithPanel(ts("HTTP outgoing requests per second by client", "").
			Legend(tableLegend("max").
				SortBy("Max").
				SortDesc(true)).
			WithTarget(dash.PromQuery(`sum by (client_name) (rate(buildbuddy_http_client_request_count{region="${region}"}[${window}]))`, "{{method}}"))).
		WithPanel(ts("HTTP outgoing bytes read by client", dash.UnitBytesPerSec).
			Legend(tableLegend("max").
				SortBy("Max").
				SortDesc(true)).
			WithTarget(dash.PromQuery(`sum by (client_name) (rate(buildbuddy_http_client_response_size_bytes_sum{region="${region}"}[${window}]))`, "")))
}

func executorPoolRow() *dashboard.RowBuilder {
	return row("Executor pool (${pool})").
		Repeat("pool").
		WithPanel(ts("Fraction of working executors", "").
			Description("Chart is stacked, so values always add up to 100%.").
			WithTarget(dash.PromQuery(`count(sum by (pod_name) (buildbuddy_remote_execution_tasks_executing{region="${region}", job="${pool}"}) > 0) / count(sum by (pod_name) (buildbuddy_remote_execution_tasks_executing{region="${region}", job="${pool}"}))`, "Working").RefId("A")).
			WithTarget(dash.PromQuery(`1 - count(sum by (pod_name) (buildbuddy_remote_execution_tasks_executing{region="${region}", job="${pool}"}) > 0) / count(sum by (pod_name) (buildbuddy_remote_execution_tasks_executing{region="${region}", job="${pool}"}))`, "Idle").RefId("B"))).
		WithPanel(ts("Executor autoscaling", dash.UnitShort).
			Min(0).
			ShowPoints(common.VisibilityModeNever).
			Tooltip(multiTooltipUnsorted()).
			OverrideByName("Avg queue length", []dashboard.DynamicConfigValue{
				{Id: "color", Value: map[string]any{"fixedColor": "orange", "mode": "fixed"}},
				{Id: "custom.lineWidth", Value: 3},
			}).
			OverrideByName("p25 queue length", colorProp("pink")).
			OverrideByName("p50 queue length", colorProp("dark-orange")).
			OverrideByName("p90 queue length", colorProp("red")).
			OverrideByName("p99 queue length", colorProp("dark-red")).
			OverrideByName("Pods running", []dashboard.DynamicConfigValue{
				{Id: "color", Value: map[string]any{"fixedColor": "blue", "mode": "fixed"}},
				{Id: "custom.lineWidth", Value: 3},
			}).
			OverrideByName("Autoscaler target", colorProp("light-blue")).
			OverrideByName("Tasks running", []dashboard.DynamicConfigValue{
				{Id: "color", Value: map[string]any{"fixedColor": "yellow", "mode": "fixed"}},
				{Id: "unit", Value: "tasks"},
			}).
			WithTarget(dash.PromQuery(`avg(sum by (pod_name) (buildbuddy_remote_execution_queue_length{region="${region}", job="${pool}"}))`, "Avg queue length").RefId("A")).
			WithTarget(dash.PromQuery(`quantile(
    0.25,
    sum by (pod_name) (buildbuddy_remote_execution_queue_length{region="${region}", job="${pool}"}))`, "p25 queue length").RefId("H")).
			WithTarget(dash.PromQuery(`quantile(
    0.5,
    sum by (pod_name) (buildbuddy_remote_execution_queue_length{region="${region}", job="${pool}"}))`, "p50 queue length").RefId("B")).
			WithTarget(dash.PromQuery(`quantile(
    0.9,
    sum by (pod_name) (buildbuddy_remote_execution_queue_length{region="${region}", job="${pool}"}))`, "p90 queue length").RefId("C")).
			WithTarget(dash.PromQuery(`quantile(
    0.99,
    sum by (pod_name) (buildbuddy_remote_execution_queue_length{region="${region}", job="${pool}"}))`, "p99 queue length").RefId("D")).
			WithTarget(dash.PromQuery(`sum(up{region="${region}", job="${pool}"})`, "Pods running").RefId("E")).
			WithTarget(dash.PromQuery(`kube_horizontalpodautoscaler_status_desired_replicas{region="${region}", horizontalpodautoscaler="${pool}-autoscaler"}`, "Autoscaler target").RefId("F")).
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_execution_tasks_executing{region="${region}", job="${pool}"})`, "Tasks running").RefId("G"))).
		WithPanel(ts("Action stage durations (quantile=${quantile})", dash.UnitMicroseconds).
			WithTarget(dash.PromQuery(`histogram_quantile(
  ${quantile},
  sum by (le, stage) (rate(buildbuddy_remote_execution_executed_action_metadata_durations_usec_bucket{region="${region}", stage!="worker", job="${pool}"}[${window}]))
)`, "{{stage}}"))).
		WithPanel(ts("Tasks executing by stage", "").
			Min(0).
			Tooltip(multiTooltipUnsorted()).
			WithTarget(dash.PromQuery(`sum by (stage) (buildbuddy_remote_execution_tasks_executing{region="${region}", job="${pool}"})`, "{{stage}}"))).
		WithPanel(ts("Actions executed per second", dash.UnitOps).
			Min(0).
			FillOpacity(10).
			ShowPoints(common.VisibilityModeNever).
			SpanNulls(common.BoolOrFloat64{Bool: new(true)}).
			Tooltip(multiTooltipUnsorted()).
			WithTarget(dash.PromQuery(`sum by (status) (rate(buildbuddy_remote_execution_count{region="${region}", job="${pool}"}[${window}]))`, "{{status}}"))).
		WithPanel(ts("Avg resources allocated to tasks", "").
			WithTarget(dash.PromQuery(`avg(buildbuddy_remote_execution_assigned_milli_cpu{region="${region}", job="${pool}"}
  / on (pod_name) (
  label_replace(kube_pod_container_resource_limits_cpu_cores{region="${region}", pod=~"${pool}-.*"} * 1000, "pod_name", "$1", "pod", "(.*)")))`, "Avg CPU assigned").RefId("A")).
			WithTarget(dash.PromQuery(`avg(buildbuddy_remote_execution_assigned_ram_bytes{region="${region}", job="${pool}"}
  / on (pod_name) (
  label_replace(kube_pod_container_resource_limits_memory_bytes{region="${region}", pod=~"${pool}-.*"}, "pod_name", "$1", "pod", "(.*)")))`, "Avg RAM assigned").RefId("B"))).
		WithPanel(ts("Node CPU usage", "cpu").
			Min(0).
			ShowPoints(common.VisibilityModeNever).
			Legend(tableLegend("mean", "lastNotNull").
				SortBy("Last *").
				SortDesc(true)).
			WithTarget(dash.PromQuery(`sum by (nodename, pod) (1 - rate(node_cpu_seconds_total{region="${region}", mode="idle"}[${window}]) * on (nodename) group_left(pod) (label_replace(kube_pod_info{region="${region}", pod=~"${pool}-[^-]+-[^-]+"}, "nodename", "$1", "node", "(.*)")))`, "{{pod}} (node: {{nodename}})"))).
		WithPanel(ts("Node CPU usage summary", "cpu").
			Min(0).
			ShowPoints(common.VisibilityModeNever).
			OverrideByName("mean", []dashboard.DynamicConfigValue{
				{Id: "custom.lineWidth", Value: 2},
			}).
			WithTarget(dash.PromQuery(`avg(sum by (nodename, pod) (1 - rate(node_cpu_seconds_total{region="${region}", mode="idle"}[${window}]) * on (nodename) group_left(pod) (label_replace(kube_pod_info{region="${region}", pod=~"${pool}-[^-]+-[^-]+"}, "nodename", "$1", "node", "(.*)"))))`, "mean").RefId("A")).
			WithTarget(dash.PromQuery(`quantile(0.1, sum by (nodename, pod) (1 - rate(node_cpu_seconds_total{region="${region}", mode="idle"}[${window}]) * on (nodename) group_left(pod) (label_replace(kube_pod_info{region="${region}", pod=~"${pool}-[^-]+-[^-]+"}, "nodename", "$1", "node", "(.*)"))))`, "p10").RefId("B")).
			WithTarget(dash.PromQuery(`quantile(0.5, sum by (nodename, pod) (1 - rate(node_cpu_seconds_total{region="${region}", mode="idle"}[${window}]) * on (nodename) group_left(pod) (label_replace(kube_pod_info{region="${region}", pod=~"${pool}-[^-]+-[^-]+"}, "nodename", "$1", "node", "(.*)"))))`, "p50").RefId("C")).
			WithTarget(dash.PromQuery(`quantile(0.9, sum by (nodename, pod) (1 - rate(node_cpu_seconds_total{region="${region}", mode="idle"}[${window}]) * on (nodename) group_left(pod) (label_replace(kube_pod_info{region="${region}", pod=~"${pool}-[^-]+-[^-]+"}, "nodename", "$1", "node", "(.*)"))))`, "p90").RefId("D"))).
		WithPanel(ts("Task memory usage", dash.UnitDecimalBytes).
			Tooltip(multiTooltipUnsorted()).
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_execution_memory_usage_bytes{region="${region}", job="${pool}"})`, "Current memory usage").RefId("A")).
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_execution_peak_memory_usage_bytes{region="${region}", job="${pool}"})`, "Current peak memory usage").RefId("C")).
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_execution_assigned_ram_bytes{region="${region}", job="${pool}"})`, "Estimated memory usage").RefId("B")).
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_execution_assignable_ram_bytes{region="${region}", job="${pool}"})`, "Max assignable memory").RefId("D"))).
		WithPanel(ts("Task CPU usage (each 100% = 1 fully utilized core)", dash.UnitPercentUnit).
			Tooltip(multiTooltipUnsorted()).
			WithTarget(dash.PromQuery(`sum(1 - rate(node_cpu_seconds_total{region="${region}", mode="idle"}[${window}]) * on (nodename) group_left(pod) (label_replace(kube_pod_info{region="${region}", pod=~"${pool}-[^-]+-[^-]+"}, "nodename", "$1", "node", "(.*)")))`, "Node CPU").RefId("A")).
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_execution_assigned_milli_cpu{region="${region}",job="${pool}"})/1000`, "Estimated CPU").RefId("C")).
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_execution_assignable_milli_cpu{region="${region}",job="${pool}"})/1000`, "Max assignable CPU").RefId("D"))).
		WithPanel(ts("Total downloaded bytes per second", dash.UnitDecimalBytes).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_execution_file_download_size_bytes_sum{region="${region}", job="${pool}"}[${window}]))`, ""))).
		WithPanel(ts("Total uploaded bytes per second", dash.UnitDecimalBytes).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_execution_file_upload_size_bytes_sum{region="${region}", job="${pool}"}[${window}]))`, ""))).
		WithPanel(ts("Executor queue length by pod", "").
			Description("Number of actions waiting to be executed").
			WithTarget(dash.PromQuery(`sum by (pod_name) (buildbuddy_remote_execution_queue_length{region="${region}", job="${pool}"})`, "{{pod_name}}"))).
		WithPanel(ts("Total CPU usage (CPU seconds)", "").
			WithTarget(dash.PromQuery(`sum(rate(container_cpu_usage_seconds_total{region="${region}", pod=~"${pool}-[a-f0-9].*"}[5m]))`, "{{pod}}"))).
		WithPanel(ts("File cache hit rate", dash.UnitPercentUnit).
			AxisSoftMax(1).
			AxisSoftMin(0).
			Tooltip(multiTooltipUnsorted()).
			OverrideByName("p25", colorProp("red")).
			OverrideByName("p50", colorProp("orange")).
			OverrideByName("p75", colorProp("green")).
			OverrideByName("Average", []dashboard.DynamicConfigValue{
				{Id: "color", Value: map[string]any{"fixedColor": "blue", "mode": "fixed"}},
				{Id: "custom.lineWidth", Value: 2},
			}).
			WithTarget(dash.PromQuery(`quantile(0.25,
sum by (pod_name) (rate(buildbuddy_remote_execution_file_cache_requests{status="hit", region="${region}", job="${pool}"}[${window}]))
  /
sum by (pod_name) (rate(buildbuddy_remote_execution_file_cache_requests{region="${region}", job="${pool}"}[${window}]))
)`, "p25").RefId("B")).
			WithTarget(dash.PromQuery(`quantile(0.50,
sum by (pod_name) (rate(buildbuddy_remote_execution_file_cache_requests{status="hit", region="${region}", job="${pool}"}[${window}]))
  /
sum by (pod_name) (rate(buildbuddy_remote_execution_file_cache_requests{region="${region}", job="${pool}"}[${window}]))
)`, "p50").RefId("C")).
			WithTarget(dash.PromQuery(`quantile(0.75,
sum by (pod_name) (rate(buildbuddy_remote_execution_file_cache_requests{status="hit", region="${region}", job="${pool}"}[${window}]))
  /
sum by (pod_name) (rate(buildbuddy_remote_execution_file_cache_requests{region="${region}", job="${pool}"}[${window}]))
)`, "p75").RefId("D")).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_execution_file_cache_requests{status="hit", region="${region}", job="${pool}"}[${window}]))
  /
sum(rate(buildbuddy_remote_execution_file_cache_requests{region="${region}", job="${pool}"}[${window}]))`, "Average").RefId("A"))).
		WithPanel(ts("File cache last eviction age", dash.UnitMicroseconds).
			Min(0).
			AxisSoftMin(0).
			Legend(tableLegend("lastNotNull")).
			Tooltip(multiTooltipUnsorted()).
			WithTarget(dash.PromQuery(`buildbuddy_remote_execution_file_cache_last_eviction_age_usec{region="${region}", job="${pool}"}`, ""))).
		WithPanel(ts("File cache added file size", dash.UnitDecimalBytes).
			Min(0).
			AxisSoftMin(0).
			Legend(tableLegend("lastNotNull")).
			Tooltip(multiTooltipUnsorted()).
			WithTarget(dash.PromQuery(`histogram_quantile(
  0.5,
  sum by (le) (rate(buildbuddy_remote_execution_file_cache_added_file_size_bytes_bucket{region="${region}", job="${pool}"}[${window}]))
)`, "p50").RefId("A")).
			WithTarget(dash.PromQuery(`histogram_quantile(
  0.9,
  sum by (le) (rate(buildbuddy_remote_execution_file_cache_added_file_size_bytes_bucket{region="${region}", job="${pool}"}[${window}]))
)`, "p90").RefId("B")).
			WithTarget(dash.PromQuery(`histogram_quantile(
  0.99,
  sum by (le) (rate(buildbuddy_remote_execution_file_cache_added_file_size_bytes_bucket{region="${region}", job="${pool}"}[${window}]))
)`, "p99").RefId("C")).
			WithTarget(dash.PromQuery(`histogram_quantile(
  0.9999,
  sum by (le) (rate(buildbuddy_remote_execution_file_cache_added_file_size_bytes_bucket{region="${region}", job="${pool}"}[${window}]))
)`, "p99.99").RefId("D"))).
		WithPanel(schemeHeatmap("PSI - cpu partial stall").
			YAxis(yAxisLeft(dash.UnitPercentUnit)).
			WithTarget(dash.PromHeatmapQuery(`sum by (le) (rate(buildbuddy_remote_execution_task_pressure_stall_duration_fraction_bucket{resource="cpu", stall_type="some", region="${region}", job="${pool}"}[${window}]))`))).
		WithPanel(schemeHeatmap("PSI - cpu full stall").
			YAxis(yAxisLeft(dash.UnitPercentUnit)).
			WithTarget(dash.PromHeatmapQuery(`sum by (le) (rate(buildbuddy_remote_execution_task_pressure_stall_duration_fraction_bucket{resource="cpu", stall_type="full", region="${region}", job="${pool}"}[${window}]))`))).
		WithPanel(schemeHeatmap("PSI - memory partial stall").
			YAxis(yAxisLeft(dash.UnitPercentUnit)).
			WithTarget(dash.PromHeatmapQuery(`sum by (le) (rate(buildbuddy_remote_execution_task_pressure_stall_duration_fraction_bucket{resource="memory", stall_type="some", region="${region}", job="${pool}"}[${window}]))`))).
		WithPanel(schemeHeatmap("PSI - memory full stall").
			YAxis(yAxisLeft(dash.UnitPercentUnit)).
			WithTarget(dash.PromHeatmapQuery(`sum by (le) (rate(buildbuddy_remote_execution_task_pressure_stall_duration_fraction_bucket{resource="memory", stall_type="full", region="${region}", job="${pool}"}[${window}]))`))).
		WithPanel(schemeHeatmap("PSI - io partial stall").
			YAxis(yAxisLeft(dash.UnitPercentUnit)).
			WithTarget(dash.PromHeatmapQuery(`sum by (le) (rate(buildbuddy_remote_execution_task_pressure_stall_duration_fraction_bucket{resource="io", stall_type="some", region="${region}", job="${pool}"}[${window}]))`))).
		WithPanel(schemeHeatmap("PSI - io full stall").
			YAxis(yAxisLeft(dash.UnitPercentUnit)).
			WithTarget(dash.PromHeatmapQuery(`sum by (le) (rate(buildbuddy_remote_execution_task_pressure_stall_duration_fraction_bucket{resource="io", stall_type="full", region="${region}", job="${pool}"}[${window}]))`))).
		WithPanel(ts("Pooled runner count", "").
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_execution_runner_pool_count{region="${region}", job="${pool}"})`, "Total").RefId("A")).
			WithTarget(dash.PromQuery(`avg(buildbuddy_remote_execution_runner_pool_count{region="${region}", job="${pool}"})`, "Average").RefId("B")).
			WithTarget(dash.PromQuery(`sum(up{region="${region}", job="${pool}"})`, "Executor count (for comparison)").RefId("C"))).
		WithPanel(ts("Runner pool evictions", dash.UnitRequestsPerSec).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_remote_execution_runner_pool_evictions{region="${region}", job="${pool}"}[${window}]))`, ""))).
		WithPanel(ts("Recycling failures by reason", dash.UnitRequestsPerSec).
			WithTarget(dash.PromQuery(`sum by (reason) (rate(buildbuddy_remote_execution_runner_pool_failed_recycle_attempts{region="${region}", job="${pool}"}[${window}]))`, "{{reason}}"))).
		WithPanel(ts("Runner pool requests by status", dash.UnitRequestsPerSec).
			WithTarget(dash.PromQuery(`sum by (status) (rate(buildbuddy_remote_execution_recycle_runner_requests{region="${region}", job="${pool}"}[${window}]))`, "{{status}}"))).
		WithPanel(ts("Runner pool total memory usage", dash.UnitDecimalBytes).
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_execution_runner_pool_memory_usage_bytes{region="${region}", job="${pool}"})`, ""))).
		WithPanel(ts("Runner pool total workspace size", dash.UnitDecimalBytes).
			WithTarget(dash.PromQuery(`sum(buildbuddy_remote_execution_runner_pool_disk_usage_bytes{region="${region}", job="${pool}"})`, "")))
}

func golangRow() *dashboard.RowBuilder {
	return row("golang (${job})").
		Repeat("job").
		WithPanel(ts("Heap size", dash.UnitDecimalBytes).
			Height(9).
			Description("Number of heap bytes allocated and still in use.").
			WithTarget(dash.PromQuery(`sum (go_memstats_heap_alloc_bytes{region="${region}", job="${job}"}) by (job, pod_name, namespace)`, "{{job}} @ {{pod_name}}, {{namespace}}"))).
		WithPanel(ts("Next GC heap size", dash.UnitDecimalBytes).
			Height(9).
			Description("Size of the heap when the next GC will start").
			WithTarget(dash.PromQuery(`sum (go_memstats_next_gc_bytes{region="${region}", job="${job}"}) by (job, pod_name, namespace)`, "{{job}} @ {{pod_name}}, {{namespace}}"))).
		WithPanel(ts("Time since last GC", dash.UnitSeconds).
			Height(9).
			Description("Time passed since the last GC finished. Smaller times indicate that the GC is running more frequently.").
			WithTarget(dash.PromQuery(`avg_over_time((time() - sum by (job, pod_name, namespace)(go_memstats_last_gc_time_seconds{region="${region}", job="${job}"}))[$__rate_interval])`, "{{job}} @ {{pod_name}}, {{namespace}}"))).
		WithPanel(ts("Median GC duration", dash.UnitSeconds).
			Height(9).
			WithTarget(dash.PromQuery(`sum (go_gc_duration_seconds{region="${region}", quantile="0.5",job="${job}"}) by (job, pod_name, namespace)`, "{{job}} @ {{pod_name}}, {{namespace}}"))).
		WithPanel(ts("goroutines", "").
			Height(9).
			WithTarget(dash.PromQuery(`sum (go_goroutines{region="${region}", job="${job}"} ) by (pod_name, job, namespace)`, "{{job}} @ {{pod_name}}, {{namespace}}"))).
		WithPanel(ts("OS threads", "").
			Height(9).
			WithTarget(dash.PromQuery(`sum (go_threads{region="${region}", job="${job}"} ) by (pod_name, job, namespace)`, "{{job}} @ {{pod_name}}, {{namespace}}")))
}

func grpcRow() *dashboard.RowBuilder {
	return row("gRPC (${job})").
		Repeat("job").
		WithPanel(ts("Handled gRPC requests per second by status", dash.UnitRequestsPerSec).
			Height(9).
			WithTarget(dash.PromQuery(`sum by (grpc_code) (rate(grpc_server_handled_total{region="${region}", job="${job}"}[${window}]))`, "{{grpc_code}}"))).
		WithPanel(ts("Handled gRPC requests per second by method", dash.UnitRequestsPerSec).
			Height(9).
			WithTarget(dash.PromQuery(`sum by (grpc_service, grpc_method) (rate(grpc_server_handled_total{region="${region}", job="${job}"}[${window}]))`, "/{{grpc_service}}/{{grpc_method}}"))).
		WithPanel(ts("gRPC server handling duration, q=${quantile}", dash.UnitSeconds).
			Legend(tableLegend("lastNotNull").
				SortBy("Last *").
				SortDesc(true)).
			Tooltip(multiTooltipUnsorted()).
			WithTarget(dash.PromQuery(`histogram_quantile(${quantile}, sum by (le, grpc_service, grpc_method) (rate(grpc_server_handling_seconds_bucket{region="${region}", job="${job}"})[$window]))`, "/{{grpc_service}}/{{grpc_method}}"))).
		WithPanel(ts("gRPC client messages sent by method", dash.UnitRequestsPerSec).
			FillOpacity(5).
			Legend(tableLegend("lastNotNull").
				SortBy("Last *").
				SortDesc(true)).
			Tooltip(multiTooltipUnsorted()).
			WithTarget(dash.PromQuery(`sum by (grpc_method, grpc_service) (rate(grpc_client_msg_sent_total{region="${region}", job="${job}"}[${window}]))`, "/{{grpc_service}}/{{grpc_method}}"))).
		WithPanel(ts("gRPC Client Request Bytes", dash.UnitBinaryBytesPerSec).
			Legend(tableLegend("lastNotNull").
				SortBy("Last *").
				SortDesc(true)).
			Tooltip(multiTooltipUnsorted()).
			WithTarget(dash.PromQuery(`sum by (rpc_service, rpc_method) (rate(rpc_client_request_size_bytes_sum{region="${region}", job="${job}"})[$window])`, "/{{rpc_service}}/{{rpc_method}}"))).
		WithPanel(ts("gRPC Client Response Bytes", dash.UnitBinaryBytesPerSec).
			Legend(tableLegend("lastNotNull").
				SortBy("Last *").
				SortDesc(true)).
			Tooltip(multiTooltipUnsorted()).
			WithTarget(dash.PromQuery(`sum by (rpc_service, rpc_method) (rate(rpc_client_response_size_bytes_sum{region="${region}", job="${job}"})[$window])`, "/{{rpc_service}}/{{rpc_method}}"))).
		WithPanel(ts("gRPC Server Request Bytes", dash.UnitBinaryBytesPerSec).
			Legend(tableLegend("lastNotNull").
				SortBy("Last *").
				SortDesc(true)).
			Tooltip(multiTooltipUnsorted()).
			WithTarget(dash.PromQuery(`sum by (rpc_service, rpc_method) (rate(rpc_server_request_size_bytes_sum{region="${region}", job="${job}"})[$window])`, "/{{rpc_service}}/{{rpc_method}}"))).
		WithPanel(ts("gRPC Server Response Bytes", dash.UnitBinaryBytesPerSec).
			Legend(tableLegend("lastNotNull").
				SortBy("Last *").
				SortDesc(true)).
			Tooltip(multiTooltipUnsorted()).
			WithTarget(dash.PromQuery(`sum by (rpc_service, rpc_method) (rate(rpc_server_response_size_bytes_sum{region="${region}", job="${job}"})[$window])`, "/{{rpc_service}}/{{rpc_method}}")))
}

func trafficStatsRow() *dashboard.RowBuilder {
	return row("Traffic Stats (buildbuddy-app)").
		WithPanel(ts("Egress by Provider", dash.UnitBinaryBytesPerSec).
			Description("Rate of gRPC server response bytes sent over the wire, by provider.").
			Min(0).
			Legend(tableLegend("lastNotNull", "mean").
				SortBy("Mean").
				SortDesc(true)).
			Tooltip(multiTooltip()).
			Links([]cog.Builder[dashboard.DashboardLink]{
				dashboard.NewDashboardLinkBuilder("Traffic Stats Dashboard").
					Url("/d/traffic-stats/traffic-stats").
					TargetBlank(true),
			}).
			WithTarget(dash.PromQuery(`sum by(provider) (rate(buildbuddy_grpc_server_egress_bytes{region="${region}", job="buildbuddy-app"}[$window]))`, "{{provider}}"))).
		WithPanel(ts("Ingress by Provider", dash.UnitBinaryBytesPerSec).
			Description("Rate of gRPC server request bytes received over the wire, by provider.").
			Min(0).
			Legend(tableLegend("lastNotNull", "mean").
				SortBy("Mean").
				SortDesc(true)).
			Tooltip(multiTooltip()).
			Links([]cog.Builder[dashboard.DashboardLink]{
				dashboard.NewDashboardLinkBuilder("Traffic Stats Dashboard").
					Url("/d/traffic-stats/traffic-stats").
					TargetBlank(true),
			}).
			WithTarget(dash.PromQuery(`sum by(provider) (rate(buildbuddy_grpc_server_ingress_bytes{region="${region}", job="buildbuddy-app"}[$window]))`, "{{provider}}")))
}

func appNodesRow() *dashboard.RowBuilder {
	return row("App Nodes Overview (${appnode})").
		WithPanel(ts("CPU", dash.UnitPercentUnit).
			Max(1).
			Legend(hiddenLegend()).
			WithTarget(dash.PromQuery(`1 - (avg by (mode, nodename) ((rate(node_cpu_seconds_total{region="${region}", mode="idle"}[1m])) * on(instance) group_left(nodename) (node_uname_info{region="${region}", nodename=~"^${appnode}"})))`, "{{nodename}}"))).
		WithPanel(ts("Disk", dash.UnitBytesPerSec).
			AxisSoftMax(100000000).
			Legend(hiddenLegend()).
			WithTarget(dash.PromQuery(`max by (nodename) (rate(node_disk_read_bytes_total{region="${region}"}[1m]) * on(instance) group_left(nodename) (node_uname_info{region="${region}", nodename=~"^${appnode}"}))`, "reads {{nodename}}").RefId("A")).
			WithTarget(dash.PromQuery(`max by (nodename) (rate(node_disk_written_bytes_total{region="${region}"}[1m]) * on(instance) group_left(nodename) (node_uname_info{region="${region}", nodename=~"^${appnode}"}))`, "writes {{nodename}}").RefId("B"))).
		WithPanel(ts("Network", dash.UnitBytesPerSec).
			AxisSoftMax(100000000).
			WithTarget(dash.PromQuery(`rate(node_network_receive_bytes_total{region="${region}", device=~"(ens|eth).*"}[1m]) * on(instance) group_left(nodename) (node_uname_info{region="${region}", nodename=~"^${appnode}"})`, "rx {{nodename}}").RefId("A")).
			WithTarget(dash.PromQuery(`rate(node_network_transmit_bytes_total{region="${region}", device=~"(ens|eth).*"}[1m]) * on(instance) group_left(nodename) (node_uname_info{region="${region}", nodename=~"^${appnode}"})`, "tx {{nodename}}").RefId("B")))
}

func executorNodesRow() *dashboard.RowBuilder {
	return row("Executor Nodes Overview (${executornode})").
		WithPanel(ts("CPU", dash.UnitPercentUnit).
			Max(1).
			Legend(hiddenLegend()).
			WithTarget(dash.PromQuery(`1 - (avg by (mode, nodename) ((rate(node_cpu_seconds_total{region="${region}", mode="idle"}[1m])) * on(instance) group_left(nodename) (node_uname_info{region="${region}", nodename=~"^${executornode}"})))`, "{{nodename}}"))).
		WithPanel(ts("Disk", dash.UnitBytesPerSec).
			AxisSoftMax(100000000).
			Legend(hiddenLegend()).
			WithTarget(dash.PromQuery(`max by (nodename) (rate(node_disk_read_bytes_total{region="${region}"}[1m]) * on(instance) group_left(nodename) (node_uname_info{region="${region}", nodename=~"^${executornode}"}))`, "reads {{nodename}}").RefId("A")).
			WithTarget(dash.PromQuery(`max by (nodename) (rate(node_disk_written_bytes_total{region="${region}"}[1m]) * on(instance) group_left(nodename) (node_uname_info{region="${region}", nodename=~"^${executornode}"}))`, "writes {{nodename}}").RefId("B"))).
		WithPanel(ts("Network", dash.UnitBytesPerSec).
			AxisSoftMax(100000000).
			WithTarget(dash.PromQuery(`rate(node_network_receive_bytes_total{region="${region}", device=~"(ens|eth).*"}[1m]) * on(instance) group_left(nodename) (node_uname_info{region="${region}", nodename=~"^${executornode}"})`, "rx {{nodename}}").RefId("A")).
			WithTarget(dash.PromQuery(`rate(node_network_transmit_bytes_total{region="${region}", device=~"(ens|eth).*"}[1m]) * on(instance) group_left(nodename) (node_uname_info{region="${region}", nodename=~"^${executornode}"})`, "tx {{nodename}}").RefId("B")))
}

func gkeNodepoolRow() *dashboard.RowBuilder {
	return row("GKE Nodepool Overview (${gkepool})").
		Repeat("gkepool").
		WithPanel(ts("CPU", dash.UnitPercentUnit).
			Max(1).
			Legend(hiddenLegend()).
			WithTarget(dash.PromQuery(`1 - (avg by (mode, nodename) ((rate(node_cpu_seconds_total{region="${region}", mode="idle"}[1m])) * on(instance) group_left(nodename) (node_uname_info{region="${region}", nodename=~"^${gkepool}-([0-9a-f]{8})-(grp-)?[0-9a-z]{4}$"})))`, "{{nodename}}"))).
		WithPanel(ts("Disk", dash.UnitBytesPerSec).
			AxisSoftMax(100000000).
			Legend(hiddenLegend()).
			WithTarget(dash.PromQuery(`max by (nodename) (rate(node_disk_read_bytes_total{region="${region}"}[1m]) * on(instance) group_left(nodename) (node_uname_info{region="${region}", nodename=~"^${gkepool}-([0-9a-f]{8})-(grp-)?[0-9a-z]{4}$"}))`, "reads {{nodename}}").RefId("A")).
			WithTarget(dash.PromQuery(`max by (nodename) (rate(node_disk_written_bytes_total{region="${region}"}[1m]) * on(instance) group_left(nodename) (node_uname_info{region="${region}", nodename=~"^${gkepool}-([0-9a-f]{8})-(grp-)?[0-9a-z]{4}$"}))`, "writes {{nodename}}").RefId("B"))).
		WithPanel(ts("Network", dash.UnitBytesPerSec).
			AxisSoftMax(100000000).
			WithTarget(dash.PromQuery(`rate(node_network_receive_bytes_total{region="${region}", device=~"(ens|eth).*"}[1m]) * on(instance) group_left(nodename) (node_uname_info{region="${region}", nodename=~"^${gkepool}-([0-9a-f]{8})-(grp-)?[0-9a-z]{4}$"})`, "rx {{nodename}}").RefId("A")).
			WithTarget(dash.PromQuery(`rate(node_network_transmit_bytes_total{region="${region}", device=~"(ens|eth).*"}[1m]) * on(instance) group_left(nodename) (node_uname_info{region="${region}", nodename=~"^${gkepool}-([0-9a-f]{8})-(grp-)?[0-9a-z]{4}$"})`, "tx {{nodename}}").RefId("B")))
}

func internalRow() *dashboard.RowBuilder {
	return row("Internal").
		WithPanel(ts("Build event handler duration", dash.UnitMicroseconds).
			Height(10).
			Description("Measures the time spent handling build events. In a healthy state, this should be very small (on the order of microseconds) in most cases, since the build event handler needs to be very high throughput.").
			WithTarget(dash.PromQuery(`histogram_quantile(0.5, sum(rate(buildbuddy_build_event_handler_duration_usec_bucket{region="${region}"}[${window}])) by (le))`, "Median").RefId("A")).
			WithTarget(dash.PromQuery(`histogram_quantile(0.90, sum(rate(buildbuddy_build_event_handler_duration_usec_bucket{region="${region}"}[${window}])) by (le))`, "90th %").RefId("B")).
			WithTarget(dash.PromQuery(`histogram_quantile(0.95, sum(rate(buildbuddy_build_event_handler_duration_usec_bucket{region="${region}"}[${window}])) by (le))`, "95th %").RefId("C"))).
		WithPanel(ts("Unexpected events", dash.UnitOps).
			Height(10).
			WithTarget(dash.PromQuery(`sum by (name) (rate(buildbuddy_unexpected_event{region="${region}"}[${window}]))`, ""))).
		WithPanel(ts("Download throughput by group", dash.UnitBinaryBytesPerSec).
			Description("Total number of bytes downloaded by consumers of the cache, per second, broken down by group ID.").
			Min(0).
			FillOpacity(10).
			ShowPoints(common.VisibilityModeNever).
			Legend(rightLegend()).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`sum by (group_id) (rate(buildbuddy_remote_cache_download_size_bytes_sum{region="${region}", job="buildbuddy-app", group_id!=""}[${window}]))`, "{{group_id}}"))).
		WithPanel(ts("Upload throughput by group", dash.UnitBinaryBytesPerSec).
			Description("Total number of bytes uploaded by consumers of the cache, per second, broken down by group ID.").
			Min(0).
			FillOpacity(10).
			ShowPoints(common.VisibilityModeNever).
			Legend(rightLegend()).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`sum by (group_id) (rate(buildbuddy_remote_cache_upload_size_bytes_sum{region="${region}", job="buildbuddy-app", group_id!=""}[${window}]))`, "{{group_id}}")))
}

func clickhouseRow() *dashboard.RowBuilder {
	return row("ClickHouse").
		WithPanel(ts("ClickHouse SQL queries per second (by query template)", dash.UnitRequestsPerSec).
			Height(13).
			Span(24).
			WithTarget(dash.PromQuery(`sum by (sql_query_template) (rate(buildbuddy_clickhouse_query_count{region="${region}"}[${window}]))`, "{{sql_query_template}}"))).
		WithPanel(ts("Median ClickHouse SQL query duration by query template", dash.UnitMicroseconds).
			Height(14).
			Span(24).
			WithTarget(dash.PromQuery(`histogram_quantile(
  0.5,
  sum by (sql_query_template, le) (rate(buildbuddy_clickhouse_query_duration_usec_bucket{region="${region}"}[${window}]))
)`, "{{sql_query_template}}"))).
		WithPanel(ts("ClickHouse SQL errors per second by query template", dash.UnitRequestsPerSec).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`sum by(sql_query_template) (rate(buildbuddy_clickhouse_error_count{region="${region}"}[${window}]))`, ""))).
		WithPanel(ts("ClickHouse SQL error % (errors per second / queries per second) by query template", dash.UnitPercentUnit).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`sum by(sql_query_template) (rate(buildbuddy_clickhouse_error_count{region="${region}"}[${window}]))
/ (sum by(sql_query_template) (rate(buildbuddy_clickhouse_query_count{region="${region}"}[${window}])))`, ""))).
		WithPanel(ts("ClickHouse inserted rows by table", dash.UnitOps).
			Description("The number of rows Buildbuddy app wrote to clickhouse. This can be higher than the number of insert operations.").
			Tooltip(multiTooltip()).
			Links([]cog.Builder[dashboard.DashboardLink]{
				dashboard.NewDashboardLinkBuilder("Detailed Graphs on Clickhouse").
					Url("/d/clickhouse-operator/clickhouse-operator-dashboard"),
			}).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_clickhouse_insert_count{region="${region}"}[${window}])) by (clickhouse_table_name)`, "{{clickhouse_table_name}}"))).
		WithPanel(ts("ClickHouse row insert error ratio by table", dash.UnitPercentUnit).
			Description("The number of rows that failed to be inserted to clickhouse divided by the total number of rows that we attempted to write.").
			Tooltip(multiTooltip()).
			Links([]cog.Builder[dashboard.DashboardLink]{
				dashboard.NewDashboardLinkBuilder("Detailed Graphs on Clickhouse").
					Url("/d/clickhouse-operator/clickhouse-operator-dashboard"),
			}).
			WithTarget(dash.PromQuery(`sum by(clickhouse_table_name) (rate(buildbuddy_clickhouse_insert_count{region="${region}", status!="ok"}[${window}])) / sum by(clickhouse_table_name) (rate(buildbuddy_clickhouse_insert_count{region="${region}"}[${window}]))`, "{{clickhouse_table_name}}")))
}

func victoriaMetricsRow() *dashboard.RowBuilder {
	return row("VictoriaMetrics").
		WithPanel(ts("vmagent (global) - scrape download rate", dash.UnitBytesPerSec).
			Min(0).
			FillOpacity(10).
			Tooltip(multiTooltipUnsorted()).
			WithTarget(dash.PromQuery(`sum(rate(vm_promscrape_scrape_response_size_bytes_sum{region="${region}", app_kubernetes_io_instance="victoria-metrics-agent-global"}[${window}]))`, ""))).
		WithPanel(ts("vmagent (global) - remote write upload rate", dash.UnitBytesPerSec).
			Min(0).
			FillOpacity(10).
			WithTarget(dash.PromQuery(`sum(rate(vmagent_remotewrite_bytes_sent_total{region="${region}", app_kubernetes_io_instance="victoria-metrics-agent-global"}[${window}]))`, ""))).
		WithPanel(ts("vmagent regional (${region}) - scrape download rate", dash.UnitBytesPerSec).
			Min(0).
			FillOpacity(10).
			WithTarget(dash.PromQuery(`sum(rate(vm_promscrape_scrape_response_size_bytes_sum{app_kubernetes_io_instance="victoria-metrics-agent-regional", region="${region}"}[${window}]))`, ""))).
		WithPanel(ts("vmagent regional (${region}) - remote write upload rate", dash.UnitBytesPerSec).
			Min(0).
			FillOpacity(10).
			WithTarget(dash.PromQuery(`sum by (url) (rate(vmagent_remotewrite_bytes_sent_total{app_kubernetes_io_instance="victoria-metrics-agent-regional", region="${region}"}[${window}]))`, ""))).
		WithPanel(ts("free disk space", dash.UnitDecimalBytes).
			Min(0).
			WithTarget(dash.PromQuery(`sum by (pod_name) (vm_free_disk_space_bytes{region="${region}"})`, "__auto"))).
		WithPanel(ts("vmstorage (global) - churn rate (new timeseries creation rate)", "series/s").
			Min(0).
			WithTarget(dash.PromQuery(`sum by (apps_kubernetes_io_pod_index) (rate(vm_new_timeseries_created_total{region="${region}", app="vmstorage"}[${window}]))`, "pod {{apps_kubernetes_io_pod_index}}"))).
		WithPanel(ts("vmstorage (global) - active timeseries", dash.UnitNone).
			Min(0).
			WithTarget(dash.PromQuery(`sum by (apps_kubernetes_io_pod_index) (max_over_time(vm_cache_entries{region="${region}", type="storage/hour_metric_ids"}[24h]))`, "pod {{apps_kubernetes_io_pod_index}}"))).
		WithPanel(ts("vmstorage (global) - ingestion rate", "rowsps").
			Min(0).
			Legend(hiddenLegend()).
			WithTarget(dash.PromQuery(`sum(rate(vm_rows_inserted_total{region="${region}"}[${window}]))`, "")))
}

func ociMirrorRow() *dashboard.RowBuilder {
	return row("OCI registry mirror").
		WithPanel(ts("OCI mirror manifest cache events", dash.UnitRequestsPerSec).
			Legend(rightLegend()).
			WithTarget(dash.PromQuery(`sum by (cache_event_type) (rate(buildbuddy_ociregistry_cache_events{region="${region}", oci_resource_type="manifest"}[${window}]))`, ""))).
		WithPanel(ts("OCI mirror blob cache events", dash.UnitRequestsPerSec).
			Legend(rightLegend()).
			WithTarget(dash.PromQuery(`sum by (cache_event_type) (rate(buildbuddy_ociregistry_cache_events{region="${region}", oci_resource_type=~"blob.*"}[${window}]))`, ""))).
		WithPanel(ts("OCI registry mirror cache download throughput", dash.UnitBytesPerSec).
			Description("Total number of bytes downloaded by consumers of the cache, per second. This does _not_ represent the average download speed across cache requests.").
			Legend(hiddenLegend()).
			WithTarget(dash.PromQuery(`sum(rate(buildbuddy_ociregistry_cache_download_size_bytes_sum{region="${region}"}[${window}]))`, "")))
}

func quotaRow() *dashboard.RowBuilder {
	return row("Quota").
		WithPanel(ts("Quota Exceeded", "").
			WithTarget(dash.PromQuery(`sum by (quota_namespace, quota_key) (rate(buildbuddy_quota_quota_exceeded_count{region="${region}"}[${window}]))`, "{{quota_key}}, {{quota_namespace}}"))).
		WithPanel(ts("Quota Empty Key Count by Namespace", "").
			WithTarget(dash.PromQuery(`sum by (quota_namespace) (rate(buildbuddy_quota_quota_key_empty_count{region="${region}"}[${window}]))`, "{{quota_namespace}}")))
}

func regionVariable() *dashboard.QueryVariableBuilder {
	query := `label_values(up, region)`
	return dash.QueryVar("region", query).
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		Current(dash.SelectedOption("us-west1", "us-west1")).
		Definition(query)
}

func windowVariable() *dashboard.CustomVariableBuilder {
	values := "30s, 1m, 5m, 10m, 15m, 30m, 1h, 2h, 4h, 8h, 16h, 1d, 2d, 5d, 7d, 14d, 30d"
	return dashboard.NewCustomVariableBuilder("window").
		Label("Averaging Window").
		Values(dashboard.StringOrMap{String: &values}).
		Current(dash.SelectedOption("1m", "1m"))
}

func jobVariable() *dashboard.QueryVariableBuilder {
	query := `label_values(up{region="$region"}, job)`
	return dash.QueryVar("job", query).
		Label("Jobs").
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		IncludeAll(true).
		Regex("buildbuddy-app|executor.*").
		Sort(dashboard.VariableSortDisabled).
		Current(dash.SelectedOption("All", "$__all")).
		Definition(query)
}

func poolVariable() *dashboard.QueryVariableBuilder {
	query := `label_values(up{region="$region"}, job)`
	return dash.QueryVar("pool", query).
		Label("Executor pool").
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		IncludeAll(true).
		Regex("executor.*").
		Sort(dashboard.VariableSortDisabled).
		Current(dash.SelectedOption("All", "$__all")).
		Definition(query)
}

func quantileVariable() *dashboard.CustomVariableBuilder {
	values := "0.25,0.5,0.75,0.9,0.95,0.99,0.999,0.9999"
	return dashboard.NewCustomVariableBuilder("quantile").
		Values(dashboard.StringOrMap{String: &values}).
		Current(dash.SelectedOption("0.5", "0.5"))
}

func cacheNameVariable() *dashboard.QueryVariableBuilder {
	query := `label_values(buildbuddy_remote_cache_disk_cache_partition_capacity_bytes{region="$region", job="buildbuddy-app", namespace!="raft-dev"},cache_name)`
	return dash.QueryVar("cache_name", query).
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		IncludeAll(true).
		Sort(dashboard.VariableSortDisabled).
		Current(dash.SelectedOption("All", "$__all")).
		Definition(query)
}

func gkePoolVariable() *dashboard.QueryVariableBuilder {
	query := `label_values(node_uname_info{region="$region"}, nodename)`
	return dash.QueryVar("gkepool", query).
		Refresh(dashboard.VariableRefreshOnTimeRangeChanged).
		IncludeAll(true).
		Regex("^(gke-.*)-([0-9a-f]{8})-(grp-)?([0-9a-z]{4})$").
		Current(dash.SelectedOption("All", "$__all")).
		Definition(query)
}

func appNodeVariable() *dashboard.QueryVariableBuilder {
	query := `label_values(kube_pod_info{pod=~"buildbuddy-app-.*", region="$region"}, node)`
	return dash.QueryVar("appnode", query).
		Refresh(dashboard.VariableRefreshOnTimeRangeChanged).
		IncludeAll(true).
		Multi(true).
		Current(dash.SelectedOption("All", "$__all")).
		Definition(query)
}

func executorNodeVariable() *dashboard.QueryVariableBuilder {
	query := `label_values(kube_pod_info{pod=~"executor-.*", region="$region"}, node)`
	return dash.QueryVar("executornode", query).
		Refresh(dashboard.VariableRefreshOnTimeRangeChanged).
		IncludeAll(true).
		Multi(true).
		Current(dash.SelectedOption("All", "$__all")).
		Definition(query)
}

func proberVariable() *dashboard.QueryVariableBuilder {
	query := `label_values(cloudprober_op_latency_usec_count{region="$region"},probe)`
	return dash.QueryVar("prober", query).
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		IncludeAll(true).
		AllowCustomValue(false).
		Current(dash.SelectedOption("All", "$__all")).
		Definition(query)
}

func build() (dashboard.Dashboard, error) {
	return dashboard.NewDashboardBuilder("BuildBuddy Metrics").
		Uid("1rsE5yoGz").
		Tags([]string{"generated", "file:buildbuddy.json"}).
		Editable().
		Timezone("").
		Refresh("1m").
		Time("now-3h", "now").
		Timepicker(dashboard.NewTimePickerBuilder().
			RefreshIntervals([]string{"1s", "5s", "10s", "15s", "30s", "1m", "5m", "15m", "30m", "1h", "2h", "1d"})).
		WithVariable(regionVariable()).
		WithVariable(windowVariable()).
		WithVariable(jobVariable()).
		WithVariable(poolVariable()).
		WithVariable(quantileVariable()).
		WithVariable(cacheNameVariable()).
		WithVariable(gkePoolVariable()).
		WithVariable(appNodeVariable()).
		WithVariable(executorNodeVariable()).
		WithVariable(proberVariable()).
		WithRow(systemStatusRow()).
		WithRow(probersRow()).
		WithRow(invocationsRow()).
		WithRow(invocationFinalizersRow()).
		WithRow(workflowsRow()).
		WithRow(distributedCacheRow()).
		WithRow(remoteCacheRow()).
		WithRow(pebbleRow()).
		WithRow(pebbleLevelsRow()).
		WithRow(sqlRow()).
		WithRow(redisRow()).
		WithRow(blobstoreRow()).
		WithRow(remoteExecutionRow()).
		WithRow(httpRow()).
		WithRow(executorPoolRow()).
		WithRow(golangRow()).
		WithRow(grpcRow()).
		WithRow(trafficStatsRow()).
		WithRow(appNodesRow()).
		WithRow(executorNodesRow()).
		WithRow(gkeNodepoolRow()).
		WithRow(internalRow()).
		WithRow(clickhouseRow()).
		WithRow(victoriaMetricsRow()).
		WithRow(ociMirrorRow()).
		WithRow(quotaRow()).
		Build()
}

func main() {
	dash.MustMarshal(build())
}
