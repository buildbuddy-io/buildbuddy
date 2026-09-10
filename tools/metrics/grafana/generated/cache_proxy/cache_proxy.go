// Generates the "Cache Proxy Metrics" Grafana dashboard.
//
// The dashboard mirrors the rows of the main BuildBuddy dashboard
// (tools/metrics/grafana/generated/buildbuddy) that apply to cache-proxy
// pods, filtered to job=~"cache-proxy.*", plus rows specific to the proxy:
// the local hit rates of its ActionCache, ByteStream and CAS front ends and
// the pipelines that forward atime updates and cache hits to the remote
// cache.
//
// This program writes the dashboard JSON to stdout. It is intended to be
// invoked from a Bazel genrule; see the BUILD file alongside this one.
package main

import (
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/dash"
	"github.com/grafana/grafana-foundation-sdk/go/cog"
	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
)

const (
	// proxyFilter selects series exported by cache-proxy pods themselves.
	// The job is matched by regex because us-west1 runs several deployments
	// (cache-proxy-a, -b, -c) while every other region runs a single
	// "cache-proxy" job.
	proxyFilter = `region="${region}", job=~"cache-proxy.*"`

	// podFilter selects cache-proxy pods in metrics that come from Kubernetes
	// rather than from the proxy (kube-state-metrics, cAdvisor). Those carry
	// the pod's namespace but no cache-proxy job label, and pod names differ
	// per region (StatefulSet ordinals on metal, Deployment hashes on GKE,
	// cache-proxy-a/b/c in us-west1), so the namespace is the stable handle.
	podFilter = `region="${region}", namespace=~"cache-proxy-.*"`

	// cacheFilter narrows proxyFilter to one pebble cache, for panels that
	// repeat per ${cache_name}.
	cacheFilter = proxyFilter + `, cache_name="${cache_name}"`

	// envoyFilter selects the Envoy (Contour) upstream clusters that front the
	// cache proxies: one per Service port (1986 for gRPC, 443 for TLS). The
	// cluster name embeds the proxies' namespace (cache-proxy-prod,
	// cache-proxy-dev), hence the wildcard. These series are exported by the
	// Envoy pods, so they describe the proxies as external clients see them;
	// they only exist in regions with an Envoy ingress.
	envoyFilter = `region="${region}", namespace="projectcontour", envoy_cluster_name=~"cache-proxy-.*_cache-proxy-service_.*"`
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

// lastValueLegend is the table legend used across the gRPC row: one row per
// series, sorted by the most recent value.
func lastValueLegend() *common.VizLegendOptionsBuilder {
	return tableLegend("lastNotNull").
		SortBy("Last *").
		SortDesc(true)
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

// rightAxisProps moves a series to a separate right-hand axis with the given
// unit; for use with OverrideByName/OverrideByQuery.
func rightAxisProps(unit string) []dashboard.DynamicConfigValue {
	return []dashboard.DynamicConfigValue{
		{Id: "custom.axisPlacement", Value: "right"},
		{Id: "unit", Value: unit},
	}
}

// dashedThreshold draws a dashed line at value and colors the series red
// above it.
func dashedThreshold(p *timeseries.PanelBuilder, value float64) *timeseries.PanelBuilder {
	return p.
		Thresholds(dashboard.NewThresholdsConfigBuilder().
			Mode(dashboard.ThresholdsModeAbsolute).
			Steps([]dashboard.Threshold{{Color: "green"}, {Color: "red", Value: new(value)}})).
		ThresholdsStyle(common.NewGraphThresholdsStyleConfigBuilder().
			Mode(common.GraphThresholdsStyleModeDashed))
}

func systemStatusRow() *dashboard.RowBuilder {
	return row("System Status").
		WithPanel(ts("Cache Proxy Instances", dash.UnitShort).
			Description("Pods whose metrics endpoint is up, pods Kubernetes reports Ready, backends Envoy considers healthy (the max over Envoy pods and clusters, so a replica that is still converging after a restart does not drag it down; Envoy trusts the EndpointSlice, so this follows readiness), and the autoscaler target. The first three should agree; a Ready or Envoy-healthy count above the up count means traffic is being sent to a pod nobody can reach.").
			Height(7).
			Decimals(0).
			Min(0).
			ShowPoints(common.VisibilityModeNever).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`sum by (job) (up{`+proxyFilter+`})`, "{{job}} up").RefId("A")).
			WithTarget(dash.PromQuery(`sum(kube_pod_status_ready{`+podFilter+`, condition="true"})`, "Ready").RefId("B")).
			WithTarget(dash.PromQuery(`max(envoy_cluster_membership_healthy{`+envoyFilter+`})`, "healthy in Envoy").RefId("D")).
			WithTarget(dash.PromQuery(`sum by (horizontalpodautoscaler) (kube_horizontalpodautoscaler_status_desired_replicas{region="${region}", horizontalpodautoscaler=~"cache-proxy.*autoscaler"})`, "{{horizontalpodautoscaler}} target").RefId("C"))).
		WithPanel(ts("Cache-proxy nodes cordoned or NotReady", dash.UnitShort).
			Description("Nodes hosting a cache-proxy pod that are cordoned (unschedulable) or whose Ready condition is not True. A node that shows here while its pod still counts as Ready on the left is a pod receiving traffic that nobody can reach. Quiet nodes are omitted.").
			Height(7).
			Min(0).
			Max(1).
			Decimals(0).
			LineInterpolation(common.LineInterpolationStepAfter).
			FillOpacity(15).
			ShowPoints(common.VisibilityModeNever).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`max by (node) (kube_node_status_condition{region="${region}", condition="Ready", status!="true", node=~"${node}"} == 1) and on (node) count by (node) (kube_pod_info{`+podFilter+`})`, "{{node}} NotReady").RefId("A")).
			WithTarget(dash.PromQuery(`max by (node) (kube_node_spec_unschedulable{region="${region}", node=~"${node}"} == 1) and on (node) count by (node) (kube_pod_info{`+podFilter+`})`, "{{node}} cordoned").RefId("B"))).
		WithPanel(ts("cache-proxy versions", "").
			WithTarget(dash.PromQuery(`sum by (version, commit) (buildbuddy_version{`+proxyFilter+`})`, "{{version}} ({{commit}})"))).
		WithPanel(ts("Failing Health Checks", "").
			AxisSoftMax(0).
			WithTarget(dash.PromQuery(`sum(1 - (buildbuddy_health_check_status{`+proxyFilter+`} == 0)) by (pod_name, health_check_name)`, "__auto"))).
		WithPanel(ts("Unexpected Restarts", dash.UnitShort).
			FillOpacity(10).
			ShowPoints(common.VisibilityModeNever).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`sum(increase(kube_pod_container_status_restarts_total{`+podFilter+`}[1m])) by (pod, container, namespace) > 0`, ""))).
		WithPanel(ts("CPU usage", "").
			Legend(hiddenLegend()).
			WithTarget(dash.PromQuery(`sum(rate(container_cpu_usage_seconds_total{`+podFilter+`, container=""}[1m])) by (pod)`, "__auto").RefId("A")).
			WithTarget(dash.PromQuery(`max(container_spec_cpu_quota{`+podFilter+`, container=""} / container_spec_cpu_period{`+podFilter+`, container=""}) by (pod)`, "{{pod}} limit").RefId("B"))).
		WithPanel(ts("Memory usage", dash.UnitBytes).
			Legend(hiddenLegend()).
			WithTarget(dash.PromQuery(`sum(container_memory_working_set_bytes{`+podFilter+`, container=""}) by (pod)`, "__auto").RefId("A")).
			WithTarget(dash.PromQuery(`sum(container_spec_memory_limit_bytes{`+podFilter+`, container=""}) by (pod)`, "{{pod}} limit").RefId("B"))).
		WithPanel(dashedThreshold(ts("CPU throttled period fraction", dash.UnitPercentUnit), 0.5).
			Description("Fraction of CFS scheduling periods (100ms) in which the cache-proxy container exhausted its CPU quota and was throttled. CPU usage alone cannot show demand above the limit; this shows how often the container hit the wall. Sustained values above ~0.5 have caused liveness-probe freezes.").
			Min(0).
			Max(1).
			Legend(hiddenLegend()).
			WithTarget(dash.PromQuery(`sum by (pod) (rate(container_cpu_cfs_throttled_periods_total{`+podFilter+`, container="cache-proxy"}[1m])) / sum by (pod) (rate(container_cpu_cfs_periods_total{`+podFilter+`, container="cache-proxy"}[1m]))`, "__auto")))
}

// ingressRow shows the cache proxies from the front door: what Envoy sees
// when it forwards client requests to the cache-proxy Service. The
// proxies' own metrics cannot show a request that never reached a proxy,
// which is exactly what happens when Envoy keeps forwarding to a backend
// that is gone but still listed as Ready (us-sjc, 2026-09-09: a node was
// rebooted without a successful drain and 1 in 14 requests failed for five
// minutes while every proxy-side graph looked normal).
//
// The 5xx ratio and connect failure panels carry the thresholds of the
// CacheProxyUpstream5xxRatioHigh and CacheProxyUpstreamConnectFailures
// alerts in buildbuddy-internal's alerts-regional.yaml.
func ingressRow() *dashboard.RowBuilder {
	rq := `envoy_cluster_upstream_rq_xx{` + envoyFilter
	return row("Ingress (Envoy)").
		WithPanel(ts("Upstream 5xx responses", dash.UnitRequestsPerSec).
			Description("Responses Envoy returned as 5xx on behalf of the cache-proxy clusters. For the gRPC cluster these are almost all Envoy-generated: 503 when it could not connect to the backend, 504 when the backend never answered. Baseline is ~0.").
			Min(0).
			ShowPoints(common.VisibilityModeNever).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`sum by (envoy_cluster_name, envoy_response_code_class) (rate(`+rq+`, envoy_response_code_class="5"}[${window}]))`, "{{envoy_cluster_name}} {{envoy_response_code_class}}xx"))).
		WithPanel(dashedThreshold(ts("Upstream 5xx ratio", dash.UnitPercentUnit), 0.01).
			Description("Share of each cluster's upstream responses that were 5xx. One unreachable backend out of N costs about 1/N of requests. The dashed line is the CacheProxyUpstream5xxRatioHigh alert threshold (1% for 2m); the alert evaluates the ratio summed over both clusters, so a fault confined to the low-traffic 443 cluster can show here without firing it.").
			Min(0).
			AxisSoftMax(0.02).
			ShowPoints(common.VisibilityModeNever).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`sum by (envoy_cluster_name) (rate(`+rq+`, envoy_response_code_class="5"}[${window}])) / sum by (envoy_cluster_name) (rate(`+rq+`}[${window}]))`, "{{envoy_cluster_name}}"))).
		WithPanel(dashedThreshold(ts("Upstream connect failures", dash.UnitEventsPerSec), 5).
			Description("Connections Envoy could not open to a cache-proxy backend, summed over all Envoy pods. A refused connection (backend gone, port closed) fails immediately; an unanswered SYN (node gone) fails after the 2s connect timeout and is counted in both series. The dashed line is the CacheProxyUpstreamConnectFailures alert threshold (5/s for 2m).").
			Min(0).
			ShowPoints(common.VisibilityModeNever).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`sum(rate(envoy_cluster_upstream_cx_connect_fail{`+envoyFilter+`}[${window}]))`, "connect failures (all)").RefId("A")).
			WithTarget(dash.PromQuery(`sum(rate(envoy_cluster_upstream_cx_connect_timeout{`+envoyFilter+`}[${window}]))`, "of which timed out").RefId("B")))
}

func probersRow() *dashboard.RowBuilder {
	return row("Probers").
		WithPanel(dashedThreshold(ts("Success Ratio", dash.UnitPercentUnit), 0.9).
			Height(7).
			Span(24).
			AxisSoftMin(0).
			AxisSoftMax(1).
			ShowPoints(common.VisibilityModeNever).
			WithTarget(dash.PromQuery(`sum(increase(cloudprober_success{region="${region}", probe=~".*proxy.*"}[10m])) by (probe) / sum(increase(cloudprober_total{region="${region}", probe=~".*proxy.*"}[10m])) by (probe)`, "{{probe}} Up")))
}

func atimeUpdaterRow() *dashboard.RowBuilder {
	return row("Atime Updater").
		WithPanel(ts("Remote Atime Updates (per-digest)", "").
			Description("Atime updates enqueued for the remote cache, by the outcome of the enqueue.").
			WithTarget(dash.PromQuery(`sum by (status) (rate(buildbuddy_proxy_remote_atime_updates{`+proxyFilter+`}[${window}]))`, "{{status}}"))).
		WithPanel(ts("Remote Atime Update Requests", dash.UnitRequestsPerSec).
			Description("FindMissingBlobs requests sent to the remote cache to refresh blob atimes, by gRPC status.").
			WithTarget(dash.PromQuery(`sum by (status) (rate(buildbuddy_proxy_remote_atime_update_requests{`+proxyFilter+`}[${window}]))`, "{{status}}")))
}

func hitTrackerRow() *dashboard.RowBuilder {
	requests := `buildbuddy_proxy_remote_hit_tracker_requests`
	return row("Hit Tracker").
		WithPanel(ts("Remote Cache Hit Updates (per-hit)", "").
			Span(8).
			Description("Cache hits enqueued for the remote hit tracker, by the outcome of the enqueue.").
			WithTarget(dash.PromQuery(`sum by (status) (rate(buildbuddy_proxy_remote_hit_tracker_updates{`+proxyFilter+`}[${window}]))`, "{{status}}"))).
		WithPanel(ts("Remote Cache Hit Update Requests", dash.UnitRequestsPerSec).
			Span(8).
			Description("HitTrackerService.Track RPCs sent to the remote hit tracker, by gRPC status.").
			WithTarget(dash.PromQuery(`sum by (status) (rate(`+requests+`_count{`+proxyFilter+`}[${window}]))`, "{{status}}"))).
		WithPanel(ts("Hit-Updates per Remote Cache Hit Update Request q=${quantile}", "").
			Span(8).
			WithTarget(dash.PromQuery(`histogram_quantile(${quantile}, sum by (le, status) (rate(`+requests+`_bucket{`+proxyFilter+`}[${window}])))`, "{{status}}")))
}

// counter is a proxy-side counter together with the label matchers that
// select the traffic of interest, e.g. CAS reads exclude BatchUpdateBlobs.
type counter struct {
	metric   string
	matchers string
}

// rate returns the per-second rate of c over ${window}. extra is appended to
// the selector, e.g. `, cache_status="hit"`.
func (c counter) rate(extra string) string {
	return fmt.Sprintf(`rate(%s{%s%s}[${window}])`, c.metric, c.matchers, extra)
}

// sumOf adds up the given terms, parenthesized when there is more than one so
// the result can take part in a division.
func sumOf(terms []string) string {
	if len(terms) == 1 {
		return terms[0]
	}
	return "(" + strings.Join(terms, " + ") + ")"
}

// trafficPanel builds one panel of the Cache Proxy row: reads split by
// cache_status and total writes on the left axis, plus the read hit rate on
// a right-hand percent axis. All reads and all writes are added up, which is
// how the "All" panels combine the three front ends.
func trafficPanel(title, unit string, reads, writes []counter) *timeseries.PanelBuilder {
	var byStatus, hits, total, written []string
	for _, r := range reads {
		byStatus = append(byStatus, "sum by (cache_status) ("+r.rate("")+")")
		hits = append(hits, "sum("+r.rate(`, cache_status="hit"`)+")")
		total = append(total, "sum("+r.rate("")+")")
	}
	for _, w := range writes {
		written = append(written, "sum("+w.rate("")+")")
	}
	return ts(title, unit).
		OverrideByQuery("C", rightAxisProps(dash.UnitPercentUnit)).
		WithTarget(dash.PromQuery(strings.Join(byStatus, " + "), "read {{cache_status}}").RefId("A")).
		WithTarget(dash.PromQuery(strings.Join(written, " + "), "write").RefId("B")).
		WithTarget(dash.PromQuery(sumOf(hits)+" / "+sumOf(total), "read hit rate").RefId("C"))
}

func cacheProxyRow() *dashboard.RowBuilder {
	ac := func(suffix string) counter {
		return counter{"buildbuddy_proxy_action_cache_" + suffix, proxyFilter}
	}
	bs := func(suffix string) counter {
		return counter{"buildbuddy_proxy_byte_stream_" + suffix, proxyFilter}
	}
	// CAS reads and writes share one counter, split by the op label:
	// BatchUpdateBlobs is the only write.
	casRead := func(suffix string) counter {
		return counter{"buildbuddy_proxy_content_addressable_storage_" + suffix, proxyFilter + `, op!="BatchUpdateBlobs"`}
	}
	casWrite := func(suffix string) counter {
		return counter{"buildbuddy_proxy_content_addressable_storage_" + suffix, proxyFilter + `, op="BatchUpdateBlobs"`}
	}
	return row("Cache Proxy").
		WithPanel(trafficPanel("Action Cache (Bytes)", dash.UnitBytesPerSec,
			[]counter{ac("read_bytes")}, []counter{ac("write_bytes")})).
		WithPanel(trafficPanel("Action Cache (Requests)", dash.UnitRequestsPerSec,
			[]counter{ac("read_requests")}, []counter{ac("write_requests")})).
		WithPanel(trafficPanel("Byte Stream (Bytes)", dash.UnitBytesPerSec,
			[]counter{bs("read_bytes")}, []counter{bs("write_bytes")})).
		WithPanel(trafficPanel("Byte Stream (Requests)", dash.UnitRequestsPerSec,
			[]counter{bs("read_requests")}, []counter{bs("write_requests")})).
		WithPanel(trafficPanel("CAS (Bytes)", dash.UnitBytesPerSec,
			[]counter{casRead("bytes")}, []counter{casWrite("bytes")}).
			Span(8)).
		WithPanel(trafficPanel("CAS (Digests)", dash.UnitRequestsPerSec,
			[]counter{casRead("digests")}, []counter{casWrite("digests")}).
			Span(8)).
		WithPanel(trafficPanel("CAS (Requests)", dash.UnitRequestsPerSec,
			[]counter{casRead("requests")}, []counter{casWrite("requests")}).
			Span(8)).
		WithPanel(trafficPanel("All (Bytes)", dash.UnitBytesPerSec,
			[]counter{ac("read_bytes"), bs("read_bytes"), casRead("bytes")},
			[]counter{ac("write_bytes"), bs("write_bytes"), casWrite("bytes")})).
		WithPanel(trafficPanel("All (Digests)", dash.UnitRequestsPerSec,
			[]counter{ac("read_requests"), bs("read_requests"), casRead("digests")},
			[]counter{ac("write_requests"), bs("write_requests"), casWrite("digests")}))
}

func distributedCacheRow() *dashboard.RowBuilder {
	// methodPanel plots latency percentiles of one DistributedCache RPC on
	// the left axis and its request rate on the right.
	methodPanel := func(method string) *timeseries.PanelBuilder {
		filters := fmt.Sprintf(`%s, grpc_service="distributed_cache.DistributedCache", grpc_method="%s"`, proxyFilter, method)
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
	lookups := `buildbuddy_remote_cache_lookaside_cache_lookup_count`
	evictionAge := `buildbuddy_remote_cache_lookaside_cache_eviction_age_msec`
	backfill := `buildbuddy_remote_cache_distributed_cache_backfill_latency_usec`
	return row("Distributed Cache").
		WithPanel(ts("Request Mix", "").
			WithTarget(dash.PromQuery(`sum(rate(grpc_server_started_total{`+proxyFilter+`, grpc_service="distributed_cache.DistributedCache"}[${window}])) by (grpc_method)`, "{{grpc_method}}"))).
		WithPanel(methodPanel("Metadata")).
		WithPanel(methodPanel("GetWithMetadata")).
		WithPanel(methodPanel("GetMulti")).
		WithPanel(methodPanel("FindMissing")).
		WithPanel(methodPanel("Write")).
		WithPanel(methodPanel("Read")).
		WithPanel(ts("Lookaside cache hits and misses", dash.UnitRequestsPerSec).
			AxisPlacement(common.AxisPlacementLeft).
			Tooltip(multiTooltip()).
			OverrideByName("hit_ratio", rightAxisProps(dash.UnitPercentUnit)).
			WithTarget(dash.PromQuery(`sum(rate(`+lookups+`{`+proxyFilter+`}[${window}])) by (status)`, "__auto").RefId("A")).
			WithTarget(dash.PromQuery(`sum(rate(`+lookups+`{`+proxyFilter+`, status="hit"}[${window}])) / sum(rate(`+lookups+`{`+proxyFilter+`}[${window}]))`, "hit_ratio").RefId("B"))).
		WithPanel(ts("Lookaside cache eviction age by reason", dash.UnitMilliseconds).
			WithTarget(dash.PromQuery(`histogram_quantile(0.99, sum by (le, eviction_reason) (rate(`+evictionAge+`_bucket{`+proxyFilter+`}[${window}])))`, "{{eviction_reason}} P99").RefId("A")).
			WithTarget(dash.PromQuery(`histogram_quantile(0.95, sum by (le, eviction_reason) (rate(`+evictionAge+`_bucket{`+proxyFilter+`}[${window}])))`, "{{eviction_reason}} P95").RefId("B")).
			WithTarget(dash.PromQuery(`histogram_quantile(0.5, sum by (le, eviction_reason) (rate(`+evictionAge+`_bucket{`+proxyFilter+`}[${window}])))`, "{{eviction_reason}} P50").RefId("C")).
			WithTarget(dash.PromQuery(`sum by (eviction_reason) (increase(`+evictionAge+`_sum{`+proxyFilter+`}[${window}])) / sum by (eviction_reason) (increase(`+evictionAge+`_count{`+proxyFilter+`}[${window}]))`, "{{eviction_reason}} avg").RefId("D"))).
		WithPanel(ts("Backfill count by status", dash.UnitRequestsPerSec).
			Description("The number of digests backfilled").
			AxisPlacement(common.AxisPlacementLeft).
			Legend(rightLegend()).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`sum by (status) (rate(`+backfill+`_count{`+proxyFilter+`}[${window}]))`, "__auto"))).
		WithPanel(ts("Successful backfill latency", dash.UnitMicroseconds).
			WithTarget(dash.PromQuery(`histogram_quantile(0.99, sum by (le) (rate(`+backfill+`_bucket{`+proxyFilter+`, status="OK"}[${window}])))`, "P99").RefId("A")).
			WithTarget(dash.PromQuery(`histogram_quantile(0.95, sum by (le) (rate(`+backfill+`_bucket{`+proxyFilter+`, status="OK"}[${window}])))`, "P95").RefId("B")).
			WithTarget(dash.PromQuery(`histogram_quantile(0.5, sum by (le) (rate(`+backfill+`_bucket{`+proxyFilter+`, status="OK"}[${window}])))`, "P50").RefId("C")).
			WithTarget(dash.PromQuery(`sum(rate(`+backfill+`_sum{`+proxyFilter+`, status="OK"}[${window}])) / sum(rate(`+backfill+`_count{`+proxyFilter+`, status="OK"}[${window}]))`, "avg").RefId("D")))
}

// perCache returns a full-width panel repeated once per ${cache_name}.
func perCache(title, unit string) *timeseries.PanelBuilder {
	return ts(title, unit).
		Span(24).
		Repeat("cache_name").
		RepeatDirection(dashboard.PanelRepeatDirectionH)
}

func remoteCacheRow() *dashboard.RowBuilder {
	diskCache := `buildbuddy_remote_cache_disk_cache`
	eviction := `buildbuddy_remote_cache_pebble_cache_eviction`
	return row("Remote Cache").
		WithPanel(perCache("Disk Cache Avg Last Evicted Age (${cache_name})", dash.UnitSeconds).
			Description("Avg age of last item evicted by the disk cache").
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`avg(`+diskCache+`_last_eviction_age_usec{`+cacheFilter+`}/1e6) by (partition_id)`, ""))).
		WithPanel(dash.Heatmap("Files Added to Disk Cache by Size (${cache_name})", "bytes").
			Repeat("cache_name").
			RepeatDirection(dashboard.PanelRepeatDirectionH).
			MaxDataPoints(25).
			WithTarget(dash.PromHeatmapQuery(`sum(increase(` + diskCache + `_added_file_size_bytes_bucket{` + cacheFilter + `}[$__interval])) by (le)`))).
		WithPanel(perCache("Disk Cache Filesystem Usage (${cache_name})", dash.UnitPercentUnit).
			Min(0).
			Max(1).
			Legend(tableLegend("lastNotNull").
				SortBy("Last *")).
			WithTarget(dash.PromQuery(`max((`+diskCache+`_filesystem_total_bytes{`+cacheFilter+`} - `+diskCache+`_filesystem_avail_bytes{`+cacheFilter+`}) / `+diskCache+`_filesystem_total_bytes{`+cacheFilter+`}) by (pod_name)`, "{{pod_name}}"))).
		WithPanel(perCache("Disk Cache eviction rate (${cache_name})", "").
			WithTarget(dash.PromQuery(`max(rate(`+diskCache+`_num_evictions{`+cacheFilter+`}[10m])) by (pod_name, partition_id)`, "__auto"))).
		WithPanel(perCache("Eviction resample latency (${cache_name})", dash.UnitMicroseconds).
			WithTarget(dash.PromQuery(`histogram_quantile(${quantile}, sum(rate(`+eviction+`_resample_latency_usec_bucket{`+cacheFilter+`}[${window}])) by (le, partition_id))`, "__auto"))).
		WithPanel(perCache("Eviction sample queue length (${cache_name})", dash.UnitShort).
			WithTarget(dash.PromQuery(`sum(`+eviction+`_samples_chan_size{`+cacheFilter+`}) by (partition_id)`, "__auto"))).
		WithPanel(perCache("Eviction evict latency (${cache_name})", dash.UnitMicroseconds).
			WithTarget(dash.PromQuery(`histogram_quantile(${quantile}, sum(rate(`+eviction+`_evict_latency_usec_bucket{`+cacheFilter+`}[${window}])) by (le, partition_id))`, "__auto"))).
		WithPanel(perCache("Eviction samples by status (${cache_name})", dash.UnitOps).
			WithTarget(dash.PromQuery(`sum(rate(`+eviction+`_samples{`+cacheFilter+`}[${window}])) by (partition_id, status)`, "__auto"))).
		WithPanel(perCache("Disk Cache Partition Usage (${cache_name})", dash.UnitPercentUnit).
			Min(0).
			Max(1).
			WithTarget(dash.PromQuery(`max(`+diskCache+`_partition_size_bytes{`+cacheFilter+`} / `+diskCache+`_partition_capacity_bytes{`+cacheFilter+`}) by (pod_name, partition_id)`, "{{pod_name}} {{partition_id}}")))
}

func pebbleRow() *dashboard.RowBuilder {
	pebble := `buildbuddy_remote_cache_pebble_cache_pebble`
	// compressionFactor is the inverse of the compression ratio
	// (compressed / decompressed bytes) at quantile q of the ratio
	// distribution, i.e. how many times smaller the data got. Because of the
	// inversion, quantile q of the ratio is percentile 1-q of the factor.
	compressionFactor := func(q string) string {
		return `1 / histogram_quantile(` + q + `, sum(rate(buildbuddy_pebble_compression_ratio_bucket{` + cacheFilter + `}[10m])) by (le))`
	}
	// opLatency is quantile q of pebble operation latency, by operation.
	opLatency := func(q string) string {
		return `histogram_quantile(` + q + `, sum(rate(` + pebble + `_op_latency_usec_bucket{` + proxyFilter + `, pebble_id="${cache_name}"}[1m])) by (le, pebble_op))`
	}
	return row("Remote Cache Pebble").
		WithPanel(ts("Compression Ratio", "").
			Span(24).
			Description("How many times smaller pebble's compression makes a stream of data (decompressed / compressed bytes), at percentiles of that factor across streams: p90 is what the best-compressing 10% of streams exceed, p1 is what 99% of streams achieve. Each series is the inverse of the matching quantile of the compressed/decompressed ratio histogram.").
			WithTarget(dash.PromQuery(compressionFactor("0.1"), "p90").RefId("A")).
			WithTarget(dash.PromQuery(compressionFactor("0.5"), "p50").RefId("B")).
			WithTarget(dash.PromQuery(compressionFactor("0.99"), "p1").RefId("C"))).
		WithPanel(perCache("Compaction rate (${cache_name}) (by type)", "").
			WithTarget(dash.PromQuery(`sum(rate(`+pebble+`_compact_count{`+cacheFilter+`}[1m])) by (compaction_type)`, "__auto"))).
		WithPanel(perCache("Compaction state (${cache_name})", dash.UnitBytes).
			OverrideByName("in progress (count)", rightAxisProps(dash.UnitNone)).
			OverrideByName("marked files", rightAxisProps(dash.UnitNone)).
			WithTarget(dash.PromQuery(`sum(`+pebble+`_compact_in_progress_bytes{`+cacheFilter+`})`, "in progress (bytes)").RefId("A")).
			WithTarget(dash.PromQuery(`sum(`+pebble+`_compact_in_progress{`+cacheFilter+`})`, "in progress (count)").RefId("B")).
			WithTarget(dash.PromQuery(`sum(`+pebble+`_compact_marked_files{`+cacheFilter+`})`, "marked files").RefId("C"))).
		WithPanel(perCache("Compaction estimated debt (${cache_name})", dash.UnitBytes).
			WithTarget(dash.PromQuery(`sum(`+pebble+`_compact_estimated_debt_bytes{`+cacheFilter+`}) by (pod_name)`, "{{pod_name}}"))).
		WithPanel(perCache("Op Rate (${cache_name})", dash.UnitOps).
			WithTarget(dash.PromQuery(`sum(rate(`+pebble+`_op_count{`+proxyFilter+`, pebble_id="${cache_name}"}[1m])) by (pebble_op)`, "__auto"))).
		WithPanel(perCache("Op p50 Latency (${cache_name})", dash.UnitMicroseconds).
			WithTarget(dash.PromQuery(opLatency("0.50"), "__auto"))).
		WithPanel(perCache("Op p95 Latency (${cache_name})", dash.UnitMicroseconds).
			WithTarget(dash.PromQuery(opLatency("0.95"), "__auto"))).
		WithPanel(perCache("Op p99 Latency (${cache_name})", dash.UnitMicroseconds).
			WithTarget(dash.PromQuery(opLatency("0.99"), "__auto")))
}

func pebbleLevelsRow() *dashboard.RowBuilder {
	level := `buildbuddy_remote_cache_pebble_cache_pebble_level`
	// gauge sums a per-level gauge across pods.
	gauge := func(title, unit, metric string) *timeseries.PanelBuilder {
		return ts(title, unit).
			WithTarget(dash.PromQuery(`sum(`+level+`_`+metric+`{`+cacheFilter+`}) by (level)`, "{{level}}"))
	}
	// rate sums the per-second rate of a per-level counter across pods.
	rate := func(title, unit, metric string) *timeseries.PanelBuilder {
		return ts(title, unit).
			WithTarget(dash.PromQuery(`sum(rate(`+level+`_`+metric+`{`+cacheFilter+`}[1m])) by (level)`, "{{level}}"))
	}
	return row("Remote Cache Pebble Levels (${cache_name})").
		Repeat("cache_name").
		WithPanel(gauge("Number files (by level)", "", "num_files")).
		WithPanel(gauge("Size (by level)", dash.UnitBytes, "size_bytes")).
		WithPanel(gauge("Compaction score (by level)", dash.UnitNone, "score")).
		WithPanel(rate("Bytes in (by level)", dash.UnitBytes, "bytes_in_count")).
		WithPanel(rate("Bytes ingested (by level)", dash.UnitBytes, "bytes_ingested_count")).
		WithPanel(rate("Bytes moved (by level)", dash.UnitBytes, "bytes_moved_count")).
		WithPanel(rate("Bytes read (by level)", dash.UnitBytes, "bytes_read_count")).
		WithPanel(rate("Bytes compacted (by level)", dash.UnitBytes, "bytes_compacted_count")).
		WithPanel(rate("Bytes flushed (by level)", dash.UnitBytes, "bytes_flushed_count")).
		WithPanel(rate("Tables compacted (by level)", dash.UnitNone, "tables_compacted_count")).
		WithPanel(rate("Tables flushed (by level)", dash.UnitNone, "tables_flushed_count")).
		WithPanel(rate("Tables ingested (by level)", dash.UnitNone, "tables_ingested_count")).
		WithPanel(rate("Tables moved (by level)", dash.UnitNone, "tables_moved_count"))
}

func encryptionRow() *dashboard.RowBuilder {
	encryption := `buildbuddy_encryption`
	return row("Encryption").
		WithPanel(ts("Encryption Key Refreshes", "").
			WithTarget(dash.PromQuery(`sum(rate(`+encryption+`_key_refresh_count{`+proxyFilter+`}[${window}]))`, "Refreshes").RefId("A")).
			WithTarget(dash.PromQuery(`sum(rate(`+encryption+`_key_refresh_failure_count{`+proxyFilter+`}[${window}]))`, "Refresh Failures").RefId("B"))).
		WithPanel(ts("Encrypted/Decrypted Blobs", "").
			WithTarget(dash.PromQuery(`sum(rate(`+encryption+`_encrypted_blob_count{`+proxyFilter+`}[${window}]))`, "Encrypted Blobs").RefId("A")).
			WithTarget(dash.PromQuery(`sum(rate(`+encryption+`_decrypted_blob_count{`+proxyFilter+`}[${window}]))`, "Decrypted Blobs").RefId("B")).
			WithTarget(dash.PromQuery(`sum(rate(`+encryption+`_decryption_error_count{`+proxyFilter+`}[${window}]))`, "Decryption Errors").RefId("C")))
}

func grpcRow() *dashboard.RowBuilder {
	// bytesPanel plots the throughput of one otelgrpc size histogram by RPC.
	bytesPanel := func(title, metric string) *timeseries.PanelBuilder {
		return ts(title, dash.UnitBinaryBytesPerSec).
			Legend(lastValueLegend()).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`sum by (rpc_service, rpc_method) (rate(`+metric+`{`+proxyFilter+`}[${window}]))`, "/{{rpc_service}}/{{rpc_method}}"))
	}
	return row("gRPC (cache-proxy)").
		WithPanel(ts("Handled gRPC requests per second by status", dash.UnitRequestsPerSec).
			Height(9).
			WithTarget(dash.PromQuery(`sum by (grpc_code) (rate(grpc_server_handled_total{`+proxyFilter+`}[${window}]))`, "{{grpc_code}}"))).
		WithPanel(ts("Handled gRPC requests per second by method", dash.UnitRequestsPerSec).
			Height(9).
			WithTarget(dash.PromQuery(`sum by (grpc_service, grpc_method) (rate(grpc_server_handled_total{`+proxyFilter+`}[${window}]))`, "/{{grpc_service}}/{{grpc_method}}"))).
		WithPanel(ts("gRPC server handling duration, q=${quantile}", dash.UnitSeconds).
			Legend(lastValueLegend()).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`histogram_quantile(${quantile}, sum by (le, grpc_service, grpc_method) (rate(grpc_server_handling_seconds_bucket{`+proxyFilter+`}[${window}])))`, "/{{grpc_service}}/{{grpc_method}}"))).
		WithPanel(ts("gRPC client messages sent by method", dash.UnitRequestsPerSec).
			FillOpacity(5).
			Legend(lastValueLegend()).
			Tooltip(multiTooltip()).
			WithTarget(dash.PromQuery(`sum by (grpc_method, grpc_service) (rate(grpc_client_msg_sent_total{`+proxyFilter+`}[${window}]))`, "/{{grpc_service}}/{{grpc_method}}"))).
		WithPanel(bytesPanel("gRPC Client Request Bytes", "rpc_client_request_size_bytes_sum")).
		WithPanel(bytesPanel("gRPC Client Response Bytes", "rpc_client_response_size_bytes_sum")).
		WithPanel(bytesPanel("gRPC Server Request Bytes", "rpc_server_request_size_bytes_sum")).
		WithPanel(bytesPanel("gRPC Server Response Bytes", "rpc_server_response_size_bytes_sum"))
}

func trafficStatsRow() *dashboard.RowBuilder {
	// providerPanel plots one of the wire-level byte counters by provider,
	// linking to the dedicated traffic stats dashboard.
	providerPanel := func(title, description, metric string) *timeseries.PanelBuilder {
		return ts(title, dash.UnitBinaryBytesPerSec).
			Description(description).
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
			WithTarget(dash.PromQuery(`sum by (provider) (rate(`+metric+`{`+proxyFilter+`}[${window}]))`, "{{provider}}"))
	}
	return row("Traffic Stats").
		WithPanel(providerPanel("Egress by Provider",
			"Rate of gRPC server response bytes sent over the wire, by provider.",
			"buildbuddy_grpc_server_egress_bytes")).
		WithPanel(providerPanel("Ingress by Provider",
			"Rate of gRPC server request bytes received over the wire, by provider.",
			"buildbuddy_grpc_server_ingress_bytes"))
}

func golangRow() *dashboard.RowBuilder {
	legend := "{{job}} @ {{pod_name}}, {{namespace}}"
	return row("golang (cache-proxy)").
		WithPanel(ts("Heap size", dash.UnitDecimalBytes).
			Height(9).
			Description("Number of heap bytes allocated and still in use.").
			WithTarget(dash.PromQuery(`sum(go_memstats_heap_alloc_bytes{`+proxyFilter+`}) by (job, pod_name, namespace)`, legend))).
		WithPanel(ts("Next GC heap size", dash.UnitDecimalBytes).
			Height(9).
			Description("Size of the heap when the next GC will start").
			WithTarget(dash.PromQuery(`sum(go_memstats_next_gc_bytes{`+proxyFilter+`}) by (job, pod_name, namespace)`, legend))).
		WithPanel(ts("Time since last GC", dash.UnitSeconds).
			Height(9).
			Description("Time passed since the last GC finished. Smaller times indicate that the GC is running more frequently.").
			WithTarget(dash.PromQuery(`avg_over_time((time() - sum by (job, pod_name, namespace) (go_memstats_last_gc_time_seconds{`+proxyFilter+`}))[$__rate_interval])`, legend))).
		WithPanel(ts("Median GC duration", dash.UnitSeconds).
			Height(9).
			WithTarget(dash.PromQuery(`sum(go_gc_duration_seconds{`+proxyFilter+`, quantile="0.5"}) by (job, pod_name, namespace)`, legend))).
		WithPanel(ts("goroutines", "").
			Height(9).
			WithTarget(dash.PromQuery(`sum(go_goroutines{`+proxyFilter+`}) by (job, pod_name, namespace)`, legend))).
		WithPanel(ts("OS threads", "").
			Height(9).
			WithTarget(dash.PromQuery(`sum(go_threads{`+proxyFilter+`}) by (job, pod_name, namespace)`, legend)))
}

func nodesRow() *dashboard.RowBuilder {
	// byNode restricts a node-exporter series to the nodes selected by
	// ${node} and labels it with the node name.
	byNode := `* on(instance) group_left(nodename) (node_uname_info{region="${region}", nodename=~"^${node}"})`
	return row("Nodes Overview (${node})").
		WithPanel(ts("CPU", dash.UnitPercentUnit).
			Max(1).
			Legend(hiddenLegend()).
			WithTarget(dash.PromQuery(`1 - (avg by (mode, nodename) ((rate(node_cpu_seconds_total{region="${region}", mode="idle"}[1m])) `+byNode+`))`, "{{nodename}}"))).
		WithPanel(ts("Disk", dash.UnitBytesPerSec).
			AxisSoftMax(100000000).
			Legend(hiddenLegend()).
			WithTarget(dash.PromQuery(`max by (nodename) (rate(node_disk_read_bytes_total{region="${region}"}[1m]) `+byNode+`)`, "reads {{nodename}}").RefId("A")).
			WithTarget(dash.PromQuery(`max by (nodename) (rate(node_disk_written_bytes_total{region="${region}"}[1m]) `+byNode+`)`, "writes {{nodename}}").RefId("B"))).
		WithPanel(ts("Network", dash.UnitBytesPerSec).
			AxisSoftMax(100000000).
			WithTarget(dash.PromQuery(`rate(node_network_receive_bytes_total{region="${region}", device=~"(ens|eth).*"}[1m]) `+byNode, "rx {{nodename}}").RefId("A")).
			WithTarget(dash.PromQuery(`rate(node_network_transmit_bytes_total{region="${region}", device=~"(ens|eth).*"}[1m]) `+byNode, "tx {{nodename}}").RefId("B")))
}

func internalRow() *dashboard.RowBuilder {
	return row("Internal").
		WithPanel(ts("Unexpected events", "").
			Span(24).
			WithTarget(dash.PromQuery(`sum by (name) (rate(buildbuddy_unexpected_event{`+proxyFilter+`}[${window}]))`, "")))
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

func quantileVariable() *dashboard.CustomVariableBuilder {
	values := "0.25,0.5,0.75,0.9,0.95,0.99,0.999,0.9999"
	return dashboard.NewCustomVariableBuilder("quantile").
		Values(dashboard.StringOrMap{String: &values}).
		Current(dash.SelectedOption("0.5", "0.5"))
}

func cacheNameVariable() *dashboard.QueryVariableBuilder {
	query := `label_values(buildbuddy_remote_cache_disk_cache_partition_capacity_bytes{region="$region", job=~"cache-proxy.*", namespace!="raft-dev"},cache_name)`
	return dash.QueryVar("cache_name", query).
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		IncludeAll(true).
		Sort(dashboard.VariableSortDisabled).
		Current(dash.SelectedOption("All", "$__all")).
		Definition(query)
}

func nodeVariable() *dashboard.QueryVariableBuilder {
	query := `label_values(kube_pod_info{pod=~"cache-proxy.*", region="$region"},node)`
	return dash.QueryVar("node", query).
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		IncludeAll(true).
		Current(dash.SelectedOption("All", "$__all")).
		Definition(query)
}

func build() (dashboard.Dashboard, error) {
	return dashboard.NewDashboardBuilder("Cache Proxy Metrics").
		Uid("lVC084qx5").
		Tags([]string{"generated", "file:cache-proxy.json"}).
		Editable().
		Timezone("").
		Refresh("1m").
		Time("now-3h", "now").
		Timepicker(dashboard.NewTimePickerBuilder().
			RefreshIntervals([]string{"1s", "5s", "10s", "15s", "30s", "1m", "5m", "15m", "30m", "1h", "2h", "1d"})).
		WithVariable(regionVariable()).
		WithVariable(windowVariable()).
		WithVariable(quantileVariable()).
		WithVariable(cacheNameVariable()).
		WithVariable(nodeVariable()).
		WithRow(systemStatusRow()).
		WithRow(ingressRow()).
		WithRow(probersRow()).
		WithRow(atimeUpdaterRow()).
		WithRow(hitTrackerRow()).
		WithRow(cacheProxyRow()).
		WithRow(distributedCacheRow()).
		WithRow(remoteCacheRow()).
		WithRow(pebbleRow()).
		WithRow(pebbleLevelsRow()).
		WithRow(encryptionRow()).
		WithRow(grpcRow()).
		WithRow(trafficStatsRow()).
		WithRow(golangRow()).
		WithRow(nodesRow()).
		WithRow(internalRow()).
		Build()
}

func main() {
	dash.MustMarshal(build())
}
