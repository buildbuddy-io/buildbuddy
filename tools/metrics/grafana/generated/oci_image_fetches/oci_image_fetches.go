// Generates the "Container Image Pulls" Grafana dashboard.
//
// The dashboard tracks container image pulls performed by executors, focusing
// on pulls that actually hit the network (on_disk="false"), plus the outgoing
// HTTP traffic that those pulls generate against OCI registries.
//
// This program writes the resulting dashboard JSON to stdout. It is intended
// to be invoked from a Bazel genrule; see the BUILD file alongside this one.
package main

import (
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/dash"
	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
)

const (
	// imageFetchMetric is a histogram of image fetch attempts on executors,
	// in microseconds. The _count suffix gives fetch counts.
	imageFetchMetric = "buildbuddy_remote_execution_image_fetch_duration_usec"

	// notOnDiskFilter selects image pulls that were not already present on the
	// executor's disk, i.e. the ones that had to hit a registry.
	notOnDiskFilter = `region="$region", on_disk="false"`

	// ociClientFilter selects the two HTTP clients on the image pull path:
	// "oci" (the app-side resolver, enterprise/server/util/oci/oci.go) and
	// "oci_fetcher" (the executor-side layer fetcher,
	// enterprise/server/oci/ocifetcher/ocifetcher.go).
	//
	// It deliberately excludes "ociregistry"
	// (enterprise/server/oci/ociregistry/ociregistry.go), which is the registry
	// mirror server serving its own clients rather than an executor pulling an
	// image.
	//
	// Both clients route through mirrors when executor.container_registry_mirrors
	// is set, in which case their traffic goes to the mirror rather than
	// upstream and the upstream egress lands under "ociregistry" instead. That
	// flag is unset in every environment today, so these two clients are the
	// registry egress; revisit the filter if mirroring is ever turned on.
	ociClientFilter = `region="$region", client_name=~"oci|oci_fetcher"`

	// rateMinInterval is the floor applied to $__rate_interval on the panels
	// built from sparse image pull data. See statusesPanel and latencyPanel.
	rateMinInterval = "10m"

	// statusLinearThreshold is where the status panel's symlog axis switches
	// from logarithmic to linear, so shares of exactly zero still plot. See
	// statusesPanel.
	statusLinearThreshold = 1e-4

	// latencySpanNullsMsec bridges gaps in the latency panels up to this
	// width. See latencyPanel.
	latencySpanNullsMsec = 30 * 60 * 1000
)

// panel returns a full-width timeseries panel. It takes the shared defaults
// from dash.Timeseries and overrides only the geometry, so this dashboard
// follows the shared styling rather than drifting from it.
func panel(title, unit string) *timeseries.PanelBuilder {
	return dash.Timeseries(title, unit).
		Height(9).
		Span(24)
}

// tableLegend lists each series below the graph with the given calcs,
// replacing the mean/max/last default from dash.Timeseries.
func tableLegend(calcs ...string) *common.VizLegendOptionsBuilder {
	return common.NewVizLegendOptionsBuilder().
		DisplayMode(common.LegendDisplayModeTable).
		Placement(common.LegendPlacementBottom).
		ShowLegend(true).
		Calcs(calcs)
}

// hiddenLegend drops the legend entirely, for single-series panels where the
// title already says what the line is.
func hiddenLegend() *common.VizLegendOptionsBuilder {
	return common.NewVizLegendOptionsBuilder().
		ShowLegend(false)
}

// --- Panels ---

// statusesPanel breaks image pulls down by status, each as a share of all
// pulls.
//
// The axis is pinned to 0-100% rather than left to autoscale. Shares are
// bounded by definition, so a fixed axis is meaningful, and it keeps the scale
// stable as you switch regions or narrow to a group. Autoscaling also breaks
// outright in the common healthy case: when every failure line sits at a flat
// zero the data range is degenerate, and Grafana falls back to a default max
// of 100 -- which a percentunit axis renders as "10000%".
//
// The statuses are worth reading separately rather than as one error rate,
// because they are not all ours to fix. ImagePullMetricStatus in
// enterprise/server/remote_execution/container/container.go assigns them:
// "user_error" is a bad image reference or missing credentials (the codes
// ShouldCountImagePullError excludes), "canceled" means the task went away
// mid-pull, and "timeout" and "error" are the ones that point at us. Over 7d
// in us-sjc the split ran ok 60%, user_error 33%, timeout 6.6%,
// canceled 0.2%, error 0.06%.
func statusesPanel() *timeseries.PanelBuilder {
	return panel("Image pull statuses", dash.UnitPercentUnit).
		Description("Image pulls by status, as a share of all pulls for images that were not already on disk. user_error is a bad image reference or missing credentials and canceled means the task went away mid-pull, so neither is a failure to act on -- timeout and error are.").
		Min(0).
		Max(1).
		// Just max. A mean share over a window with wildly varying pull
		// volume is not a quantity you can reason about, and min is 0 for
		// every status whenever pulls are sparse enough to have a quiet
		// window -- which is nearly always.
		Legend(tableLegend("max")).
		// Symlog rather than plain log. Shares span several decades, from a
		// status at 100% down to a handful of errors in a million pulls, and a
		// linear axis flattens everything below a few percent into the floor.
		// Plain log cannot plot zero at all, and these lines sit at exactly
		// zero most of the time; symlog is logarithmic above the threshold and
		// linear below it, so quiet periods still draw.
		ScaleDistribution(common.NewScaleDistributionConfigBuilder().
			Type(common.ScaleDistributionSymlog).
			Log(10).
			LinearThreshold(statusLinearThreshold)).
		// Pulls that miss the on-disk cache are sparse enough that a short
		// rate window leaves the denominator at zero, which shows up as gaps.
		// Flooring the panel's min interval widens $__rate_interval enough to
		// give continuous lines at the dashboard's default 6h range, and it
		// still grows with the step at longer ranges.
		Interval(rateMinInterval).
		WithTarget(dash.PromQuery(fmt.Sprintf(
			// Dividing by scalar() rather than a vector keeps the status label
			// on the result without needing a group_left join.
			`sum by (status) (rate(%s_count{%s}[$__rate_interval]))
  /
scalar(sum(rate(%s_count{%s}[$__rate_interval])))`,
			imageFetchMetric, notOnDiskFilter, imageFetchMetric, notOnDiskFilter),
			"{{status}}"))
}

// latencyPanel graphs one quantile of image pull latency. Each quantile gets
// its own panel: overlaying them buries p50 under the much larger p99.
// %g throughout, not %d on quantile*100 and %.2f in the expression: those
// truncate anything finer than two decimals, so latencyPanel(0.999) would have
// queried histogram_quantile(1.00, ...) under a "p99" title.
func latencyPanel(quantile float64) *timeseries.PanelBuilder {
	label := fmt.Sprintf("p%g", quantile*100)
	return panel(fmt.Sprintf("Image pull %s latency", label), dash.UnitMicroseconds).
		Description(fmt.Sprintf("%s latency of successful image pulls for images that were not already on disk.", label)).
		Min(0).
		Legend(hiddenLegend()).
		// Pulls are sparse enough that at a full-width panel's native step the
		// rate window covers no samples much of the time, and the quantile
		// comes back empty. Measured over 24h at a 45s step, a 60s window
		// returned a value for 80% of steps in us-sjc and 23% in us-central1;
		// a 10m window brings those to 100% and 88%.
		Interval(rateMinInterval).
		// Bridge whatever gaps remain, but only short ones -- a genuinely
		// quiet stretch should still read as a break rather than a line drawn
		// through it.
		SpanNulls(common.BoolOrFloat64{Float64: new(float64(latencySpanNullsMsec))}).
		WithTarget(dash.PromQuery(fmt.Sprintf(
			`histogram_quantile(%g, sum by (le) (rate(%s_bucket{%s, status="ok"}[$__rate_interval])))`,
			quantile, imageFetchMetric, notOnDiskFilter),
			label))
}

func outgoingRequestsPanel() *timeseries.PanelBuilder {
	return panel("Outgoing OCI HTTP requests", dash.UnitRequestsPerSec).
		Description(`Outgoing HTTP requests from the image pull path, by client. "oci" is the app-side resolver and "oci_fetcher" the executor-side layer fetcher; the ociregistry mirror server is not included.`).
		Min(0).
		WithTarget(dash.PromQuery(fmt.Sprintf(
			`sum by (client_name) (rate(buildbuddy_http_client_request_count{%s}[$__rate_interval]))`,
			ociClientFilter),
			"{{client_name}}"))
}

func outgoingBytesPanel() *timeseries.PanelBuilder {
	return panel("Outgoing OCI HTTP bytes read", dash.UnitBytesPerSec).
		Description(`Bytes read by the image pull path, by client. "oci" is the app-side resolver and "oci_fetcher" the executor-side layer fetcher; the ociregistry mirror server is not included.`).
		Min(0).
		WithTarget(dash.PromQuery(fmt.Sprintf(
			`sum by (client_name) (rate(buildbuddy_http_client_response_size_bytes_sum{%s}[$__rate_interval]))`,
			ociClientFilter),
			"{{client_name}}"))
}

// --- Template variables ---

func regionVariable() *dashboard.QueryVariableBuilder {
	query := `label_values(up, region)`
	return dash.QueryVar("region", query).
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		Current(dash.SelectedOption("us-sjc", "us-sjc")).
		Definition(query)
}

// --- Dashboard ---

func build() (dashboard.Dashboard, error) {
	return dashboard.NewDashboardBuilder("Container Image Pulls").
		Uid("oci-image-fetches").
		Description("Container image pull health on executors, plus the outgoing OCI registry traffic those pulls generate.").
		Tags([]string{"generated", "file:oci-image-fetches.json"}).
		Editable().
		Refresh("1m").
		Time("now-6h", "now").
		Tooltip(dashboard.DashboardCursorSyncCrosshair).
		WithVariable(regionVariable()).
		WithPanel(statusesPanel()).
		WithPanel(latencyPanel(0.50)).
		WithPanel(latencyPanel(0.99)).
		WithPanel(outgoingRequestsPanel()).
		WithPanel(outgoingBytesPanel()).
		Build()
}

func main() {
	dash.MustMarshal(build())
}
