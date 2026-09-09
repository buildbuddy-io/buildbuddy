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

	// ociClientFilter selects the outgoing HTTP clients used to talk to OCI
	// registries: "oci" (the app-side resolver) and "oci_fetcher" (the
	// executor-side layer fetcher).
	ociClientFilter = `region="$region", client_name=~"oci|oci_fetcher"`

	// attributableStatuses are the image fetch outcomes that say something
	// about whether *we* can pull images. "user_error" (bad image reference,
	// missing credentials, no such repo) and "canceled" (the task went away
	// mid-pull) are excluded: neither is a pull we failed to serve. This
	// mirrors ShouldCountImagePullError in
	// enterprise/server/remote_execution/container/container.go, which already
	// draws that line for error counting and logging.
	//
	// The distinction is not academic. Over 7d in us-sjc the split is
	// ok 60%, user_error 33%, timeout 6.6%, canceled 0.2%, error 0.06% --
	// so counting every status would peg "success" near 60% and swamp the
	// infrastructure signal with user misconfiguration.
	attributableStatuses = `status=~"ok|error|timeout"`

	// successRateMinInterval is the floor for the success rate panel's
	// $__rate_interval. See successRatePanel.
	successRateMinInterval = "10m"
)

// ts returns a full-width timeseries panel: one panel per dashboard row, thin
// lines and no fill. Callers set their own axis floor and legend.
func ts(title, unit string) *timeseries.PanelBuilder {
	return timeseries.NewPanelBuilder().
		Title(title).
		Datasource(dash.Prometheus()).
		Unit(unit).
		LineWidth(1).
		FillOpacity(0).
		GradientMode(common.GraphGradientModeNone).
		ShowPoints(common.VisibilityModeAuto).
		Tooltip(common.NewVizTooltipOptionsBuilder().
			Mode(common.TooltipDisplayModeMulti).
			Sort(common.SortOrderDescending)).
		Height(9).
		Span(24)
}

// tableLegend lists each series below the graph with its mean, max and last
// value. Worth the vertical space only when a panel draws more than one line.
func tableLegend() *common.VizLegendOptionsBuilder {
	return common.NewVizLegendOptionsBuilder().
		DisplayMode(common.LegendDisplayModeTable).
		Placement(common.LegendPlacementBottom).
		ShowLegend(true).
		Calcs([]string{"mean", "max", "last"})
}

// hiddenLegend drops the legend entirely, for single-series panels where the
// title already says what the line is.
func hiddenLegend() *common.VizLegendOptionsBuilder {
	return common.NewVizLegendOptionsBuilder().
		ShowLegend(false)
}

// --- Panels ---

func successRatePanel() *timeseries.PanelBuilder {
	return ts("Image pull success rate", dash.UnitPercentUnit).
		Description("Fraction of image pulls that succeeded, counting only pulls for images that were not already on disk. Excludes user_error (bad image reference or credentials) and canceled pulls, which are not failures we can act on.").
		// The axis is deliberately left to autoscale rather than pinned to
		// 0-100%. Success sits just under 1, so a fixed 0-1 axis would flatten
		// the line against the top and hide exactly the dips worth seeing.
		Legend(tableLegend()).
		// Pulls that miss the on-disk cache are sparse enough that a short
		// rate window leaves the denominator at zero, which shows up as gaps.
		// Flooring the panel's min interval widens $__rate_interval enough to
		// give a continuous line at the dashboard's default 6h range, and it
		// still grows with the step at longer ranges.
		Interval(successRateMinInterval).
		WithTarget(dash.PromQuery(fmt.Sprintf(
			// "or vector(0)" pins the line to zero in the rare window where
			// every pull failed. Without it the numerator is an empty vector,
			// the division drops out entirely, and a total outage would render
			// as "No data" — indistinguishable from a broken query.
			`(sum(rate(%s_count{%s, status="ok"}[$__rate_interval])) or vector(0))
  /
sum(rate(%s_count{%s, %s}[$__rate_interval]))`,
			imageFetchMetric, notOnDiskFilter,
			imageFetchMetric, notOnDiskFilter, attributableStatuses),
			"Success rate"))
}

// latencyPanel graphs one quantile of image pull latency. Each quantile gets
// its own panel: overlaying them buries p50 under the much larger p99.
func latencyPanel(quantile float64) *timeseries.PanelBuilder {
	label := fmt.Sprintf("p%d", int(quantile*100))
	return ts(fmt.Sprintf("Image pull %s latency", label), dash.UnitMicroseconds).
		Description(fmt.Sprintf("%s latency of successful image pulls for images that were not already on disk.", label)).
		Min(0).
		Legend(hiddenLegend()).
		WithTarget(dash.PromQuery(fmt.Sprintf(
			`histogram_quantile(%.2f, sum by (le) (rate(%s_bucket{%s, status="ok"}[$__rate_interval])))`,
			quantile, imageFetchMetric, notOnDiskFilter),
			label))
}

func outgoingRequestsPanel() *timeseries.PanelBuilder {
	return ts("Outgoing OCI HTTP requests", dash.UnitRequestsPerSec).
		Description(`Outgoing HTTP requests to OCI registries, by client.`).
		Min(0).
		Legend(tableLegend()).
		WithTarget(dash.PromQuery(fmt.Sprintf(
			`sum by (client_name) (rate(buildbuddy_http_client_request_count{%s}[$__rate_interval]))`,
			ociClientFilter),
			"{{client_name}}"))
}

func outgoingBytesPanel() *timeseries.PanelBuilder {
	return ts("Outgoing OCI HTTP bytes read", dash.UnitBytesPerSec).
		Description(`Bytes read from OCI registries, by client.`).
		Min(0).
		Legend(tableLegend()).
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
		WithPanel(successRatePanel()).
		WithPanel(latencyPanel(0.50)).
		WithPanel(latencyPanel(0.99)).
		WithPanel(outgoingRequestsPanel()).
		WithPanel(outgoingBytesPanel()).
		Build()
}

func main() {
	dash.MustMarshal(build())
}
