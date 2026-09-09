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

	// statusesMinInterval is the floor for the status breakdown panel's
	// $__rate_interval. See statusesPanel.
	statusesMinInterval = "10m"
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

// statusesPanel breaks image pulls down by status, each as a share of all
// pulls. Every status is queried, but "ok" is hidden from the graph: it
// accounts for most of the volume and plotting it would squash every other
// line flat against the bottom. Keeping it in the query rather than filtering
// it out means the panel still has a series whenever any pull happened, so a
// window with no failures draws a flat zero rather than reading "No data" --
// which would be indistinguishable from a broken query. It stays in the legend
// table, where the ok share is useful context.
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
	return ts("Image pull statuses", dash.UnitPercentUnit).
		Description("Image pulls by status, as a share of all pulls for images that were not already on disk. Successful pulls are hidden from the graph so the failure lines stay readable, but remain in the legend. user_error is a bad image reference or missing credentials and canceled means the task went away mid-pull, so neither is a failure to act on -- timeout and error are.").
		Min(0).
		Legend(tableLegend()).
		OverrideByName("ok", []dashboard.DynamicConfigValue{
			{Id: "custom.hideFrom", Value: common.HideSeriesConfig{Viz: true, Legend: false, Tooltip: false}},
		}).
		// Pulls that miss the on-disk cache are sparse enough that a short
		// rate window leaves the denominator at zero, which shows up as gaps.
		// Flooring the panel's min interval widens $__rate_interval enough to
		// give continuous lines at the dashboard's default 6h range, and it
		// still grows with the step at longer ranges.
		Interval(statusesMinInterval).
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
