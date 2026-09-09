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
	notOnDiskFilter = `region="$region", use_oci_fetcher=~"$use_oci_fetcher", on_disk="false"`

	// ociClientFilter selects the outgoing HTTP clients used to talk to OCI
	// registries: "oci" (the app-side resolver) and "oci_fetcher" (the
	// executor-side layer fetcher).
	ociClientFilter = `region="$region", client_name=~"oci|oci_fetcher"`

	// errorRateMinInterval is the floor for the error rate panel's
	// $__rate_interval. See errorRatePanel.
	errorRateMinInterval = "10m"
)

// ts returns a full-width timeseries panel: one panel per dashboard row, thin
// lines, no fill, and a table legend so the current/mean/max of each series is
// readable without hovering.
func ts(title, unit string) *timeseries.PanelBuilder {
	return timeseries.NewPanelBuilder().
		Title(title).
		Datasource(dash.Prometheus()).
		Unit(unit).
		LineWidth(1).
		FillOpacity(0).
		GradientMode(common.GraphGradientModeNone).
		ShowPoints(common.VisibilityModeAuto).
		Min(0).
		Legend(common.NewVizLegendOptionsBuilder().
			DisplayMode(common.LegendDisplayModeTable).
			Placement(common.LegendPlacementBottom).
			ShowLegend(true).
			Calcs([]string{"mean", "max", "last"})).
		Tooltip(common.NewVizTooltipOptionsBuilder().
			Mode(common.TooltipDisplayModeMulti).
			Sort(common.SortOrderDescending)).
		Height(9).
		Span(24)
}

// --- Panels ---

func errorRatePanel() *timeseries.PanelBuilder {
	return ts("Image pull error rate", dash.UnitPercentUnit).
		Description("Fraction of image pulls that failed, counting only pulls for images that were not already on disk. Errors are rare (typically well under 1%), so the axis autoscales rather than being pinned to 0-100%.").
		// Pulls that miss the on-disk cache are sparse enough that a short
		// rate window leaves the denominator at zero, which shows up as gaps.
		// Flooring the panel's min interval widens $__rate_interval enough to
		// give a continuous line at the dashboard's default 6h range, and it
		// still grows with the step at longer ranges.
		Interval(errorRateMinInterval).
		WithTarget(dash.PromQuery(fmt.Sprintf(
			// "or vector(0)" keeps the line at zero during stretches with no
			// errors at all. Without it the numerator is an empty vector, the
			// division drops out entirely, and the panel reads "No data" —
			// indistinguishable from a broken query.
			`(sum(rate(%s_count{%s, status="error"}[$__rate_interval])) or vector(0))
  /
sum(rate(%s_count{%s}[$__rate_interval]))`,
			imageFetchMetric, notOnDiskFilter, imageFetchMetric, notOnDiskFilter),
			"Error rate"))
}

func latencyPanel() *timeseries.PanelBuilder {
	p := ts("Image pull latency", dash.UnitMicroseconds).
		Description("Latency of successful image pulls for images that were not already on disk.")
	for _, q := range []struct {
		quantile float64
		refID    string
	}{
		{0.50, "A"},
		{0.90, "B"},
		{0.99, "C"},
	} {
		p.WithTarget(dash.PromQuery(fmt.Sprintf(
			`histogram_quantile(%.2f, sum by (le) (rate(%s_bucket{%s, status="ok"}[$__rate_interval])))`,
			q.quantile, imageFetchMetric, notOnDiskFilter),
			fmt.Sprintf("p%d", int(q.quantile*100)),
		).RefId(q.refID))
	}
	return p
}

func outgoingRequestsPanel() *timeseries.PanelBuilder {
	return ts("Outgoing OCI HTTP requests", dash.UnitRequestsPerSec).
		Description(`Outgoing HTTP requests to OCI registries, by client.`).
		WithTarget(dash.PromQuery(fmt.Sprintf(
			`sum by (client_name) (rate(buildbuddy_http_client_request_count{%s}[$__rate_interval]))`,
			ociClientFilter),
			"{{client_name}}"))
}

func outgoingBytesPanel() *timeseries.PanelBuilder {
	return ts("Outgoing OCI HTTP bytes read", dash.UnitBytesPerSec).
		Description(`Bytes read from OCI registries, by client.`).
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
		Current(dash.SelectedOption("us-west1", "us-west1")).
		Definition(query)
}

func useOCIFetcherVariable() *dashboard.QueryVariableBuilder {
	query := fmt.Sprintf(`label_values(%s_count{region="$region"}, use_oci_fetcher)`, imageFetchMetric)
	return dash.QueryVar("use_oci_fetcher", query).
		Label("Use OCI fetcher").
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		IncludeAll(true).
		AllValue(".*").
		Current(dash.SelectedOption("All", "$__all")).
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
		WithVariable(useOCIFetcherVariable()).
		WithPanel(errorRatePanel()).
		WithPanel(latencyPanel()).
		WithPanel(outgoingRequestsPanel()).
		WithPanel(outgoingBytesPanel()).
		Build()
}

func main() {
	dash.MustMarshal(build())
}
