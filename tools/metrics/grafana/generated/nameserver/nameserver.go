// Generates the Grafana dashboard for the density nameserver (DNS) metrics.
package main

import (
	"github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/dash"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/heatmap"
	"github.com/grafana/grafana-foundation-sdk/go/prometheus"
	"github.com/grafana/grafana-foundation-sdk/go/stat"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
)

const (
	window = "${window}"
	region = "${region}"

	filter = `region=~"` + region + `"`
	reqs   = "buildbuddy_dns_server_request_count"
	dur    = "buildbuddy_dns_server_handler_duration_usec"
)

func q(expr, legend string) *prometheus.DataqueryBuilder {
	return dash.PromQuery(expr, legend).Interval(window)
}

func rateAll() string {
	return `sum(rate(` + reqs + `{` + filter + `}[` + window + `]))`
}

func rateBy(label string) string {
	return `sum by (` + label + `) (rate(` + reqs + `{` + filter + `}[` + window + `]))`
}

// rcodeShare is the percentage of all queries answered with a response code
// matching rcodeRe.
func rcodeShare(rcodeRe string) string {
	return `100 * sum(rate(` + reqs + `{rcode=~"` + rcodeRe + `", ` + filter + `}[` + window + `]))` +
		` / sum(rate(` + reqs + `{` + filter + `}[` + window + `]))`
}

func latencyQuantile(quantile string) string {
	return `histogram_quantile(` + quantile + `, sum by (le) (rate(` + dur + `_bucket{` + filter + `}[` + window + `])))`
}

func grid(h, w, x, y uint32) dashboard.GridPos {
	return dashboard.GridPos{H: h, W: w, X: x, Y: y}
}

func rowAt(title string, y uint32) *dashboard.RowBuilder {
	return dashboard.NewRowBuilder(title).GridPos(grid(1, 24, 0, y))
}

func statPanel(title, description, unit, expr string) *stat.PanelBuilder {
	return dash.Stat(title, unit).
		Description(description).
		WithTarget(dash.PromQuery(expr, title))
}

func totalQPSStat() *stat.PanelBuilder {
	return statPanel("Queries/sec", "Total DNS queries handled per second.", dash.UnitOps, rateAll()).
		GridPos(grid(4, 6, 0, 1))
}

func nxdomainStat() *stat.PanelBuilder {
	return statPanel("NXDOMAIN %", "Share of queries answered NXDOMAIN (name does not exist).", dash.UnitPercent, rcodeShare("NXDOMAIN")).
		GridPos(grid(4, 6, 6, 1))
}

func errorStat() *stat.PanelBuilder {
	return statPanel("Server error %", "Share of queries answered SERVFAIL / REFUSED / NOTIMP / FORMERR.", dash.UnitPercent, rcodeShare("SERVFAIL|REFUSED|NOTIMP|FORMERR")).
		GridPos(grid(4, 6, 12, 1))
}

func p99Stat() *stat.PanelBuilder {
	return statPanel("p99 latency", "99th percentile query handler latency.", dash.UnitMicroseconds, latencyQuantile("0.99")).
		GridPos(grid(4, 6, 18, 1))
}

func qpsByTypePanel() *timeseries.PanelBuilder {
	return dash.StackedTimeseries("Queries/sec by Record Type", dash.UnitOps).
		Description("DNS query rate broken down by record type (A, AAAA, TXT, UPDATE, …).").
		GridPos(grid(8, 12, 0, 6)).
		WithTarget(q(rateBy("record_type"), "{{record_type}}"))
}

func qpsByRcodePanel() *timeseries.PanelBuilder {
	return dash.StackedTimeseries("Queries/sec by Response Code", dash.UnitOps).
		Description("DNS query rate broken down by response code (NOERROR, NXDOMAIN, REFUSED, …).").
		GridPos(grid(8, 12, 12, 6)).
		WithTarget(q(rateBy("rcode"), "{{rcode}}"))
}

func latencyPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Handler Latency Percentiles", dash.UnitMicroseconds).
		Description("Query handler latency percentiles.").
		GridPos(grid(8, 12, 0, 15)).
		WithTarget(q(latencyQuantile("0.5"), "p50").RefId("A")).
		WithTarget(q(latencyQuantile("0.9"), "p90").RefId("B")).
		WithTarget(q(latencyQuantile("0.99"), "p99").RefId("C"))
}

func durationHeatmapPanel() *heatmap.PanelBuilder {
	return dash.Heatmap("Handler Duration", dash.UnitMicroseconds).
		Description("Distribution of query handler durations.").
		GridPos(grid(8, 12, 12, 15)).
		WithTarget(dash.PromHeatmapQuery(`sum by (le) (rate(`+dur+`_bucket{`+filter+`}[`+window+`]))`).Interval(window).RefId("A"))
}

func regionVariable() *dashboard.QueryVariableBuilder {
	query := `label_values(up, region)`
	return dash.QueryVar("region", query).
		Label("Region").
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		IncludeAll(true).
		AllValue(".*").
		Definition(query)
}

func windowVariable() *dashboard.CustomVariableBuilder {
	values := "1m, 5m, 10m, 30m, 1h, 6h, 12h, 1d"
	return dashboard.NewCustomVariableBuilder("window").
		Label("Averaging Window").
		Values(dashboard.StringOrMap{String: &values}).
		Current(dash.SelectedOption("5m", "5m"))
}

func build() (dashboard.Dashboard, error) {
	return dashboard.NewDashboardBuilder("Nameserver Metrics").
		Uid("nameserver-metrics").
		Tags([]string{"generated", "file:nameserver.json"}).
		Editable().
		Refresh("1m").
		Time("now-6h", "now").
		Tooltip(dashboard.DashboardCursorSyncCrosshair).
		Timepicker(dashboard.NewTimePickerBuilder()).
		WithVariable(regionVariable()).
		WithVariable(windowVariable()).
		WithRow(rowAt("Overview", 0)).
		WithPanel(totalQPSStat()).
		WithPanel(nxdomainStat()).
		WithPanel(errorStat()).
		WithPanel(p99Stat()).
		WithRow(rowAt("Traffic", 5)).
		WithPanel(qpsByTypePanel()).
		WithPanel(qpsByRcodePanel()).
		WithRow(rowAt("Latency", 14)).
		WithPanel(latencyPanel()).
		WithPanel(durationHeatmapPanel()).
		Build()
}

func main() {
	dash.MustMarshal(build())
}
