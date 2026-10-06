// Generates the Grafana dashboard for gRPC server traffic stats.
package main

import (
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/dash"
	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
)

// filterLabels are the traffic metric labels the dashboard can filter and
// group by. Each one gets a filter variable and a "Group By" option.
var filterLabels = []string{"region", "job", "provider", "remote_region", "group_id"}

// selector matches the series picked by the filter variables.
func selector() string {
	matchers := make([]string, 0, len(filterLabels))
	for _, label := range filterLabels {
		matchers = append(matchers, fmt.Sprintf(`%s=~"${%s}"`, label, label))
	}
	return strings.Join(matchers, ",")
}

func bytesRatePanel(title, description, metric string) *timeseries.PanelBuilder {
	return dash.Timeseries(title, dash.UnitBinaryBytesPerSec).
		Description(description).
		Min(0).
		Legend(common.NewVizLegendOptionsBuilder().
			DisplayMode(common.LegendDisplayModeTable).
			Placement(common.LegendPlacementBottom).
			ShowLegend(true).
			Calcs([]string{"lastNotNull", "mean"}).
			SortBy("Mean").
			SortDesc(true)).
		Height(12).
		Span(24).
		WithTarget(dash.PromQuery(fmt.Sprintf(`sum by(${group_by}) (rate(%s{%s}[${window}]))`, metric, selector()), "{{${group_by}}}"))
}

// filterVariable returns a multi-select variable over one label of the egress
// metric. Every label other than region is narrowed to the selected regions.
func filterVariable(label string) *dashboard.QueryVariableBuilder {
	metric := "buildbuddy_grpc_server_egress_bytes"
	if label != "region" {
		metric += `{region=~"${region}"}`
	}
	query := fmt.Sprintf("label_values(%s, %s)", metric, label)
	return dash.QueryVar(label, query).
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		Multi(true).
		IncludeAll(true).
		AllValue(".*").
		Current(dash.SelectedOption("All", "$__all")).
		Definition(query)
}

func groupByVariable() *dashboard.CustomVariableBuilder {
	return dashboard.NewCustomVariableBuilder("group_by").
		Label("Group By").
		Values(dashboard.StringOrMap{String: new(strings.Join(filterLabels, ", "))}).
		Current(dash.SelectedOption("provider", "provider"))
}

func windowVariable() *dashboard.CustomVariableBuilder {
	return dashboard.NewCustomVariableBuilder("window").
		Label("Averaging Window").
		Values(dashboard.StringOrMap{String: new("1m, 5m, 10m, 30m, 1h, 4h, 1d")}).
		Current(dash.SelectedOption("5m", "5m"))
}

func build() (dashboard.Dashboard, error) {
	builder := dashboard.NewDashboardBuilder("Traffic Stats").
		Uid("traffic-stats").
		Tags([]string{"generated", "file:traffic-stats.json"}).
		Editable().
		Timezone("").
		Refresh("1m").
		Time("now-6h", "now").
		Timepicker(dashboard.NewTimePickerBuilder().
			RefreshIntervals([]string{"10s", "30s", "1m", "5m", "15m", "30m", "1h"}))
	for _, label := range filterLabels {
		builder.WithVariable(filterVariable(label))
	}
	return builder.
		WithVariable(groupByVariable()).
		WithVariable(windowVariable()).
		WithPanel(bytesRatePanel(
			"Egress Bytes Rate",
			"Rate of gRPC server response bytes sent over the wire, grouped by the selected label.",
			"buildbuddy_grpc_server_egress_bytes")).
		WithPanel(bytesRatePanel(
			"Ingress Bytes Rate",
			"Rate of gRPC server request bytes received over the wire, grouped by the selected label.",
			"buildbuddy_grpc_server_ingress_bytes")).
		Build()
}

func main() {
	dash.MustMarshal(build())
}
