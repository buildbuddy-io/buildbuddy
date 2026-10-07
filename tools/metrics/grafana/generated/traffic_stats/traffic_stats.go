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

// filterLabel is a metric label that gets a filter variable and a "Group By" option.
type filterLabel struct {
	name        string
	displayName string // shown instead of name when set
	description string // selector tooltip
}

func (l filterLabel) text() string {
	if l.displayName != "" {
		return l.displayName
	}
	return l.name
}

var filterLabels = []filterLabel{
	{
		name:        "region",
		displayName: "Server Region",
		description: "Region of the BuildBuddy server that handled the RPC.",
	},
	{
		name:        "job",
		displayName: "Server Job",
		description: "Prometheus scrape job, i.e. the kind of BuildBuddy server that handled the RPC (app, cache proxy).",
	},
	{
		name:        "provider",
		displayName: "Client Cloud Provider",
		description: `Cloud provider the client connected from, inferred from its peer IP. "internal" is a private IP inside our own network, "other" is a public IP outside every known provider range.`,
	},
	{
		name:        "remote_region",
		displayName: "Client Region",
		description: `Cloud region the client connected from, inferred from its peer IP. "unknown" when the provider could not be identified; empty for internal traffic.`,
	},
	{
		name:        "group_id",
		displayName: "Group ID",
		description: `BuildBuddy group of the authenticated client. "unknown" for unauthenticated RPCs.`,
	},
}

// selector matches the series picked by the filter variables.
func selector() string {
	matchers := make([]string, 0, len(filterLabels))
	for _, l := range filterLabels {
		matchers = append(matchers, fmt.Sprintf(`%s=~"${%s}"`, l.name, l.name))
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
func filterVariable(l filterLabel) *dashboard.QueryVariableBuilder {
	metric := "buildbuddy_grpc_server_egress_bytes"
	if l.name != "region" {
		metric += `{region=~"${region}"}`
	}
	query := fmt.Sprintf("label_values(%s, %s)", metric, l.name)
	return dash.QueryVar(l.name, query).
		Description(l.description).
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		Multi(true).
		IncludeAll(true).
		AllValue(".*").
		Current(dash.SelectedOption("All", "$__all")).
		Definition(query).
		Label(l.text())
}

func groupByVariable() *dashboard.CustomVariableBuilder {
	options := make([]string, 0, len(filterLabels))
	var current dashboard.VariableOption
	for _, l := range filterLabels {
		// Grafana's "text : value" syntax shows the display name but queries get the label name.
		option := l.name
		if l.displayName != "" {
			option = l.displayName + " : " + l.name
		}
		options = append(options, option)
		if l.name == "provider" {
			current = dash.SelectedOption(l.text(), l.name)
		}
	}
	if *(current.Text.String) == "" {
		panic("provider label not found in filterLabels")
	}
	return dashboard.NewCustomVariableBuilder("group_by").
		Label("Group By").
		Description("Label each series is split by. The other selectors still filter which series are included.").
		Values(dashboard.StringOrMap{String: new(strings.Join(options, ", "))}).
		Current(current)
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
	for _, l := range filterLabels {
		builder.WithVariable(filterVariable(l))
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
