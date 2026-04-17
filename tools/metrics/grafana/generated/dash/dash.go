// Package dash has helpers for setting up generated dashboards.
package dash

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/prometheus"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
)

// Grafana unit codes accepted by the timeseries panel's Unit() setter.
// See https://grafana.com/docs/grafana/latest/panels-visualizations/configure-standard-options/#unit
const (
	UnitBytesPerSec   = "Bps"
	UnitBitsPerSec    = "bps"
	UnitPacketsPerSec = "pps"
	UnitEventsPerSec  = "eps"
	UnitPercent       = "percent"
	UnitSeconds       = "s"
	UnitShort         = "short"
)

// Datasource returns a reference to the default Victoria Metrics datasource.
func Datasource() dashboard.DataSourceRef {
	return dashboard.DataSourceRef{
		Type: new("prometheus"),
		Uid:  new("vm"),
	}
}

// PromQuery returns a range-query target with the given expression and legend format.
func PromQuery(expr, legend string) *prometheus.DataqueryBuilder {
	return prometheus.NewDataqueryBuilder().
		Expr(expr).
		LegendFormat(legend).
		Range()
}

// Timeseries returns a timeseries panel preconfigured defaults.
func Timeseries(title, unit string) *timeseries.PanelBuilder {
	return timeseries.NewPanelBuilder().
		Title(title).
		Datasource(Datasource()).
		Unit(unit).
		LineWidth(1).
		FillOpacity(0).
		GradientMode(common.GraphGradientModeNone).
		ShowPoints(common.VisibilityModeAuto).
		Legend(
			common.NewVizLegendOptionsBuilder().
				DisplayMode(common.LegendDisplayModeTable).
				Placement(common.LegendPlacementBottom).
				ShowLegend(true).
				Calcs([]string{"mean", "max", "last"}),
		).
		Tooltip(
			common.NewVizTooltipOptionsBuilder().
				Mode(common.TooltipDisplayModeMulti).
				Sort(common.SortOrderDescending),
		).
		Height(8).
		Span(12)
}

// QueryVar returns a label_values based query variable.
// Callers can chain additional methods (Label, Refresh, Multi, IncludeAll,
// Current, AllValue) as needed.
func QueryVar(name, query string) *dashboard.QueryVariableBuilder {
	return dashboard.NewQueryVariableBuilder(name).
		Datasource(Datasource()).
		Query(dashboard.StringOrMap{String: &query}).
		Sort(dashboard.VariableSortAlphabeticalAsc)
}

// SelectedOption builds a VariableOption to pass to a QueryVar Current().
func SelectedOption(text, value string) dashboard.VariableOption {
	return dashboard.VariableOption{
		Text:  dashboard.StringOrArrayOfString{String: &text},
		Value: dashboard.StringOrArrayOfString{String: &value},
	}
}

// MustMarshal marshals the given dashboard to indented JSON on stdout, or exits 1
// on error. Intended to be the body of a generator's main().
func MustMarshal(d dashboard.Dashboard, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "build dashboard: %v\n", err)
		os.Exit(1)
	}
	out, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(out))
}
