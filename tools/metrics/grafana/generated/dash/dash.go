// Package dash has helpers for setting up generated dashboards.
package dash

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/heatmap"
	"github.com/grafana/grafana-foundation-sdk/go/prometheus"
	"github.com/grafana/grafana-foundation-sdk/go/stat"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
)

// Grafana unit codes accepted by the timeseries panel's Unit() setter.
// See https://grafana.com/docs/grafana/latest/panels-visualizations/configure-standard-options/#unit
const (
	UnitBytesPerSec   = "Bps"
	UnitBitsPerSec    = "bps"
	UnitBytes         = "bytes"
	UnitPacketsPerSec = "pps"
	UnitEventsPerSec  = "eps"
	UnitMicroseconds  = "µs"
	UnitOps           = "ops"
	UnitPercent       = "percent"
	UnitPercentUnit   = "percentunit"
	UnitSeconds       = "s"
	UnitShort         = "short"
	UnitWatts         = "watt"
)

// Prometheus returns a reference to the default prometheus metrics datasource
// (backed by VictoriaMetrics).
func Prometheus() dashboard.DataSourceRef {
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

// PromHeatmapQuery returns a Prometheus range-query target formatted as a heatmap.
func PromHeatmapQuery(expr string) *prometheus.DataqueryBuilder {
	return PromQuery(expr, "__auto").
		Format(prometheus.PromQueryFormatHeatmap)
}

// Timeseries returns a timeseries panel preconfigured defaults.
func Timeseries(title, unit string) *timeseries.PanelBuilder {
	return timeseries.NewPanelBuilder().
		Title(title).
		Datasource(Prometheus()).
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

// StackedTimeseries returns a timeseries panel suitable for stacked rates.
func StackedTimeseries(title, unit string) *timeseries.PanelBuilder {
	return Timeseries(title, unit).
		FillOpacity(10).
		Stacking(
			common.NewStackingConfigBuilder().
				Group("A").
				Mode(common.StackingModeNormal),
		)
}

// Stat returns a stat panel preconfigured with common defaults.
func Stat(title, unit string) *stat.PanelBuilder {
	return stat.NewPanelBuilder().
		Title(title).
		Datasource(Prometheus()).
		Unit(unit).
		GraphMode(common.BigValueGraphModeNone).
		ColorMode(common.BigValueColorModeValue).
		ReduceOptions(
			common.NewReduceDataOptionsBuilder().
				Calcs([]string{"lastNotNull"}).
				Values(false),
		).
		Height(4).
		Span(4)
}

// Heatmap returns a heatmap panel preconfigured for Prometheus histogram buckets.
func Heatmap(title, unit string) *heatmap.PanelBuilder {
	return heatmap.NewPanelBuilder().
		Title(title).
		Datasource(Prometheus()).
		Unit(unit).
		Calculate(false).
		CellGap(1).
		Color(
			heatmap.NewHeatmapColorOptionsBuilder().
				Mode(heatmap.HeatmapColorModeOpacity).
				Scheme("Oranges").
				Fill("#3274D9").
				Scale(heatmap.HeatmapColorScaleExponential).
				Exponent(0.5).
				Steps(128).
				Reverse(false),
		).
		FilterValues(heatmap.NewFilterValueRangeBuilder().Le(1e-9)).
		RowsFrame(heatmap.NewRowsHeatmapOptionsBuilder().Layout(common.HeatmapCellLayoutAuto)).
		ShowValue(common.VisibilityModeNever).
		Tooltip(
			heatmap.NewHeatmapTooltipBuilder().
				Mode(common.TooltipDisplayModeSingle).
				YHistogram(true),
		).
		YAxis(
			heatmap.NewYAxisConfigBuilder().
				AxisPlacement(common.AxisPlacementLeft).
				Reverse(false).
				Unit(unit),
		).
		ExemplarsColor("rgba(255,0,255,0.7)").
		HideLegend().
		Height(8).
		Span(12)
}

// QueryVar returns a label_values based query variable.
// Callers can chain additional methods (Label, Refresh, Multi, IncludeAll,
// Current, AllValue) as needed.
func QueryVar(name, query string) *dashboard.QueryVariableBuilder {
	return dashboard.NewQueryVariableBuilder(name).
		Datasource(Prometheus()).
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
