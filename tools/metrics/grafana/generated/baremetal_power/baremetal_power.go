// Generates a Grafana dashboard for baremetal PDU power metrics.
package main

import (
	"github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/dash"
	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
)

const pduLabelValuesQuery = `label_values(pdu_snmp_scrape_pdus_returned,pdu)`

func powerPanel(title, legendSortBy string) *timeseries.PanelBuilder {
	return dash.Timeseries(title, dash.UnitWatts).
		ThresholdsStyle(
			common.NewGraphThresholdsStyleConfigBuilder().
				Mode(common.GraphThresholdsStyleModeDashedAndArea),
		).
		Legend(
			common.NewVizLegendOptionsBuilder().
				DisplayMode(common.LegendDisplayModeTable).
				Placement(common.LegendPlacementBottom).
				ShowLegend(true).
				Calcs([]string{"mean", "max", "lastNotNull"}).
				SortBy(legendSortBy).
				SortDesc(true),
		).
		Tooltip(
			common.NewVizTooltipOptionsBuilder().
				HideZeros(false).
				Mode(common.TooltipDisplayModeSingle).
				Sort(common.SortOrderNone),
		).
		Height(8).
		Span(12)
}

func powerThresholds(values ...float64) *dashboard.ThresholdsConfigBuilder {
	steps := []dashboard.Threshold{{Value: nil, Color: "green"}}
	colors := []string{"dark-yellow", "dark-red"}
	for i, value := range values {
		steps = append(steps, dashboard.Threshold{
			Value: new(value),
			Color: colors[i],
		})
	}
	return dashboard.NewThresholdsConfigBuilder().
		Mode(dashboard.ThresholdsModeAbsolute).
		Steps(steps)
}

func totalInputPowerByRackPanel() *timeseries.PanelBuilder {
	return powerPanel("Total input power by rack", "Max").
		Id(4).
		GridPos(dashboard.GridPos{H: 8, W: 12, X: 0, Y: 1}).
		Max(22000).
		Thresholds(powerThresholds(15120, 17210)).
		WithTarget(
			dash.PromQuery(
				`sum by (pdu) (
    pdu_infeedPower{job="snmp_servertech_sentry3"}
        or
    pdu_st4InputCordActivePower{job="snmp_servertech_sentry4"}
)`,
				"__auto",
			).RefId("A"),
		)
}

func totalInputPowerByUnitPanel() *timeseries.PanelBuilder {
	return powerPanel("${pdu} > Total input power by unit", "Last *").
		Id(1).
		GridPos(dashboard.GridPos{H: 8, W: 12, X: 0, Y: 10}).
		AxisSoftMax(10800).
		Thresholds(powerThresholds(7560, 8640)).
		WithTarget(
			dash.PromQuery(
				`sum by (pdu, towerIndex) (pdu_infeedPower{job="snmp_servertech_sentry3", pdu="${pdu}"})`,
				"{{pdu}} > Unit {{towerIndex}}",
			).RefId("A"),
		).
		WithTarget(
			dash.PromQuery(
				`sum by (pdu, st4UnitName) (pdu_st4InputCordActivePower{job="snmp_servertech_sentry4", pdu="${pdu}"})`,
				"{{pdu}} > Unit {{st4UnitName}}",
			).RefId("B"),
		)
}

func outputPowerPanel() *timeseries.PanelBuilder {
	return powerPanel("${pdu} > Output power (POPS PDUs only)", "Last *").
		Id(3).
		GridPos(dashboard.GridPos{H: 8, W: 12, X: 12, Y: 10}).
		Thresholds(
			dashboard.NewThresholdsConfigBuilder().
				Mode(dashboard.ThresholdsModeAbsolute).
				Steps([]dashboard.Threshold{
					{Value: nil, Color: "green"},
					{Value: new(80.0), Color: "red"},
				}),
		).
		ThresholdsStyle(
			common.NewGraphThresholdsStyleConfigBuilder().
				Mode(common.GraphThresholdsStyleModeOff),
		).
		WithTarget(
			dash.PromQuery(
				`sum by (pdu, towerIndex, outletIndex) (pdu_outletPower{job="snmp_servertech_sentry3", pdu="${pdu}"})`,
				"{{pdu}} > Unit {{towerIndex}} > Outlet {{outletIndex}}",
			).RefId("A"),
		)
}

func overviewRow() *dashboard.RowBuilder {
	return dashboard.NewRowBuilder("Overview").
		Id(5).
		GridPos(dashboard.GridPos{H: 1, W: 24, X: 0, Y: 0})
}

func rackRow() *dashboard.RowBuilder {
	return dashboard.NewRowBuilder("Rack ${pdu}").
		Id(2).
		GridPos(dashboard.GridPos{H: 1, W: 24, X: 0, Y: 9}).
		Repeat("pdu")
}

func pduVariable() *dashboard.QueryVariableBuilder {
	return dashboard.NewQueryVariableBuilder("pdu").
		Datasource(dash.Datasource()).
		Label("PDU").
		Description("PDU hostname from Ansible inventory").
		Query(dashboard.StringOrMap{
			Map: map[string]any{
				"qryType": 1,
				"query":   pduLabelValuesQuery,
				"refId":   "PrometheusVariableQueryEditor-VariableQuery",
			},
		}).
		Current(dashboard.VariableOption{
			Text:  dashboard.StringOrArrayOfString{String: new("All")},
			Value: dashboard.StringOrArrayOfString{ArrayOfString: []string{"$__all"}},
		}).
		Multi(true).
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		Sort(dashboard.VariableSortAlphabeticalAsc).
		IncludeAll(true).
		Definition(pduLabelValuesQuery)
}

func build() (dashboard.Dashboard, error) {
	return dashboard.NewDashboardBuilder("Baremetal Power").
		Uid("cfa27mmpg0rnke").
		Tags([]string{"generated", "file:baremetal-power.json"}).
		Timezone("America/Los_Angeles").
		Editable().
		Tooltip(dashboard.DashboardCursorSyncOff).
		Timepicker(dashboard.NewTimePickerBuilder()).
		Refresh("1m").
		WithVariable(pduVariable()).
		WithRow(overviewRow()).
		WithPanel(totalInputPowerByRackPanel()).
		WithRow(rackRow()).
		WithPanel(totalInputPowerByUnitPanel()).
		WithPanel(outputPowerPanel()).
		Build()
}

func main() {
	dash.MustMarshal(build())
}
