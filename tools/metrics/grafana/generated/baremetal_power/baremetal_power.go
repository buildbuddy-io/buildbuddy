// Generates a Grafana dashboard for baremetal PDU power metrics.
package main

import (
	"github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/dash"
	"github.com/grafana/grafana-foundation-sdk/go/bargauge"
	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/stat"
	"github.com/grafana/grafana-foundation-sdk/go/statetimeline"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
)

const pduLabelValuesQuery = `label_values(pdu_snmp_scrape_pdus_returned{region="${region}"},pdu)`

// Sentry4 PDU ratings. Each rack has two daisy-chained 3-phase delta units
// ("Primary" = AA:*, "Link1" = BA:*) with a 60 A input and six 20 A branch
// breakers (BR1/BR4 on L1-L2, BR2/BR5 on L2-L3, BR3/BR6 on L3-L1). Servers are
// dual-fed from both units.
const (
	lineRatingAmps   = 60
	branchRatingAmps = 20
	// continuousLoadFactor is the NEC 80% continuous-load derating.
	continuousLoadFactor = 0.8
)

func metricPanel(title, unit, legendSortBy string) *timeseries.PanelBuilder {
	return dash.Timeseries(title, unit).
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

func powerPanel(title, legendSortBy string) *timeseries.PanelBuilder {
	return metricPanel(title, dash.UnitWatts, legendSortBy)
}

// warnCritThresholds returns green/dark-yellow/dark-red thresholds starting at
// the given warning and critical values.
func warnCritThresholds(values ...float64) *dashboard.ThresholdsConfigBuilder {
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

// currentThresholds returns thresholds at 80% and 100% of a breaker rating.
func currentThresholds(ratingAmps float64) *dashboard.ThresholdsConfigBuilder {
	return warnCritThresholds(ratingAmps*continuousLoadFactor, ratingAmps)
}

func totalInputPowerByRackPanel() *timeseries.PanelBuilder {
	return powerPanel("Total input power by rack", "Max").
		Id(4).
		GridPos(dashboard.GridPos{H: 8, W: 12, X: 0, Y: 1}).
		Max(22000).
		Thresholds(warnCritThresholds(15120, 17210)).
		WithTarget(
			dash.PromQuery(
				`sum by (pdu) (
    pdu_infeedPower{job="snmp_servertech_sentry3", region="${region}"}
        or
    pdu_st4InputCordActivePower{job="snmp_servertech_sentry4", region="${region}"}
)`,
				"__auto",
			).RefId("A"),
		)
}

// busiestUnitLoadSharePanel shows what fraction of each rack's power flows
// through its most-loaded PDU unit. Dual-fed racks sit near 50%; a jump to
// 80%+ means one feed is dead and servers are running on a single PSU. The
// total-power panel hides this because the total barely changes.
func busiestUnitLoadSharePanel() *stat.PanelBuilder {
	return dash.Stat("Share of rack load on busiest unit", dash.UnitPercent).
		Id(6).
		Description("Dual-fed racks sit near 50%. Above 70% one feed has failed and servers are on a single PSU.").
		GridPos(dashboard.GridPos{H: 4, W: 12, X: 12, Y: 1}).
		Min(0).
		Max(100).
		ColorMode(common.BigValueColorModeBackground).
		TextMode(common.BigValueTextModeValueAndName).
		Thresholds(warnCritThresholds(60, 70)).
		WithTarget(
			dash.PromQuery(
				`100 * max by (pdu) (sum by (pdu, towerIndex) (pdu_infeedPower{job="snmp_servertech_sentry3", region="${region}"}))
    / sum by (pdu) (pdu_infeedPower{job="snmp_servertech_sentry3", region="${region}"})`,
				"{{pdu}}",
			).RefId("A"),
		).
		WithTarget(
			dash.PromQuery(
				`100 * max by (pdu) (sum by (pdu, st4UnitName) (pdu_st4InputCordActivePower{job="snmp_servertech_sentry4", region="${region}"}))
    / sum by (pdu) (pdu_st4InputCordActivePower{job="snmp_servertech_sentry4", region="${region}"})`,
				"{{pdu}}",
			).RefId("B"),
		)
}

// abnormalStatusPanel counts Sentry4 breakers/phases per PDU that reported a
// non-normal status in the last 10 minutes.
func abnormalStatusPanel() *stat.PanelBuilder {
	return dash.Stat("Breakers/phases with abnormal status (10m)", dash.UnitNone).
		Id(7).
		Description("Sentry4 status codes: 11 pwrError, 12 breakerTripped, 14 lowAlarm, 17 highAlarm. Healthy racks read 0.").
		GridPos(dashboard.GridPos{H: 4, W: 12, X: 12, Y: 5}).
		ColorMode(common.BigValueColorModeBackground).
		TextMode(common.BigValueTextModeValueAndName).
		Thresholds(warnCritThresholds(1, 4)).
		WithTarget(
			dash.PromQuery(
				`count by (pdu) (
    max_over_time(pdu_st4BranchStatus{job="snmp_servertech_sentry4", region="${region}"}[10m]) != 0
        or
    max_over_time(pdu_st4PhaseVoltageStatus{job="snmp_servertech_sentry4", region="${region}"}[10m]) != 0
)
    or
0 * count by (pdu) (pdu_st4InputCordActivePower{job="snmp_servertech_sentry4", region="${region}"})`,
				"{{pdu}}",
			).RefId("A"),
		)
}

// phaseVoltageSpreadPanel is an early-warning graph for a degraded PDU.
// Healthy units stay under 1 V. A steady 2-4 V spread on one unit while its
// sibling stays flat is a degrading input connection.
func phaseVoltageSpreadPanel() *timeseries.PanelBuilder {
	return metricPanel("Phase voltage spread by unit (max - min)", dash.UnitVolts, "Last *").
		Id(8).
		Description("Healthy units stay under 1 V. A steady 2-4 V spread on one unit is a degrading input connection; tens of volts is a lost phase.").
		GridPos(dashboard.GridPos{H: 8, W: 12, X: 0, Y: 9}).
		Min(0).
		Thresholds(warnCritThresholds(2, 10)).
		WithTarget(
			dash.PromQuery(
				`(
    max by (pdu, towerIndex) (pdu_infeedVoltage{job="snmp_servertech_sentry3", region="${region}"})
        -
    min by (pdu, towerIndex) (pdu_infeedVoltage{job="snmp_servertech_sentry3", region="${region}"})
) / 10`,
				"{{pdu}} > Unit {{towerIndex}}",
			).RefId("A"),
		).
		WithTarget(
			dash.PromQuery(
				`(
    max by (pdu, st4UnitName) (pdu_st4PhaseVoltage{job="snmp_servertech_sentry4", region="${region}"})
        -
    min by (pdu, st4UnitName) (pdu_st4PhaseVoltage{job="snmp_servertech_sentry4", region="${region}"})
) / 10`,
				"{{pdu}} > Unit {{st4UnitName}}",
			).RefId("B"),
		)
}

// lineCurrentZeroPanel highlights input lines carrying no current while the
// rest of the unit is loaded: an open conductor upstream of the PDU.
func lineCurrentZeroPanel() *timeseries.PanelBuilder {
	return metricPanel("Input lines at 0 A while unit is loaded", dash.UnitNone, "Last *").
		Id(9).
		Description("Count of input lines per unit reading exactly 0 A while the unit's other lines carry load. Anything above 0 is an open conductor.").
		GridPos(dashboard.GridPos{H: 8, W: 12, X: 12, Y: 9}).
		Min(0).
		Thresholds(warnCritThresholds(1, 2)).
		WithTarget(
			dash.PromQuery(
				`count by (pdu, st4UnitName) (
    pdu_st4LineCurrent{job="snmp_servertech_sentry4", region="${region}"} == 0
        and on (pdu, st4UnitName)
    sum by (pdu, st4UnitName) (pdu_st4LineCurrent{job="snmp_servertech_sentry4", region="${region}"}) > 200
)
    or
0 * count by (pdu, st4UnitName) (pdu_st4LineCurrent{job="snmp_servertech_sentry4", region="${region}"})`,
				"{{pdu}} > Unit {{st4UnitName}}",
			).RefId("A"),
		)
}

func totalInputPowerByUnitPanel() *timeseries.PanelBuilder {
	return powerPanel("${pdu} > Total input power by unit", "Last *").
		Id(1).
		GridPos(dashboard.GridPos{H: 8, W: 12, X: 0, Y: 18}).
		AxisSoftMax(10800).
		Thresholds(warnCritThresholds(7560, 8640)).
		WithTarget(
			dash.PromQuery(
				`sum by (pdu, towerIndex) (pdu_infeedPower{job="snmp_servertech_sentry3", region="${region}", pdu="${pdu}"})`,
				"{{pdu}} > Unit {{towerIndex}}",
			).RefId("A"),
		).
		WithTarget(
			dash.PromQuery(
				`sum by (pdu, st4UnitName) (pdu_st4InputCordActivePower{job="snmp_servertech_sentry4", region="${region}", pdu="${pdu}"})`,
				"{{pdu}} > Unit {{st4UnitName}}",
			).RefId("B"),
		)
}

func outputPowerPanel() *timeseries.PanelBuilder {
	return powerPanel("${pdu} > Output power (POPS PDUs only)", "Last *").
		Id(3).
		GridPos(dashboard.GridPos{H: 8, W: 12, X: 12, Y: 18}).
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
				`sum by (pdu, towerIndex, outletIndex) (pdu_outletPower{job="snmp_servertech_sentry3", region="${region}", pdu="${pdu}"})`,
				"{{pdu}} > Unit {{towerIndex}} > Outlet {{outletIndex}}",
			).RefId("A"),
		)
}

// phaseVoltagePanel plots line-to-line voltage per phase pair. All pairs of a
// unit should sit together near 207 V. When one conductor opens, the two pairs
// touching it collapse and their readings sum to the healthy pair's.
func phaseVoltagePanel() *timeseries.PanelBuilder {
	return metricPanel("${pdu} > Phase-pair voltage", dash.UnitVolts, "Last *").
		Id(10).
		Description("Line-to-line voltage per phase pair. All three should sit together near 207 V; two pairs collapsing (and summing to the third) means an open conductor.").
		GridPos(dashboard.GridPos{H: 8, W: 12, X: 0, Y: 26}).
		Min(0).
		Max(240).
		Thresholds(
			dashboard.NewThresholdsConfigBuilder().
				Mode(dashboard.ThresholdsModeAbsolute).
				Steps([]dashboard.Threshold{
					{Value: nil, Color: "dark-red"},
					{Value: new(190.0), Color: "green"},
					{Value: new(230.0), Color: "dark-red"},
				}),
		).
		WithTarget(
			dash.PromQuery(
				`pdu_infeedVoltage{job="snmp_servertech_sentry3", region="${region}", pdu="${pdu}"} / 10`,
				"{{pdu}} > Unit {{towerIndex}} > Infeed {{infeedIndex}}",
			).RefId("A"),
		).
		WithTarget(
			dash.PromQuery(
				`pdu_st4PhaseVoltage{job="snmp_servertech_sentry4", region="${region}", pdu="${pdu}"} / 10`,
				"{{pdu}} > {{st4PhaseLabel}}",
			).RefId("B"),
		)
}

// lineCurrentPanel plots current per input line against the 60 A rating. A
// line pinned at exactly 0 A while its siblings carry load is an open conductor.
func lineCurrentPanel() *timeseries.PanelBuilder {
	return metricPanel("${pdu} > Input line current", dash.UnitAmps, "Last *").
		Id(11).
		Description("Per input line, against the 60 A rating (48 A continuous). A line pinned at 0 A while its siblings carry load is an open conductor.").
		GridPos(dashboard.GridPos{H: 8, W: 12, X: 12, Y: 26}).
		Min(0).
		Thresholds(currentThresholds(lineRatingAmps)).
		WithTarget(
			dash.PromQuery(
				`pdu_infeedLoadValue{job="snmp_servertech_sentry3", region="${region}", pdu="${pdu}"} / 100`,
				"{{pdu}} > Unit {{towerIndex}} > Infeed {{infeedIndex}}",
			).RefId("A"),
		).
		WithTarget(
			dash.PromQuery(
				`pdu_st4LineCurrent{job="snmp_servertech_sentry4", region="${region}", pdu="${pdu}"} / 100`,
				"{{pdu}} > {{st4LineLabel}}",
			).RefId("B"),
		)
}

// branchCurrentPanel plots current per 20 A branch breaker.
func branchCurrentPanel() *timeseries.PanelBuilder {
	return metricPanel("${pdu} > Branch breaker current", dash.UnitAmps, "Last *").
		Id(12).
		Description("Per 20 A branch breaker (16 A continuous). BR1/BR4 are on L1-L2, BR2/BR5 on L2-L3, BR3/BR6 on L3-L1.").
		GridPos(dashboard.GridPos{H: 8, W: 12, X: 0, Y: 34}).
		Min(0).
		Thresholds(currentThresholds(branchRatingAmps)).
		WithTarget(
			dash.PromQuery(
				`pdu_st4BranchCurrent{job="snmp_servertech_sentry4", region="${region}", pdu="${pdu}"} / 100`,
				"{{pdu}} > {{st4BranchLabel}}",
			).RefId("A"),
		)
}

// sentry4StatusMappings maps Sentry4 DeviceStatus codes to names and colors.
func sentry4StatusMappings() []dashboard.ValueMapping {
	options := map[string]dashboard.ValueMappingResult{}
	for code, m := range map[string]struct{ text, color string }{
		"0":  {"normal", "green"},
		"11": {"pwrError", "red"},
		"12": {"breakerTripped", "dark-red"},
		"14": {"lowAlarm", "orange"},
		"16": {"highWarning", "yellow"},
		"17": {"highAlarm", "red"},
	} {
		options[code] = dashboard.ValueMappingResult{Text: new(m.text), Color: new(m.color)}
	}
	return []dashboard.ValueMapping{{
		ValueMap: &dashboard.ValueMap{
			Type:    dashboard.MappingTypeValueToText,
			Options: options,
		},
	}}
}

// branchStatusPanel is a state timeline of each breaker's Sentry4 status. A
// real trip is one long red bar. Rapid striping between normal, pwrError and
// breakerTripped across every breaker of one phase pair is the PDU misreading a
// floating phase, not breakers tripping.
func branchStatusPanel() *statetimeline.PanelBuilder {
	return statetimeline.NewPanelBuilder().
		Title("${pdu} > Branch breaker status").
		Id(13).
		Description("Sentry4 st4BranchStatus per breaker. A real trip is one long red bar. Rapid striping on all breakers of one phase pair means a floating phase, not tripped breakers.").
		Datasource(dash.Prometheus()).
		GridPos(dashboard.GridPos{H: 8, W: 12, X: 12, Y: 34}).
		Mappings(sentry4StatusMappings()).
		ColorScheme(dashboard.NewFieldColorBuilder().Mode(dashboard.FieldColorModeIdThresholds)).
		Thresholds(
			dashboard.NewThresholdsConfigBuilder().
				Mode(dashboard.ThresholdsModeAbsolute).
				Steps([]dashboard.Threshold{
					{Value: nil, Color: "green"},
					{Value: new(1.0), Color: "red"},
				}),
		).
		ShowValue(common.VisibilityModeNever).
		RowHeight(0.8).
		MergeValues(true).
		AlignValue(common.TimelineValueAlignmentLeft).
		Legend(
			common.NewVizLegendOptionsBuilder().
				DisplayMode(common.LegendDisplayModeList).
				Placement(common.LegendPlacementBottom).
				ShowLegend(true),
		).
		Tooltip(
			common.NewVizTooltipOptionsBuilder().
				Mode(common.TooltipDisplayModeSingle).
				Sort(common.SortOrderNone),
		).
		WithTarget(
			dash.PromQuery(
				`pdu_st4BranchStatus{job="snmp_servertech_sentry4", region="${region}", pdu="${pdu}"}`,
				"{{st4BranchLabel}}",
			).RefId("A"),
		)
}

// utilizationPanel shows the PDU-reported percent of rating for each input
// line, branch breaker and cord.
func utilizationPanel() *bargauge.PanelBuilder {
	return bargauge.NewPanelBuilder().
		Title("${pdu} > Utilization").
		Id(14).
		Description("Percent of rating as reported by the PDU: lines of 60 A, branch breakers of 20 A, cords of their power rating.").
		Datasource(dash.Prometheus()).
		GridPos(dashboard.GridPos{H: 8, W: 12, X: 0, Y: 42}).
		Unit(dash.UnitPercent).
		Min(0).
		Max(100).
		Thresholds(warnCritThresholds(70, 80)).
		Orientation(common.VizOrientationHorizontal).
		DisplayMode(common.BarGaugeDisplayModeGradient).
		ShowUnfilled(true).
		ReduceOptions(
			common.NewReduceDataOptionsBuilder().
				Calcs([]string{"lastNotNull"}).
				Values(false),
		).
		WithTarget(
			dash.PromQuery(
				`pdu_st4LineCurrentUtilized{job="snmp_servertech_sentry4", region="${region}", pdu="${pdu}"} / 10`,
				"Line {{st4LineLabel}}",
			).RefId("A"),
		).
		WithTarget(
			dash.PromQuery(
				`pdu_st4BranchCurrentUtilized{job="snmp_servertech_sentry4", region="${region}", pdu="${pdu}"} / 10`,
				"Breaker {{st4BranchLabel}}",
			).RefId("B"),
		).
		WithTarget(
			dash.PromQuery(
				`pdu_st4InputCordPowerUtilized{job="snmp_servertech_sentry4", region="${region}", pdu="${pdu}"} / 10`,
				"Cord {{st4UnitName}}",
			).RefId("C"),
		)
}

// phasePowerPanel shows which phase pairs actually carry load. Dead pairs drop
// to zero while the sibling unit's matching pairs rise by the same amount.
func phasePowerPanel() *timeseries.PanelBuilder {
	return powerPanel("${pdu} > Active power by phase pair", "Last *").
		Id(15).
		Description("Dead phase pairs drop to zero while the sibling unit's matching pairs rise by the same amount at the same instant.").
		GridPos(dashboard.GridPos{H: 8, W: 12, X: 12, Y: 42}).
		Min(0).
		ThresholdsStyle(
			common.NewGraphThresholdsStyleConfigBuilder().
				Mode(common.GraphThresholdsStyleModeOff),
		).
		WithTarget(
			dash.PromQuery(
				`pdu_st4PhaseActivePower{job="snmp_servertech_sentry4", region="${region}", pdu="${pdu}"}`,
				"{{pdu}} > {{st4PhaseLabel}}",
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
		GridPos(dashboard.GridPos{H: 1, W: 24, X: 0, Y: 17}).
		Repeat("pdu")
}

func regionVariable() *dashboard.QueryVariableBuilder {
	return dash.QueryVar("region", `label_values(pdu_snmp_scrape_pdus_returned,region)`).
		Label("Region").
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		Current(dash.SelectedOption("us-sjc", "us-sjc"))
}

func pduVariable() *dashboard.QueryVariableBuilder {
	return dashboard.NewQueryVariableBuilder("pdu").
		Datasource(dash.Prometheus()).
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
		WithVariable(regionVariable()).
		WithVariable(pduVariable()).
		WithRow(overviewRow()).
		WithPanel(totalInputPowerByRackPanel()).
		WithPanel(busiestUnitLoadSharePanel()).
		WithPanel(abnormalStatusPanel()).
		WithPanel(phaseVoltageSpreadPanel()).
		WithPanel(lineCurrentZeroPanel()).
		WithRow(rackRow()).
		WithPanel(totalInputPowerByUnitPanel()).
		WithPanel(outputPowerPanel()).
		WithPanel(phaseVoltagePanel()).
		WithPanel(lineCurrentPanel()).
		WithPanel(branchCurrentPanel()).
		WithPanel(branchStatusPanel()).
		WithPanel(utilizationPanel()).
		WithPanel(phasePowerPanel()).
		Build()
}

func main() {
	dash.MustMarshal(build())
}
