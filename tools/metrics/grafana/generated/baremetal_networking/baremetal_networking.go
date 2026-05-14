// Generates a Grafana dashboard for baremetal switch network metrics.
//
// This program writes the resulting dashboard JSON to stdout. It is intended
// to be invoked from a Bazel genrule; see the BUILD file alongside this one.
package main

import (
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/dash"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
)

// SJC external uplinks. The "External Throughput" panel sums only these
// device + interface pairs so it tracks traffic leaving the DC rather
// than aggregate device throughput.
const (
	externalDevices    = "sjc-s1|sjc-s2"
	externalInterfaces = "Ethernet1/1|Ethernet31/1"
)

// --- Top-level (aggregate) panels ---

func externalThroughputPanel() *timeseries.PanelBuilder {
	in := fmt.Sprintf(
		`sum(rate(interfaces_interface_state_counters_in_octets{region="${region}", source=~%q, interface_name=~%q}[1m])) * 8`,
		externalDevices, externalInterfaces,
	)
	out := fmt.Sprintf(
		`sum(rate(interfaces_interface_state_counters_out_octets{region="${region}", source=~%q, interface_name=~%q}[1m])) * 8`,
		externalDevices, externalInterfaces,
	)
	return dash.Timeseries("External Throughput", dash.UnitBitsPerSec).
		WithTarget(dash.PromQuery(in, "ingress")).
		WithTarget(dash.PromQuery(out, "egress"))
}

func trafficByDevicePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Traffic by device", dash.UnitBitsPerSec).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_in_octets{region="${region}", interface_name!~"Port-Channel.*"}[1m])) by (source) * 8`,
			"ingress {{source}}",
		)).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_out_octets{region="${region}", interface_name!~"Port-Channel.*"}[1m])) by (source) * 8`,
			"egress {{source}}",
		))
}

func errorsAndDiscardsPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Errors and Discards", dash.UnitEventsPerSec).
		Description("Absolute rate of errors/discards. Prefer the 'Error rate' panel for detecting actual issues — a high absolute count on a busy link may still be a tiny fraction of total traffic.").
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_in_errors{region="${region}"}[1m])) by (source)`,
			"ingress errors {{source}}",
		)).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_in_discards{region="${region}"}[1m])) by (source)`,
			"ingress discards {{source}}",
		)).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_out_errors{region="${region}"}[1m])) by (source)`,
			"egress errors {{source}}",
		)).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_out_discards{region="${region}"}[1m])) by (source)`,
			"egress discards {{source}}",
		)).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_in_fcs_errors{region="${region}"}[1m])) by (source)`,
			"ingress fcs errors {{source}}",
		)).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_out_fcs_errors{region="${region}"}[1m])) by (source)`,
			"egress fcs errors {{source}}",
		))
}

func errorRateByDevicePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Error rate by device", dash.UnitPercent).
		Description("Errors and discards as a percentage of total packets, by device. More reliable than absolute counts for spotting real problems.").
		WithTarget(dash.PromQuery(
			`100 * sum(rate(interfaces_interface_state_counters_in_errors{region="${region}"}[1m])) by (source)`+
				` / sum(rate(interfaces_interface_state_counters_in_pkts{region="${region}"}[1m])) by (source)`,
			"ingress errors {{source}}",
		)).
		WithTarget(dash.PromQuery(
			`100 * sum(rate(interfaces_interface_state_counters_in_discards{region="${region}"}[1m])) by (source)`+
				` / sum(rate(interfaces_interface_state_counters_in_pkts{region="${region}"}[1m])) by (source)`,
			"ingress discards {{source}}",
		)).
		WithTarget(dash.PromQuery(
			`100 * sum(rate(interfaces_interface_state_counters_out_errors{region="${region}"}[1m])) by (source)`+
				` / sum(rate(interfaces_interface_state_counters_out_pkts{region="${region}"}[1m])) by (source)`,
			"egress errors {{source}}",
		)).
		WithTarget(dash.PromQuery(
			`100 * sum(rate(interfaces_interface_state_counters_out_discards{region="${region}"}[1m])) by (source)`+
				` / sum(rate(interfaces_interface_state_counters_out_pkts{region="${region}"}[1m])) by (source)`,
			"egress discards {{source}}",
		))
}

func packetsByDevicePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Packets by device", dash.UnitPacketsPerSec).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_in_pkts{region="${region}", interface_name!~"Port-Channel.*"}[1m])) by (source)`,
			"ingress {{source}}",
		)).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_out_pkts{region="${region}", interface_name!~"Port-Channel.*"}[1m])) by (source)`,
			"egress {{source}}",
		))
}

func broadcastByDevicePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Broadcast packets by device", dash.UnitPacketsPerSec).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_in_broadcast_pkts{region="${region}", interface_name!~"Port-Channel.*"}[1m])) by (source)`,
			"ingress {{source}}",
		)).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_out_broadcast_pkts{region="${region}", interface_name!~"Port-Channel.*"}[1m])) by (source)`,
			"egress {{source}}",
		))
}

// --- Per-device (per-interface) panels: live inside the repeated row ---

func trafficByInterfacePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Traffic by interface", dash.UnitBitsPerSec).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_in_octets{region="${region}", source="${device}"}[1m])) by (interface_name) * 8`,
			"ingress {{interface_name}}",
		)).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_out_octets{region="${region}", source="${device}"}[1m])) by (interface_name) * 8`,
			"egress {{interface_name}}",
		))
}

func errorsAndDiscardsByInterfacePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Errors and Discards by interface", dash.UnitEventsPerSec).
		Description("Absolute rate of errors/discards. Prefer the 'Error rate' panel for detecting actual issues — a high absolute count on a busy link may still be a tiny fraction of total traffic.").
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_in_errors{region="${region}", source="${device}"}[1m])) by (interface_name)`,
			"ingress errors {{interface_name}}",
		)).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_in_discards{region="${region}", source="${device}"}[1m])) by (interface_name)`,
			"ingress discards {{interface_name}}",
		)).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_out_errors{region="${region}", source="${device}"}[1m])) by (interface_name)`,
			"egress errors {{interface_name}}",
		)).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_out_discards{region="${region}", source="${device}"}[1m])) by (interface_name)`,
			"egress discards {{interface_name}}",
		)).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_in_fcs_errors{region="${region}", source="${device}"}[1m])) by (interface_name)`,
			"ingress fcs errors {{interface_name}}",
		)).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_out_fcs_errors{region="${region}", source="${device}"}[1m])) by (interface_name)`,
			"egress fcs errors {{interface_name}}",
		))
}

func errorRateByInterfacePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Error rate by interface", dash.UnitPercent).
		Description("Errors and discards as a percentage of total packets, by interface. More reliable than absolute counts for spotting real problems.").
		WithTarget(dash.PromQuery(
			`100 * sum(rate(interfaces_interface_state_counters_in_errors{region="${region}", source="${device}"}[1m])) by (interface_name)`+
				` / sum(rate(interfaces_interface_state_counters_in_pkts{region="${region}", source="${device}"}[1m])) by (interface_name)`,
			"ingress errors {{interface_name}}",
		)).
		WithTarget(dash.PromQuery(
			`100 * sum(rate(interfaces_interface_state_counters_in_discards{region="${region}", source="${device}"}[1m])) by (interface_name)`+
				` / sum(rate(interfaces_interface_state_counters_in_pkts{region="${region}", source="${device}"}[1m])) by (interface_name)`,
			"ingress discards {{interface_name}}",
		)).
		WithTarget(dash.PromQuery(
			`100 * sum(rate(interfaces_interface_state_counters_out_errors{region="${region}", source="${device}"}[1m])) by (interface_name)`+
				` / sum(rate(interfaces_interface_state_counters_out_pkts{region="${region}", source="${device}"}[1m])) by (interface_name)`,
			"egress errors {{interface_name}}",
		)).
		WithTarget(dash.PromQuery(
			`100 * sum(rate(interfaces_interface_state_counters_out_discards{region="${region}", source="${device}"}[1m])) by (interface_name)`+
				` / sum(rate(interfaces_interface_state_counters_out_pkts{region="${region}", source="${device}"}[1m])) by (interface_name)`,
			"egress discards {{interface_name}}",
		))
}

func packetsByInterfacePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Packets by interface", dash.UnitPacketsPerSec).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_in_pkts{region="${region}", source="${device}"}[1m])) by (interface_name)`,
			"ingress {{interface_name}}",
		)).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_out_pkts{region="${region}", source="${device}"}[1m])) by (interface_name)`,
			"egress {{interface_name}}",
		))
}

func broadcastByInterfacePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Broadcast packets by interface", dash.UnitPacketsPerSec).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_in_broadcast_pkts{region="${region}", source="${device}"}[1m])) by (interface_name)`,
			"ingress {{interface_name}}",
		)).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_out_broadcast_pkts{region="${region}", source="${device}"}[1m])) by (interface_name)`,
			"egress {{interface_name}}",
		))
}

// --- Template variables ---

func regionVariable() *dashboard.QueryVariableBuilder {
	return dash.QueryVar("region", `label_values(interfaces_interface_state_counters_in_octets,region)`).
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		Current(dash.SelectedOption("us-sjc", "us-sjc"))
}

func deviceVariable() *dashboard.QueryVariableBuilder {
	return dash.QueryVar("device", `label_values(interfaces_interface_state_counters_in_octets,source)`).
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		IncludeAll(true).
		Current(dash.SelectedOption("All", "$__all"))
}

// --- Dashboard ---

func build() (dashboard.Dashboard, error) {
	deviceRow := dashboard.NewRowBuilder("Device ${device}").
		Collapsed(true).
		Repeat("device").
		WithPanel(trafficByInterfacePanel()).
		WithPanel(errorRateByInterfacePanel()).
		WithPanel(errorsAndDiscardsByInterfacePanel()).
		WithPanel(packetsByInterfacePanel()).
		WithPanel(broadcastByInterfacePanel())

	return dashboard.NewDashboardBuilder("Baremetal Networking").
		Uid("df22a5nr1yio0e").
		Tags([]string{"generated", "file:baremetal-networking.json"}).
		Editable().
		Refresh("1m").
		Time("now-6h", "now").
		WithVariable(regionVariable()).
		WithVariable(deviceVariable()).
		WithRow(dashboard.NewRowBuilder("Overview")).
		WithPanel(externalThroughputPanel()).
		WithPanel(trafficByDevicePanel()).
		WithPanel(errorRateByDevicePanel()).
		WithPanel(errorsAndDiscardsPanel()).
		WithPanel(packetsByDevicePanel()).
		WithPanel(broadcastByDevicePanel()).
		WithRow(deviceRow).
		Build()
}

func main() {
	dash.MustMarshal(build())
}
