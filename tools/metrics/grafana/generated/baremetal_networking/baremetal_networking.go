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

// External uplinks. The "External Throughput" panel sums only these
// device + interface pairs so it tracks traffic leaving the DC rather
// than aggregate device throughput.
const (
	externalDevices    = "[a-z]{3}-s[12]"
	externalInterfaces = "Ethernet1/1|Ethernet31/1"
)

const routerDataplaneInterfaces = "WAN.*|P2P.*"

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

func multicastByDevicePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Multicast packets by device", dash.UnitPacketsPerSec).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_in_multicast_pkts{region="${region}", interface_name!~"Port-Channel.*"}[1m])) by (source)`,
			"ingress {{source}}",
		)).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_out_multicast_pkts{region="${region}", interface_name!~"Port-Channel.*"}[1m])) by (source)`,
			"egress {{source}}",
		))
}

// --- Control plane (BGP) ---

func bgpSessionsEstablishedPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("BGP sessions established", dash.UnitShort).
		Description("Count of BGP neighbors in ESTABLISHED state per device. Drops indicate session flaps or peer failures.").
		WithTarget(dash.PromQuery(
			`sum(network_instances_network_instance_protocols_protocol_bgp_neighbors_neighbor_state_session_state{region="${region}", session_state="ESTABLISHED"}) by (source)`,
			"{{source}}",
		))
}

func bgpSessionStateBreakdownPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("BGP sessions by state", dash.UnitShort).
		Description("Breakdown of BGP session states across the fleet. ACTIVE / CONNECT / OPEN_SENT generally indicate a session struggling to come up.").
		WithTarget(dash.PromQuery(
			`sum(network_instances_network_instance_protocols_protocol_bgp_neighbors_neighbor_state_session_state{region="${region}"}) by (session_state)`,
			"{{session_state}}",
		))
}

func bgpPrefixesReceivedPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("BGP prefixes received", dash.UnitShort).
		Description("Prefix count received per device, summed across neighbors and address families. Sudden drops to zero indicate session loss or filter mistake.").
		WithTarget(dash.PromQuery(
			`sum(network_instances_network_instance_protocols_protocol_bgp_neighbors_neighbor_afi_safis_afi_safi_state_prefixes_received{region="${region}"}) by (source)`,
			"{{source}}",
		))
}

// --- System health ---

func cpuUtilizationByDevicePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("CPU utilization by device", dash.UnitPercent).
		Description("Sum of per-process CPU utilization per switch.").
		WithTarget(dash.PromQuery(
			`sum(system_processes_process_state_cpu_utilization{region="${region}"}) by (source)`,
			"{{source}}",
		))
}

func memoryUtilizationByDevicePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Memory utilization by device", dash.UnitPercent).
		Description("Memory utilization per device, max across processes.").
		WithTarget(dash.PromQuery(
			`max(system_processes_process_state_memory_utilization{region="${region}"}) by (source)`,
			"{{source}}",
		))
}

func temperatureByDevicePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Chassis temperature (max sensor)", "celsius").
		Description("Highest temperature reading among chassis sensors per device. Excludes per-port transceiver modules (those are shown per-interface), which on Arista arrive under the same metric with Ethernet*-named components.").
		WithTarget(dash.PromQuery(
			`max(components_component_state_temperature_instant{region="${region}", component_name!~"Ethernet.*"}) by (source)`,
			"{{source}}",
		))
}

func fanSpeedByDevicePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Fan speed (min/max)", "rpm").
		Description("Slowest and fastest fan per device. A wide spread or a fan stuck near zero is a fan failure.").
		WithTarget(dash.PromQuery(
			`min(components_component_fan_state_speed{region="${region}"}) by (source)`,
			"min {{source}}",
		)).
		WithTarget(dash.PromQuery(
			`max(components_component_fan_state_speed{region="${region}"}) by (source)`,
			"max {{source}}",
		))
}

func psuInputPowerByDevicePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("PSU input power by device", dash.UnitWatts).
		Description("Total AC input power drawn by all PSUs per device. Source: the platform inputPower property (decimal string) cast to float by the gnmic event-convert processor.").
		WithTarget(dash.PromQuery(
			`sum(components_component_properties_property_state_value{region="${region}", property_name="inputPower"}) by (source)`,
			"{{source}}",
		))
}

func psuActiveByDevicePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("PSUs active by device", dash.UnitShort).
		Description("Count of PSUs reporting oper-status ACTIVE per device. Most leaves/spines have two PSUs; a drop to 1 indicates a failed or unplugged PSU.").
		WithTarget(dash.PromQuery(
			`sum(components_component_state_oper_status{region="${region}", component_name=~"PowerSupply.*", oper_status="openconfig-platform-types:ACTIVE"}) by (source)`,
			"{{source}}",
		))
}

// --- Edge routers (TNSR) ---

func routerWanThroughputPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("WAN throughput by router", dash.UnitBitsPerSec).
		Description("Traffic on the upstream provider links. This is the DC's internet ingress/egress as seen from the edge routers.").
		WithTarget(dash.PromQuery(
			`sum(rate(router_ifHCInOctets{region="${region}", ifName=~"WAN.*"}[1m])) by (router) * 8`,
			"ingress {{router}}",
		)).
		WithTarget(dash.PromQuery(
			`sum(rate(router_ifHCOutOctets{region="${region}", ifName=~"WAN.*"}[1m])) by (router) * 8`,
			"egress {{router}}",
		))
}

func routerTrafficByInterfacePanel() *timeseries.PanelBuilder {
	in := fmt.Sprintf(
		`sum(rate(router_ifHCInOctets{region="${region}", ifName=~%q}[1m])) by (router, ifName) * 8`,
		routerDataplaneInterfaces,
	)
	out := fmt.Sprintf(
		`sum(rate(router_ifHCOutOctets{region="${region}", ifName=~%q}[1m])) by (router, ifName) * 8`,
		routerDataplaneInterfaces,
	)
	return dash.Timeseries("Router traffic by interface", dash.UnitBitsPerSec).
		WithTarget(dash.PromQuery(in, "ingress {{router}} {{ifName}}")).
		WithTarget(dash.PromQuery(out, "egress {{router}} {{ifName}}"))
}

func routerBgpSessionsPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Router BGP sessions established", dash.UnitShort).
		Description("Established BGP sessions per router and peer group. SPINES are the iBGP-facing fabric sessions; UPSTREAM are the internet providers. Each router should hold all its sessions — any drop is a session down.").
		WithTarget(dash.PromQuery(
			`sum(router_frr_bgp_peer_state{region="${region}"} == 1) by (router, peer_group)`,
			"{{router}} {{peer_group}}",
		))
}

func routerBgpPrefixesReceivedPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Router BGP prefixes received", dash.UnitShort).
		Description("Prefixes received per router and peer group. UPSTREAM carries the provider routes — a sudden drop to zero with the session still up indicates a filtering problem.").
		WithTarget(dash.PromQuery(
			`sum(router_frr_bgp_peer_prefixes_received_count_total{region="${region}"}) by (router, peer_group)`,
			"{{router}} {{peer_group}}",
		))
}

func routerBfdSessionsPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Router BFD sessions", dash.UnitShort).
		Description("VPP BFD sessions per router (one per spine link). BFD state lives in VPP rather than FRR on TNSR, so only the session count is exported.").
		WithTarget(dash.PromQuery(
			`router_bfd_udp4_sessions{region="${region}"}`,
			"{{router}}",
		))
}

func routerErrorsAndDiscardsPanel() *timeseries.PanelBuilder {
	q := func(metric string) string {
		return fmt.Sprintf(
			`sum(rate(%s{region="${region}", ifName=~%q}[1m])) by (router, ifName)`,
			metric, routerDataplaneInterfaces,
		)
	}
	return dash.Timeseries("Router errors and discards", dash.UnitEventsPerSec).
		WithTarget(dash.PromQuery(q("router_ifInErrors"), "ingress errors {{router}} {{ifName}}")).
		WithTarget(dash.PromQuery(q("router_ifInDiscards"), "ingress discards {{router}} {{ifName}}")).
		WithTarget(dash.PromQuery(q("router_ifOutErrors"), "egress errors {{router}} {{ifName}}")).
		WithTarget(dash.PromQuery(q("router_ifOutDiscards"), "egress discards {{router}} {{ifName}}"))
}

func routerDataplaneDropsPanel() *timeseries.PanelBuilder {
	// The VPP exporter keys per-interface vectors by sw_if_index; join
	// against the if_names_info mapping to get interface names.
	q := func(metric string) string {
		return fmt.Sprintf(
			`sum(rate(%s{region="${region}"}[1m])`+
				` * on(router, interface) group_left(name)`+
				` label_replace(router_if_names_info{region="${region}", name=~%q}, "interface", "$1", "index", "(.+)")) by (router, name)`,
			metric, routerDataplaneInterfaces,
		)
	}
	return dash.Timeseries("Router dataplane drops", dash.UnitPacketsPerSec).
		Description("VPP per-interface drop counters. rx-miss means the NIC ran out of descriptors (host couldn't keep up); rx-no-buf means VPP ran out of buffers. Both indicate the dataplane is shedding load.").
		WithTarget(dash.PromQuery(q("router_if_rx_miss"), "rx miss {{router}} {{name}}")).
		WithTarget(dash.PromQuery(q("router_if_rx_no_buf"), "rx no-buf {{router}} {{name}}")).
		WithTarget(dash.PromQuery(q("router_if_drops"), "drops {{router}} {{name}}"))
}

func routerInterfacesDownPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Router interfaces down", dash.UnitShort).
		Description("Dataplane interfaces that are admin-UP but not oper-UP (ifOperStatus: 1 = up).").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(
				`(router_ifOperStatus{region="${region}", ifName=~%q} != 1)`+
					` and on(router, ifName) (router_ifAdminStatus{region="${region}"} == 1)`,
				routerDataplaneInterfaces,
			),
			"{{router}} {{ifName}}",
		))
}

func routerCpuUtilizationPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Router CPU utilization", dash.UnitPercent).
		Description("Host CPU from node_exporter. Note the DPDK poll-mode workers spin at 100% on their pinned cores by design; watch for changes in the remaining headroom rather than the absolute level.").
		WithTarget(dash.PromQuery(
			`100 * (1 - avg by (router) (rate(node_cpu_seconds_total{job="node_router", region="${region}", mode="idle"}[1m])))`,
			"{{router}}",
		))
}

func routerMemoryUtilizationPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Router memory utilization", dash.UnitPercent).
		WithTarget(dash.PromQuery(
			`100 * (1 - node_memory_MemAvailable_bytes{job="node_router", region="${region}"}`+
				` / node_memory_MemTotal_bytes{job="node_router", region="${region}"})`,
			"{{router}}",
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

func multicastByInterfacePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Multicast packets by interface", dash.UnitPacketsPerSec).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_in_multicast_pkts{region="${region}", source="${device}"}[1m])) by (interface_name)`,
			"ingress {{interface_name}}",
		)).
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_out_multicast_pkts{region="${region}", source="${device}"}[1m])) by (interface_name)`,
			"egress {{interface_name}}",
		))
}

func interfaceFlapsPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Interface flaps (carrier transitions/min)", dash.UnitEventsPerSec).
		Description("Rate of carrier transitions per interface. Persistent flaps usually mean a dying optic or bad fiber pull.").
		WithTarget(dash.PromQuery(
			`sum(rate(interfaces_interface_state_counters_carrier_transitions{region="${region}", source="${device}"}[5m])) by (interface_name)`,
			"{{interface_name}}",
		))
}

func interfaceOperStatePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Interfaces down", dash.UnitShort).
		Description("Interfaces that are admin-UP, not oper-UP, and have received any traffic at some point (in_octets > 0). The traffic filter excludes never-connected access ports from the count — any port that's even briefly been linked gets LLDP/BPDU frames and accumulates octets. Note: counters reset on switch reboot.").
		WithTarget(dash.PromQuery(
			`(interfaces_interface_state_oper_status{region="${region}", source="${device}", oper_status!="UP"} == 1)`+
				` and on(source, interface_name) (interfaces_interface_state_admin_status{admin_status="UP"} == 1)`+
				` and on(source, interface_name) (interfaces_interface_state_counters_in_octets > 0)`,
			"{{interface_name}}",
		))
}

func queueDropsByInterfacePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Egress queue drops by interface", dash.UnitPacketsPerSec).
		Description("Per-queue dropped packet rate. Surfaces microburst drops that interface-level out-discards aggregates away.").
		WithTarget(dash.PromQuery(
			`sum(rate(qos_interfaces_interface_output_queues_queue_state_dropped_pkts{region="${region}", source="${device}"}[1m])) by (interface_id, name)`,
			"{{interface_id}} q{{name}}",
		))
}

func opticalRxPowerByInterfacePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Optical RX power by interface", "dBm").
		Description("Per-lane optical receive power. -8 to -12 dBm is typical for SR; -15+ is approaching the loss-of-signal threshold.").
		WithTarget(dash.PromQuery(
			`components_component_transceiver_physical_channels_channel_state_input_power_instant{region="${region}", source="${device}"}`,
			"{{component_name}} ch{{index}}",
		))
}

func opticalTxPowerByInterfacePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Optical TX power by interface", "dBm").
		WithTarget(dash.PromQuery(
			`components_component_transceiver_physical_channels_channel_state_output_power_instant{region="${region}", source="${device}"}`,
			"{{component_name}} ch{{index}}",
		))
}

func moduleTemperatureByInterfacePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Transceiver module temperature", "celsius").
		Description("Per-port transceiver module temperature. Arista doesn't populate the transceiver/state/module-temperature leaf; module temps arrive as Ethernet*-named sub-components under the chassis temperature subscription.").
		WithTarget(dash.PromQuery(
			`components_component_state_temperature_instant{region="${region}", source="${device}", component_name=~"Ethernet.*"}`,
			"{{component_name}}",
		))
}

// --- Template variables ---

func regionVariable() *dashboard.QueryVariableBuilder {
	return dash.QueryVar("region", `label_values(interfaces_interface_state_counters_in_octets,region)`).
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		Current(dash.SelectedOption("us-sjc", "us-sjc"))
}

func deviceVariable() *dashboard.QueryVariableBuilder {
	return dash.QueryVar("device", `label_values(interfaces_interface_state_counters_in_octets{region="${region}"},source)`).
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		IncludeAll(true).
		Current(dash.SelectedOption("All", "$__all"))
}

// --- Dashboard ---

func build() (dashboard.Dashboard, error) {
	controlPlaneRow := dashboard.NewRowBuilder("Control plane").
		Collapsed(true).
		WithPanel(bgpSessionsEstablishedPanel()).
		WithPanel(bgpSessionStateBreakdownPanel()).
		WithPanel(bgpPrefixesReceivedPanel())

	systemHealthRow := dashboard.NewRowBuilder("System health").
		Collapsed(true).
		WithPanel(cpuUtilizationByDevicePanel()).
		WithPanel(memoryUtilizationByDevicePanel()).
		WithPanel(temperatureByDevicePanel()).
		WithPanel(fanSpeedByDevicePanel()).
		WithPanel(psuInputPowerByDevicePanel()).
		WithPanel(psuActiveByDevicePanel())

	routersRow := dashboard.NewRowBuilder("Edge routers").
		Collapsed(true).
		WithPanel(routerWanThroughputPanel()).
		WithPanel(routerTrafficByInterfacePanel()).
		WithPanel(routerBgpSessionsPanel()).
		WithPanel(routerBgpPrefixesReceivedPanel()).
		WithPanel(routerBfdSessionsPanel()).
		WithPanel(routerErrorsAndDiscardsPanel()).
		WithPanel(routerDataplaneDropsPanel()).
		WithPanel(routerInterfacesDownPanel()).
		WithPanel(routerCpuUtilizationPanel()).
		WithPanel(routerMemoryUtilizationPanel())

	deviceRow := dashboard.NewRowBuilder("Device ${device}").
		Collapsed(true).
		Repeat("device").
		WithPanel(trafficByInterfacePanel()).
		WithPanel(errorRateByInterfacePanel()).
		WithPanel(errorsAndDiscardsByInterfacePanel()).
		WithPanel(packetsByInterfacePanel()).
		WithPanel(broadcastByInterfacePanel()).
		WithPanel(multicastByInterfacePanel()).
		WithPanel(interfaceFlapsPanel()).
		WithPanel(interfaceOperStatePanel()).
		WithPanel(queueDropsByInterfacePanel()).
		WithPanel(opticalRxPowerByInterfacePanel()).
		WithPanel(opticalTxPowerByInterfacePanel()).
		WithPanel(moduleTemperatureByInterfacePanel())

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
		WithPanel(multicastByDevicePanel()).
		WithRow(controlPlaneRow).
		WithRow(systemHealthRow).
		WithRow(routersRow).
		WithRow(deviceRow).
		Build()
}

func main() {
	dash.MustMarshal(build())
}
