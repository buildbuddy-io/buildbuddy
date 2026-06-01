// Generates a Grafana dashboard for disk performance and utilization metrics.
//
// Most panels pull from Prometheus node_exporter; block I/O latency panels
// pull from ebpf_exporter's biolatency histogram.
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

// Label filter applied to node_exporter and ebpf_exporter queries.
// Uses the template variables defined in build().
const (
	nodeFilter = `region=~"$region",rack=~"$rack",platform=~"$platform",virt=~"$virt",pool=~"$pool",instance=~"$instance"`
	diskFilter = `region=~"$region",rack=~"$rack",platform=~"$platform",virt=~"$virt",pool=~"$pool",instance=~"$instance",device=~"$device"`
)

// --- Panels ---

func readThroughputPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Disk read throughput", dash.UnitBytesPerSec).
		Description("Bytes read from disk per second, by device.").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`rate(node_disk_read_bytes_total{%s}[$__rate_interval])`, diskFilter),
			"{{instance}} {{device}}",
		))
}

func writeThroughputPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Disk write throughput", dash.UnitBytesPerSec).
		Description("Bytes written to disk per second, by device.").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`rate(node_disk_written_bytes_total{%s}[$__rate_interval])`, diskFilter),
			"{{instance}} {{device}}",
		))
}

func readIOPSPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Disk read IOPS", dash.UnitShort).
		Description("Read operations completed per second, by device.").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`rate(node_disk_reads_completed_total{%s}[$__rate_interval])`, diskFilter),
			"{{instance}} {{device}}",
		))
}

func writeIOPSPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Disk write IOPS", dash.UnitShort).
		Description("Write operations completed per second, by device.").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`rate(node_disk_writes_completed_total{%s}[$__rate_interval])`, diskFilter),
			"{{instance}} {{device}}",
		))
}

func queueDepthPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Average queue depth", dash.UnitShort).
		Description("Average number of I/O requests queued + in-service (aqu-sz from iostat). Leading indicator of saturation — climbs before latency does.").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`rate(node_disk_io_time_weighted_seconds_total{%s}[$__rate_interval])`, diskFilter),
			"{{instance}} {{device}}",
		)).
		Thresholds(
			dashboard.NewThresholdsConfigBuilder().
				Mode(dashboard.ThresholdsModeAbsolute).
				Steps([]dashboard.Threshold{
					{Value: nil, Color: "green"},
					{Value: new(4.0), Color: "orange"},
					{Value: new(16.0), Color: "red"},
				}),
		)
}

func ioUtilPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Disk %util", dash.UnitPercent).
		Description("Fraction of time the device had at least one I/O in flight. Caveat: misleading for NVMe, which can service many parallel requests — prefer queue depth and throughput-vs-rated for saturation.").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`rate(node_disk_io_time_seconds_total{%s}[$__rate_interval]) * 100`, diskFilter),
			"{{instance}} {{device}}",
		))
}

func bioLatencyPanel(title, op string, quantile float64) *timeseries.PanelBuilder {
	return dash.Timeseries(title, dash.UnitSeconds).
		Description(fmt.Sprintf("%s percentile block I/O %s latency, from the eBPF biolatency histogram.",
			quantileLabel(quantile), op)).
		WithTarget(dash.PromQuery(
			fmt.Sprintf(
				`histogram_quantile(%.2f, sum by (le,instance,device) (rate(ebpf_exporter_bio_latency_seconds_bucket{%s,operation=%q}[$__rate_interval])))`,
				quantile, diskFilter, op,
			),
			"{{instance}} {{device}}",
		))
}

func quantileLabel(q float64) string {
	return fmt.Sprintf("p%d", int(q*100+0.5))
}

func iowaitPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("CPU iowait", dash.UnitPercent).
		Description("Percentage of CPU time spent waiting on I/O. High iowait means applications are blocked on disk.").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(
				`avg by (instance) (rate(node_cpu_seconds_total{%s,mode="iowait"}[$__rate_interval])) * 100`,
				nodeFilter,
			),
			"{{instance}}",
		))
}

func diskSpacePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Filesystem used", dash.UnitPercent).
		Description("Percentage of filesystem space used.").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(
				`100 * (1 - node_filesystem_avail_bytes{%s,fstype!~"tmpfs|overlay|rootfs"} / node_filesystem_size_bytes{%s,fstype!~"tmpfs|overlay|rootfs"})`,
				nodeFilter, nodeFilter,
			),
			"{{instance}} {{mountpoint}}",
		)).
		Thresholds(
			dashboard.NewThresholdsConfigBuilder().
				Mode(dashboard.ThresholdsModeAbsolute).
				Steps([]dashboard.Threshold{
					{Value: nil, Color: "green"},
					{Value: new(80.0), Color: "orange"},
					{Value: new(90.0), Color: "red"},
				}),
		)
}

// --- Template variables ---

func queryVar(name, label, query string) *dashboard.QueryVariableBuilder {
	return dash.QueryVar(name, query).
		Label(label).
		Refresh(dashboard.VariableRefreshOnTimeRangeChanged).
		Multi(true).
		IncludeAll(true).
		AllValue(".*")
}

// --- Dashboard ---

func build() (dashboard.Dashboard, error) {
	return dashboard.NewDashboardBuilder("Disk Performance").
		Uid("disk-performance").
		Description("Per-device disk throughput, IOPS, queue depth, block I/O latency, and related saturation signals.").
		Tags([]string{"generated", "storage", "disk", "file:disk-performance.json"}).
		Editable().
		Refresh("30s").
		Time("now-1h", "now").
		Tooltip(dashboard.DashboardCursorSyncCrosshair).
		WithVariable(queryVar("region", "Region",
			`label_values(node_uname_info, region)`)).
		WithVariable(queryVar("rack", "Rack",
			`label_values(node_uname_info{region=~"$region"}, rack)`)).
		WithVariable(queryVar("platform", "Platform",
			`label_values(node_uname_info{region=~"$region",rack=~"$rack"}, platform)`)).
		WithVariable(queryVar("virt", "Virt",
			`label_values(node_uname_info{region=~"$region",rack=~"$rack",platform=~"$platform"}, virt)`)).
		WithVariable(queryVar("pool", "Pool",
			`label_values(node_uname_info{region=~"$region",rack=~"$rack",platform=~"$platform",virt=~"$virt"}, pool)`)).
		WithVariable(queryVar("instance", "Node",
			`label_values(node_uname_info{region=~"$region",rack=~"$rack",platform=~"$platform",virt=~"$virt",pool=~"$pool"}, instance)`)).
		WithVariable(queryVar("device", "Device",
			`label_values(node_disk_read_bytes_total{region=~"$region",rack=~"$rack",platform=~"$platform",virt=~"$virt",pool=~"$pool",instance=~"$instance"}, device)`)).
		WithRow(dashboard.NewRowBuilder("Throughput")).
		WithPanel(readThroughputPanel()).
		WithPanel(writeThroughputPanel()).
		WithRow(dashboard.NewRowBuilder("IOPS")).
		WithPanel(readIOPSPanel()).
		WithPanel(writeIOPSPanel()).
		WithRow(dashboard.NewRowBuilder("Saturation")).
		WithPanel(queueDepthPanel()).
		WithPanel(ioUtilPanel()).
		WithRow(dashboard.NewRowBuilder("Block I/O latency (reads)")).
		WithPanel(bioLatencyPanel("Read latency p95", "read", 0.95)).
		WithPanel(bioLatencyPanel("Read latency p99", "read", 0.99)).
		WithRow(dashboard.NewRowBuilder("Block I/O latency (writes)")).
		WithPanel(bioLatencyPanel("Write latency p95", "write", 0.95)).
		WithPanel(bioLatencyPanel("Write latency p99", "write", 0.99)).
		WithRow(dashboard.NewRowBuilder("System")).
		WithPanel(iowaitPanel()).
		WithPanel(diskSpacePanel()).
		Build()
}

func main() {
	dash.MustMarshal(build())
}
