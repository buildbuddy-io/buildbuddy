package runner

import (
	"net/netip"
	"slices"
	"sort"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/klauspost/compress/zstd"
)

const (
	maxNetworkDestinationSummaries = 20
	largeNetworkUploadBytes        = 100 * 1024
)

type networkDestinationReport struct {
	Destinations            []*networkDestinationSummary `json:"destinations"`
	SystemTraffic           *networkTrafficTotals        `json:"system_traffic,omitempty"`
	OmittedDestinationCount int                          `json:"omitted_destination_count,omitempty"`
	DetailedLog             string                       `json:"detailed_log,omitempty"`
}

type networkDestinationSummary struct {
	Hostname        string   `json:"hostname,omitempty"`
	Aliases         []string `json:"aliases,omitempty"`
	IPs             []string `json:"ips"`
	Port            uint16   `json:"port"`
	Protocol        string   `json:"protocol"`
	Category        string   `json:"category,omitempty"`
	BytesSent       int64    `json:"bytes_sent"`
	BytesReceived   int64    `json:"bytes_received"`
	ConnectionCount int64    `json:"connection_count"`
	Flags           []string `json:"flags,omitempty"`
}

type networkTrafficTotals struct {
	DestinationCount int   `json:"destination_count"`
	BytesSent        int64 `json:"bytes_sent"`
	BytesReceived    int64 `json:"bytes_received"`
	ConnectionCount  int64 `json:"connection_count"`
}

type networkDestinationGroupKey struct {
	hostname string
	ip       string
	port     uint16
	protocol string
}

func buildNetworkDestinationReport(destinations []*container.NetworkDestination, detailedLog string) *networkDestinationReport {
	groups := make(map[networkDestinationGroupKey]*networkDestinationSummary)
	systemTraffic := &networkTrafficTotals{}
	for _, destination := range destinations {
		if isSystemNetworkDestination(destination) {
			systemTraffic.DestinationCount++
			systemTraffic.BytesSent += destination.BytesSent
			systemTraffic.BytesReceived += destination.BytesReceived
			systemTraffic.ConnectionCount += destination.ConnectionCount
			continue
		}
		key := networkDestinationGroupKey{
			hostname: destination.Hostname,
			port:     destination.Port,
			protocol: destination.Protocol,
		}
		if key.hostname == "" {
			key.ip = destination.IP
		}
		summary, ok := groups[key]
		if !ok {
			summary = &networkDestinationSummary{
				Hostname: destination.Hostname,
				Port:     destination.Port,
				Protocol: destination.Protocol,
			}
			if isTelemetryNetworkDestination(destination) {
				summary.Category = "telemetry"
			}
			groups[key] = summary
		}
		appendUnique(&summary.IPs, destination.IP)
		for _, alias := range destination.Aliases {
			if alias != summary.Hostname {
				appendUnique(&summary.Aliases, alias)
			}
		}
		summary.BytesSent += destination.BytesSent
		summary.BytesReceived += destination.BytesReceived
		summary.ConnectionCount += destination.ConnectionCount
	}

	summaries := make([]*networkDestinationSummary, 0, len(groups))
	for _, summary := range groups {
		sort.Strings(summary.Aliases)
		sort.Strings(summary.IPs)
		if summary.Hostname == "" {
			summary.Flags = append(summary.Flags, "unknown_hostname")
		}
		if summary.BytesSent >= largeNetworkUploadBytes {
			summary.Flags = append(summary.Flags, "large_upload")
		}
		summaries = append(summaries, summary)
	}
	sort.Slice(summaries, func(i, j int) bool {
		if summaries[i].BytesSent != summaries[j].BytesSent {
			return summaries[i].BytesSent > summaries[j].BytesSent
		}
		if summaries[i].BytesReceived != summaries[j].BytesReceived {
			return summaries[i].BytesReceived > summaries[j].BytesReceived
		}
		return networkDestinationSummaryName(summaries[i]) < networkDestinationSummaryName(summaries[j])
	})

	report := &networkDestinationReport{DetailedLog: detailedLog}
	if systemTraffic.DestinationCount > 0 {
		report.SystemTraffic = systemTraffic
	}
	if len(summaries) > maxNetworkDestinationSummaries {
		report.OmittedDestinationCount = len(summaries) - maxNetworkDestinationSummaries
		summaries = summaries[:maxNetworkDestinationSummaries]
	}
	report.Destinations = summaries
	return report
}

func isSystemNetworkDestination(destination *container.NetworkDestination) bool {
	if destination.Port == 53 {
		return true
	}
	ip, err := netip.ParseAddr(destination.IP)
	if err != nil {
		return false
	}
	return ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast()
}

func isTelemetryNetworkDestination(destination *container.NetworkDestination) bool {
	names := append([]string{destination.Hostname}, destination.Aliases...)
	for _, name := range names {
		for _, suffix := range []string{"datadoghq.com", "sentry.io", "newrelic.com"} {
			if name == suffix || strings.HasSuffix(name, "."+suffix) {
				return true
			}
		}
	}
	return false
}

func appendUnique(values *[]string, value string) {
	if value != "" && !slices.Contains(*values, value) {
		*values = append(*values, value)
	}
}

func networkDestinationSummaryName(summary *networkDestinationSummary) string {
	if summary.Hostname != "" {
		return summary.Hostname
	}
	if len(summary.IPs) > 0 {
		return summary.IPs[0]
	}
	return ""
}

func compressNetworkDestinationReport(report []byte) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	defer encoder.Close()
	return encoder.EncodeAll(report, nil), nil
}
