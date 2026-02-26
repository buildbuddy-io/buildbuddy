// Package dnstypes defines lightweight types used by the DNS override
// pipeline. It exists as a separate package so that lightweight binaries
// (e.g. goinit) can reference these types without pulling in the full
// networking package and its heavy transitive dependencies (tracing,
// metrics, prometheus, netlink, etc.).
package dnstypes

// DNSOverride describes a single hostname-level DNS override.
type DNSOverride struct {
	HostnameToOverride string `yaml:"hostname_to_override"`
	RedirectToHostname string `yaml:"redirect_to_hostname"`
}
