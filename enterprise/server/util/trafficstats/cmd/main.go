// This program refreshes the checked-in CSV files that list IP ranges for
// each cloud provider. Each CSV uses the format:
//
//	PROVIDER,REGION,CIDR
//
// Run it from the repository root:
//
//	bazel run //enterprise/server/util/trafficstats/cmd
//
// Or with go run:
//
//	go run enterprise/server/util/trafficstats/cmd/main.go
package main

import (
	"encoding/csv"
	"log"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/trafficstats/providerips"
)

func main() {
	dir := csvDir()
	providers := []struct {
		name string
		fn   func() ([]providerips.IPRange, error)
	}{
		{"aws", providerips.FetchAWS},
		{"azure", providerips.FetchAzure},
		{"digitalocean", providerips.FetchDigitalOcean},
		{"gcp", providerips.FetchGCP},
		{"github", providerips.FetchGitHub},
		{"hetzner", providerips.FetchHetzner},
		{"latitude", providerips.FetchLatitude},
		{"macstadium", providerips.FetchMacStadium},
		{"namespace", providerips.FetchNamespace},
		{"oracle", providerips.FetchOracle},
		{"ovh", providerips.FetchOVH},
		{"phoenixnap", providerips.FetchPhoenixNAP},
	}
	var all []providerips.IPRange
	for _, p := range providers {
		ranges, err := p.fn()
		if err != nil {
			log.Fatalf("%s: %v", p.name, err)
		}
		path := filepath.Join(dir, "data", p.name+".csv")
		if err := writeCSV(path, ranges); err != nil {
			log.Fatalf("%s: %v", p.name, err)
		}
		log.Printf("wrote %s", path)
		all = append(all, ranges...)
	}
	if n := reportOverlaps(all); n > 0 {
		log.Fatalf("%d cross-provider overlaps (CSVs were still written). The classifier returns the first provider in newClassifier's list order, so check that the order gives the intended result for these.", n)
	}
}

// Provider pairs that are known to overlap. GitHub's `actions` ranges are the
// Azure (and a few AWS) blocks that GitHub-hosted runners run in, and the
// classifier deliberately checks GitHub first.
var allowedOverlaps = map[[2]string]bool{
	{"github", "azure"}: true,
	{"github", "aws"}:   true,
}

func overlapAllowed(a, b string) bool {
	a, b = strings.ToLower(a), strings.ToLower(b)
	return allowedOverlaps[[2]string{a, b}] || allowedOverlaps[[2]string{b, a}]
}

// reportOverlaps logs every prefix that is contained in (or equal to) a prefix
// from a different provider, and returns how many it found. The BGP-derived
// lists in particular can pick up customer (BYOIP) space or broad aggregates
// that belong to another provider, and the classifier's first-match lookup
// would then silently attribute that traffic to whichever provider is checked
// first.
func reportOverlaps(ranges []providerips.IPRange) int {
	type entry struct {
		prefix   netip.Prefix
		provider string
	}
	entries := make([]entry, 0, len(ranges))
	for _, r := range ranges {
		p, err := netip.ParsePrefix(r.CIDR)
		if err != nil {
			log.Fatalf("%s: invalid prefix %q: %v", r.Provider, r.CIDR, err)
		}
		entries = append(entries, entry{p.Masked(), r.Provider})
	}
	// Sort by address, broader prefixes first, so that every prefix comes
	// right after the prefixes that contain it.
	slices.SortFunc(entries, func(a, b entry) int {
		if c := a.prefix.Addr().Compare(b.prefix.Addr()); c != 0 {
			return c
		}
		return a.prefix.Bits() - b.prefix.Bits()
	})
	n := 0
	var enclosing []entry
	for _, e := range entries {
		for len(enclosing) > 0 && !enclosing[len(enclosing)-1].prefix.Contains(e.prefix.Addr()) {
			enclosing = enclosing[:len(enclosing)-1]
		}
		for _, outer := range enclosing {
			if outer.provider != e.provider && !overlapAllowed(outer.provider, e.provider) {
				log.Printf("overlap: %s %s contains %s %s", outer.provider, outer.prefix, e.provider, e.prefix)
				n++
			}
		}
		enclosing = append(enclosing, e)
	}
	return n
}

// csvDir returns the directory containing the per-provider CSV subdirectories.
// It uses the source file location so that `go run` and `bazel run` both write
// to the correct place without needing flags.
func csvDir() string {
	_, src, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(src))
}

func writeCSV(path string, ranges []providerips.IPRange) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	for _, r := range ranges {
		if err := w.Write([]string{r.Provider, r.Region, r.CIDR}); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}
