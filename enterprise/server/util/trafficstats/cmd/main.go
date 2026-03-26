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
	"os"
	"path/filepath"
	"runtime"

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
		{"gcp", providerips.FetchGCP},
		{"github", providerips.FetchGitHub},
		{"macstadium", providerips.FetchMacStadium},
	}
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
	}
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
