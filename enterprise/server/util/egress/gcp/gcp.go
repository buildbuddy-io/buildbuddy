package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
)

type gcpIPRanges struct {
	Prefixes []struct {
		IPv4Prefix string `json:"ipv4Prefix"`
		IPv6Prefix string `json:"ipv6Prefix"`
		Scope      string `json:"scope"`
	} `json:"prefixes"`
}

// This simple Go program downloads the set of GCP IP ranges from
// https://www.gstatic.com/ipranges/cloud.json and then converts them into
// CSV format:
// GCP,<REGION>,<IP-BLOCK>
//
// These ranges are checked in, in gcp.csv. Refresh them by running:
// bazel run //enterprise/server/util/egress/gcp -- --output=$PWD/enterprise/server/util/egress/gcp/gcp.csv
func main() {
	url := flag.String("url", "https://www.gstatic.com/ipranges/cloud.json", "URL from which the GCP IP ranges JSON can be read")
	outputPath := flag.String("output", "", "Path to output CSV")
	flag.Parse()

	if *url == "" || *outputPath == "" {
		log.Fatal("--url and --output are required")
	}

	resp, err := http.Get(*url)
	if err != nil {
		log.Fatalf("fetch input: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("fetch input: unexpected status %s", resp.Status)
	}

	var ranges gcpIPRanges
	if err := json.NewDecoder(resp.Body).Decode(&ranges); err != nil {
		log.Fatalf("decode JSON: %v", err)
	}

	output, err := os.Create(*outputPath)
	if err != nil {
		log.Fatalf("create output: %v", err)
	}
	defer output.Close()

	w := csv.NewWriter(output)
	defer w.Flush()

	if err := w.Write([]string{"GCP", "REGION", "IP-BLOCK"}); err != nil {
		log.Fatalf("write header: %v", err)
	}
	for _, p := range ranges.Prefixes {
		if p.IPv4Prefix != "" {
			if err := w.Write([]string{"GCP", p.Scope, p.IPv4Prefix}); err != nil {
				log.Fatalf("write IPv4 row: %v", err)
			}
		}
		if p.IPv6Prefix != "" {
			if err := w.Write([]string{"GCP", p.Scope, p.IPv6Prefix}); err != nil {
				log.Fatalf("write IPv6 row: %v", err)
			}
		}
	}

	if err := w.Error(); err != nil {
		log.Fatalf("flush CSV: %v", err)
	}
}
