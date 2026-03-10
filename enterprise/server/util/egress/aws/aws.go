package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
)

type awsIPRanges struct {
	Prefixes []struct {
		IPPrefix string `json:"ip_prefix"`
		Region   string `json:"region"`
	} `json:"prefixes"`
	IPv6Prefixes []struct {
		IPv6Prefix string `json:"ipv6_prefix"`
		Region     string `json:"region"`
	} `json:"ipv6_prefixes"`
}

// This simple Go program downloads the set of AWS IP ranges from
// https://ip-ranges.amazonaws.com/ip-ranges.json and then converts them into
// CSV format:
// AWS,<REGION>,<IP-BLOCK>
//
// These ranges are checked in, in aws.csv. Refresh them by running:
// bazel run //enterprise/server/util/egress/aws -- --output=$PWD/enterprise/server/util/egress/aws/aws.csv
func main() {
	url := flag.String("url", "https://ip-ranges.amazonaws.com/ip-ranges.json", "URL from which the AWS IP ranges JSON can be read")
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

	var ranges awsIPRanges
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

	if err := w.Write([]string{"AWS", "REGION", "IP-BLOCK"}); err != nil {
		log.Fatalf("write header: %v", err)
	}
	for _, p := range ranges.Prefixes {
		if err := w.Write([]string{"AWS", p.Region, p.IPPrefix}); err != nil {
			log.Fatalf("write IPv4 row: %v", err)
		}
	}
	for _, p := range ranges.IPv6Prefixes {
		if err := w.Write([]string{"AWS", p.Region, p.IPv6Prefix}); err != nil {
			log.Fatalf("write IPv6 row: %v", err)
		}
	}

	if err := w.Error(); err != nil {
		log.Fatalf("flush CSV: %v", err)
	}
}
