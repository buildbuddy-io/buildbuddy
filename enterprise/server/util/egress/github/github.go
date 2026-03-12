package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
)

// This simple Go program downloads the set of GitHub IP ranges from
// https://api.github.com/meta and then converts them into CSV format:
// GitHub,,<IP-BLOCK>
//
// These ranges are checked in, in github.csv. Refresh them by running:
// bazel run //enterprise/server/util/egress/github -- --output=$PWD/enterprise/server/util/egress/github/github.csv
func main() {
	url := flag.String("url", "https://api.github.com/meta", "URL from which the GitHub IP ranges can be read")
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

	var meta map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		log.Fatalf("decode JSON: %v", err)
	}

	output, err := os.Create(*outputPath)
	if err != nil {
		log.Fatalf("create output: %v", err)
	}
	defer output.Close()

	w := csv.NewWriter(output)
	defer w.Flush()

	// Collect all CIDR ranges from known IP range fields, deduplicating
	// across fields since some ranges appear in multiple categories.
	seen := map[string]bool{}
	for _, key := range []string{"actions", "api", "git", "github_enterprise_importer", "hooks", "importer", "packages", "pages", "web"} {
		raw, ok := meta[key]
		if !ok {
			continue
		}
		var cidrs []string
		if err := json.Unmarshal(raw, &cidrs); err != nil {
			continue
		}
		for _, cidr := range cidrs {
			if seen[cidr] {
				continue
			}
			seen[cidr] = true
			if err := w.Write([]string{"GitHub", "", cidr}); err != nil {
				log.Fatalf("write row: %v", err)
			}
		}
	}

	if err := w.Error(); err != nil {
		log.Fatalf("flush CSV: %v", err)
	}
}
