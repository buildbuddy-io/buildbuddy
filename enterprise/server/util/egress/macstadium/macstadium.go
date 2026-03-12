package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
)

type ripeStatResponse struct {
	Data struct {
		Prefixes []struct {
			Prefix string `json:"prefix"`
		} `json:"prefixes"`
	} `json:"data"`
}

var asns = []struct {
	asn    string
	region string
}{
	{"AS395336", "atlanta"},
	{"AS395337", "las-vegas"},
	{"AS30377", "dublin"},
}

// This simple Go program downloads the set of MacStadium IP ranges from the
// RIPE Stat API (announced prefixes for each MacStadium ASN) and converts
// them into CSV format:
// MacStadium,<REGION>,<IP-BLOCK>
//
// MacStadium does not publish a machine-readable IP range list, so we use
// the RIPE Stat API to look up BGP announcements for their three ASNs:
//   - AS395336 (Atlanta, GA)
//   - AS395337 (Las Vegas, NV)
//   - AS30377  (Dublin, IE)
//
// These ranges are checked in, in macstadium.csv. Refresh them by running:
// bazel run //enterprise/server/util/egress/macstadium -- --output=$PWD/enterprise/server/util/egress/macstadium/macstadium.csv
func main() {
	outputPath := flag.String("output", "", "Path to output CSV")
	flag.Parse()

	if *outputPath == "" {
		log.Fatal("--output is required")
	}

	output, err := os.Create(*outputPath)
	if err != nil {
		log.Fatalf("create output: %v", err)
	}
	defer output.Close()

	w := csv.NewWriter(output)
	defer w.Flush()

	for _, a := range asns {
		prefixes, err := fetchPrefixes(a.asn)
		if err != nil {
			log.Fatalf("fetch %s (%s): %v", a.asn, a.region, err)
		}
		for _, prefix := range prefixes {
			if err := w.Write([]string{"MacStadium", a.region, prefix}); err != nil {
				log.Fatalf("write row: %v", err)
			}
		}
	}

	if err := w.Error(); err != nil {
		log.Fatalf("flush CSV: %v", err)
	}
}

func fetchPrefixes(asn string) ([]string, error) {
	url := fmt.Sprintf("https://stat.ripe.net/data/announced-prefixes/data.json?resource=%s", asn)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %s", resp.Status)
	}

	var result ripeStatResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	var prefixes []string
	for _, p := range result.Data.Prefixes {
		prefixes = append(prefixes, p.Prefix)
	}
	return prefixes, nil
}
