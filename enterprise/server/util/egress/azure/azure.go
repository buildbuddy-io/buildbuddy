package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
)

type azureServiceTags struct {
	Values []struct {
		Name       string `json:"name"`
		Properties struct {
			Region          string   `json:"region"`
			SystemService   string   `json:"systemService"`
			AddressPrefixes []string `json:"addressPrefixes"`
		} `json:"properties"`
	} `json:"values"`
}

// This simple Go program downloads the set of Azure IP ranges from the Azure
// Service Tags JSON file and converts them into CSV format:
// Azure,<REGION>,<IP-BLOCK>
//
// The download URL changes weekly. By default, the program discovers the
// current URL from the Microsoft download page. You may also provide a direct
// URL via --url.
//
// These ranges are checked in, in azure.csv. Refresh them by running:
// bazel run //enterprise/server/util/egress/azure -- --output=$PWD/enterprise/server/util/egress/azure/azure.csv
func main() {
	url := flag.String("url", "", "Direct URL to the Azure Service Tags JSON file. If empty, auto-discovered from the Microsoft download page.")
	outputPath := flag.String("output", "", "Path to output CSV")
	flag.Parse()

	if *outputPath == "" {
		log.Fatal("--output is required")
	}

	jsonURL := *url
	if jsonURL == "" {
		var err error
		jsonURL, err = discoverDownloadURL()
		if err != nil {
			log.Fatalf("discover download URL: %v", err)
		}
		log.Printf("discovered Azure Service Tags URL: %s", jsonURL)
	}

	resp, err := http.Get(jsonURL)
	if err != nil {
		log.Fatalf("fetch input: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("fetch input: unexpected status %s", resp.Status)
	}

	var tags azureServiceTags
	if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil {
		log.Fatalf("decode JSON: %v", err)
	}

	output, err := os.Create(*outputPath)
	if err != nil {
		log.Fatalf("create output: %v", err)
	}
	defer output.Close()

	w := csv.NewWriter(output)
	defer w.Flush()

	for _, v := range tags.Values {
		// Only include regional AzureCloud entries, which represent all
		// Azure datacenter IPs broken down by geographic region.
		if !strings.HasPrefix(v.Name, "AzureCloud.") || v.Properties.Region == "" {
			continue
		}
		for _, prefix := range v.Properties.AddressPrefixes {
			if err := w.Write([]string{"Azure", v.Properties.Region, prefix}); err != nil {
				log.Fatalf("write row: %v", err)
			}
		}
	}

	if err := w.Error(); err != nil {
		log.Fatalf("flush CSV: %v", err)
	}
}

var downloadLinkRe = regexp.MustCompile(`https://download\.microsoft\.com/download/[^"]+ServiceTags_Public_[0-9]+\.json`)

func discoverDownloadURL() (string, error) {
	resp, err := http.Get("https://www.microsoft.com/en-us/download/confirmation.aspx?id=56519")
	if err != nil {
		return "", fmt.Errorf("fetch download page: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read download page: %w", err)
	}

	match := downloadLinkRe.Find(body)
	if match == nil {
		return "", fmt.Errorf("could not find ServiceTags download URL in page")
	}
	return string(match), nil
}
