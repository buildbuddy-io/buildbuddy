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
//
// Data sources:
//   - AWS:        https://ip-ranges.amazonaws.com/ip-ranges.json
//   - Azure:      Microsoft Service Tags JSON (URL auto-discovered weekly)
//   - GCP:        https://www.gstatic.com/ipranges/cloud.json
//   - GitHub:     https://api.github.com/meta
//   - MacStadium: RIPE Stat announced-prefixes API for AS395336, AS395337, AS30377
package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
)

func main() {
	dir := csvDir()
	providers := []struct {
		name string
		fn   func(*csv.Writer) error
	}{
		{"aws", writeAWS},
		{"azure", writeAzure},
		{"gcp", writeGCP},
		{"github", writeGitHub},
		{"macstadium", writeMacStadium},
	}
	for _, p := range providers {
		path := filepath.Join(dir, "data", p.name+".csv")
		if err := writeCSV(path, p.fn); err != nil {
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

func isIPv6(cidr string) bool {
	return strings.Contains(cidr, ":")
}

func writeCSV(path string, fn func(*csv.Writer) error) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if err := fn(w); err != nil {
		return err
	}
	w.Flush()
	return w.Error()
}

func fetchJSON(url string, v any) error {
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("fetch %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch %s: status %s", url, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

// --- AWS ---

func writeAWS(w *csv.Writer) error {
	var ranges struct {
		Prefixes []struct {
			IPPrefix string `json:"ip_prefix"`
			Region   string `json:"region"`
		} `json:"prefixes"`
	}
	if err := fetchJSON("https://ip-ranges.amazonaws.com/ip-ranges.json", &ranges); err != nil {
		return err
	}
	for _, p := range ranges.Prefixes {
		if err := w.Write([]string{"AWS", p.Region, p.IPPrefix}); err != nil {
			return err
		}
	}
	return nil
}

// --- Azure ---

var azureDownloadLinkRe = regexp.MustCompile(`https://download\.microsoft\.com/download/[^"]+ServiceTags_Public_[0-9]+\.json`)

func discoverAzureURL() (string, error) {
	resp, err := http.Get("https://www.microsoft.com/en-us/download/confirmation.aspx?id=56519")
	if err != nil {
		return "", fmt.Errorf("fetch download page: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read download page: %w", err)
	}
	match := azureDownloadLinkRe.Find(body)
	if match == nil {
		return "", fmt.Errorf("could not find ServiceTags download URL in page")
	}
	return string(match), nil
}

func writeAzure(w *csv.Writer) error {
	jsonURL, err := discoverAzureURL()
	if err != nil {
		return err
	}
	log.Printf("azure: discovered URL %s", jsonURL)

	var tags struct {
		Values []struct {
			Name       string `json:"name"`
			Properties struct {
				Region          string   `json:"region"`
				AddressPrefixes []string `json:"addressPrefixes"`
			} `json:"properties"`
		} `json:"values"`
	}
	if err := fetchJSON(jsonURL, &tags); err != nil {
		return err
	}
	for _, v := range tags.Values {
		if !strings.HasPrefix(v.Name, "AzureCloud.") || v.Properties.Region == "" {
			continue
		}
		for _, prefix := range v.Properties.AddressPrefixes {
			if isIPv6(prefix) {
				continue
			}
			if err := w.Write([]string{"Azure", v.Properties.Region, prefix}); err != nil {
				return err
			}
		}
	}
	return nil
}

// --- GCP ---

func writeGCP(w *csv.Writer) error {
	var ranges struct {
		Prefixes []struct {
			IPv4Prefix string `json:"ipv4Prefix"`
			Scope      string `json:"scope"`
		} `json:"prefixes"`
	}
	if err := fetchJSON("https://www.gstatic.com/ipranges/cloud.json", &ranges); err != nil {
		return err
	}
	for _, p := range ranges.Prefixes {
		if p.IPv4Prefix != "" {
			if err := w.Write([]string{"GCP", p.Scope, p.IPv4Prefix}); err != nil {
				return err
			}
		}
	}
	return nil
}

// --- GitHub ---

func writeGitHub(w *csv.Writer) error {
	var meta map[string]json.RawMessage
	if err := fetchJSON("https://api.github.com/meta", &meta); err != nil {
		return err
	}
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
			if isIPv6(cidr) || seen[cidr] {
				continue
			}
			seen[cidr] = true
			if err := w.Write([]string{"GitHub", "", cidr}); err != nil {
				return err
			}
		}
	}
	return nil
}

// --- MacStadium ---
//
// MacStadium does not publish a machine-readable IP range list, so we use
// the RIPE Stat API to look up BGP announcements for their three ASNs:
//   - AS395336 (Atlanta, GA)
//   - AS395337 (Las Vegas, NV)
//   - AS30377  (Dublin, IE)

var macStadiumASNs = []struct {
	asn    string
	region string
}{
	{"AS395336", "atlanta"},
	{"AS395337", "las-vegas"},
	{"AS30377", "dublin"},
}

func writeMacStadium(w *csv.Writer) error {
	for _, a := range macStadiumASNs {
		var result struct {
			Data struct {
				Prefixes []struct {
					Prefix string `json:"prefix"`
				} `json:"prefixes"`
			} `json:"data"`
		}
		url := fmt.Sprintf("https://stat.ripe.net/data/announced-prefixes/data.json?resource=%s", a.asn)
		if err := fetchJSON(url, &result); err != nil {
			return fmt.Errorf("%s (%s): %w", a.asn, a.region, err)
		}
		for _, p := range result.Data.Prefixes {
			if isIPv6(p.Prefix) {
				continue
			}
			if err := w.Write([]string{"MacStadium", a.region, p.Prefix}); err != nil {
				return err
			}
		}
	}
	return nil
}
