// Package providerips fetches IP ranges published by cloud providers and
// returns them as a flat list of (Provider, Region, CIDR) tuples.
//
// Data sources:
//   - AWS:        https://ip-ranges.amazonaws.com/ip-ranges.json
//   - Azure:      Microsoft Service Tags JSON (URL auto-discovered weekly)
//   - GCP:        https://www.gstatic.com/ipranges/cloud.json
//   - GitHub:     https://api.github.com/meta
//   - MacStadium: RIPE Stat announced-prefixes API for AS395336, AS395337, AS30377
package providerips

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
)

// IPRange represents a single IP prefix belonging to a cloud provider.
type IPRange struct {
	Provider string
	Region   string
	CIDR     string
}

func isIPv6(cidr string) bool {
	return strings.Contains(cidr, ":")
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

// FetchAWS returns IPv4 ranges from the AWS ip-ranges.json endpoint.
func FetchAWS() ([]IPRange, error) {
	var data struct {
		Prefixes []struct {
			IPPrefix string `json:"ip_prefix"`
			Region   string `json:"region"`
		} `json:"prefixes"`
	}
	if err := fetchJSON("https://ip-ranges.amazonaws.com/ip-ranges.json", &data); err != nil {
		return nil, err
	}
	var out []IPRange
	for _, p := range data.Prefixes {
		out = append(out, IPRange{Provider: "AWS", Region: p.Region, CIDR: p.IPPrefix})
	}
	return out, nil
}

// FetchGCP returns IPv4 ranges from the GCP cloud.json endpoint.
func FetchGCP() ([]IPRange, error) {
	var data struct {
		Prefixes []struct {
			IPv4Prefix string `json:"ipv4Prefix"`
			Scope      string `json:"scope"`
		} `json:"prefixes"`
	}
	if err := fetchJSON("https://www.gstatic.com/ipranges/cloud.json", &data); err != nil {
		return nil, err
	}
	var out []IPRange
	for _, p := range data.Prefixes {
		if p.IPv4Prefix != "" {
			out = append(out, IPRange{Provider: "GCP", Region: p.Scope, CIDR: p.IPv4Prefix})
		}
	}
	return out, nil
}

var azureDownloadLinkRe = regexp.MustCompile(`https://download\.microsoft\.com/download/[^"]+ServiceTags_Public_[0-9]+\.json`)

// FetchAzure returns IPv4 ranges from the Azure Service Tags JSON.
// The download URL is auto-discovered from the Microsoft download page.
func FetchAzure() ([]IPRange, error) {
	jsonURL, err := discoverAzureURL()
	if err != nil {
		return nil, err
	}

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
		return nil, err
	}
	var out []IPRange
	for _, v := range tags.Values {
		if !strings.HasPrefix(v.Name, "AzureCloud.") || v.Properties.Region == "" {
			continue
		}
		for _, prefix := range v.Properties.AddressPrefixes {
			if isIPv6(prefix) {
				continue
			}
			out = append(out, IPRange{Provider: "Azure", Region: v.Properties.Region, CIDR: prefix})
		}
	}
	return out, nil
}

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

// FetchGitHub returns deduplicated IPv4 ranges from the GitHub /meta API.
func FetchGitHub() ([]IPRange, error) {
	var meta map[string]json.RawMessage
	if err := fetchJSON("https://api.github.com/meta", &meta); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []IPRange
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
			out = append(out, IPRange{Provider: "GitHub", Region: "", CIDR: cidr})
		}
	}
	return out, nil
}

// MacStadium ASNs and their locations.
var macStadiumASNs = []struct {
	asn    string
	region string
}{
	{"AS395336", "atlanta"},
	{"AS395337", "las-vegas"},
	{"AS30377", "dublin"},
}

// FetchMacStadium returns IPv4 ranges for MacStadium ASNs via the RIPE Stat API.
func FetchMacStadium() ([]IPRange, error) {
	var out []IPRange
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
			return nil, fmt.Errorf("%s (%s): %w", a.asn, a.region, err)
		}
		for _, p := range result.Data.Prefixes {
			if isIPv6(p.Prefix) {
				continue
			}
			out = append(out, IPRange{Provider: "MacStadium", Region: a.region, CIDR: p.Prefix})
		}
	}
	return out, nil
}
