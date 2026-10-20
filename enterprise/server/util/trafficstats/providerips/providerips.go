// Package providerips fetches IP ranges published by cloud providers and
// returns them as a flat list of (Provider, Region, CIDR) tuples.
//
// Data sources:
//   - AWS:        https://ip-ranges.amazonaws.com/ip-ranges.json
//   - Azure:      Microsoft Service Tags JSON (URL auto-discovered weekly)
//   - GCP:        https://www.gstatic.com/ipranges/cloud.json
//   - GitHub:     https://api.github.com/meta
//   - DigitalOcean: https://www.digitalocean.com/geo/google.csv (official geofeed)
//   - Oracle:     https://docs.oracle.com/en-us/iaas/tools/public_ip_ranges.json
//   - MacStadium, PhoenixNAP, Hetzner, OVH, Latitude.sh, Namespace Labs: these
//     providers do not publish a range list, so we take the prefixes their ASNs
//     announce in BGP from the RIPE Stat announced-prefixes API.
package providerips

import (
	"encoding/csv"
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

// asnSite is an autonomous system operated by a provider, and the site it
// serves when the ASN maps to a single location. Region is left empty for
// ASNs that span several sites, since we have no per-prefix location data.
type asnSite struct {
	asn    string
	region string
}

// fetchASNRanges returns the IPv4 prefixes announced in BGP by each of the
// given ASNs, via the RIPE Stat announced-prefixes API. This is the fallback
// for providers that do not publish an official range list.
func fetchASNRanges(provider string, asns []asnSite) ([]IPRange, error) {
	var out []IPRange
	seen := map[string]bool{}
	for _, a := range asns {
		var result struct {
			Data struct {
				Prefixes []struct {
					Prefix string `json:"prefix"`
				} `json:"prefixes"`
			} `json:"data"`
		}
		url := fmt.Sprintf("https://stat.ripe.net/data/announced-prefixes/data.json?resource=%s&sourceapp=buildbuddy-trafficstats", a.asn)
		if err := fetchJSON(url, &result); err != nil {
			return nil, fmt.Errorf("%s %s: %w", provider, a.asn, err)
		}
		for _, p := range result.Data.Prefixes {
			if isIPv6(p.Prefix) || seen[p.Prefix] {
				continue
			}
			seen[p.Prefix] = true
			out = append(out, IPRange{Provider: provider, Region: a.region, CIDR: p.Prefix})
		}
	}
	return out, nil
}

// MacStadium ASNs and their locations.
var macStadiumASNs = []asnSite{
	{"AS395336", "atlanta"},
	{"AS395337", "las-vegas"},
	{"AS30377", "dublin"},
}

// FetchMacStadium returns IPv4 ranges announced by MacStadium's ASNs.
func FetchMacStadium() ([]IPRange, error) {
	return fetchASNRanges("MacStadium", macStadiumASNs)
}

// PhoenixNAP ASNs. Secured Servers LLC is PhoenixNAP's bare-metal business;
// the Ashburn and Atlanta ASNs are named for their site, the others announce
// space for several sites (e.g. AS60558 "SECUREDSERVERS-EU" announces /24s
// from the Phoenix 192.240.192.0/18 block as well as Amsterdam and Singapore).
var phoenixNAPASNs = []asnSite{
	{"AS20454", ""},        // SSASN2 - Secured Servers LLC
	{"AS60558", ""},        // SECUREDSERVERS-EU - PhoenixNAP
	{"AS12189", ""},        // PhoenixNAP LLC
	{"AS19437", "ashburn"}, // SS-ASH - Secured Servers LLC
	{"AS11572", "atlanta"}, // SS-ATL - PhoenixNAP LLC
}

// FetchPhoenixNAP returns IPv4 ranges announced by PhoenixNAP's ASNs.
func FetchPhoenixNAP() ([]IPRange, error) {
	return fetchASNRanges("PhoenixNAP", phoenixNAPASNs)
}

// Hetzner ASNs. AS24940 serves both the German and Finnish datacenters; the
// "Cloud" ASNs serve the newer US and Singapore sites.
var hetznerASNs = []asnSite{
	{"AS24940", ""},  // HETZNER-AS
	{"AS213230", ""}, // HETZNER-CLOUD2-AS
	{"AS212317", ""}, // HETZNER-CLOUD3-AS
	{"AS215859", ""}, // HETZNER-CLOUD4-AS
}

// FetchHetzner returns IPv4 ranges announced by Hetzner's ASNs.
func FetchHetzner() ([]IPRange, error) {
	return fetchASNRanges("Hetzner", hetznerASNs)
}

// OVH ASNs. AS35540 (OVH Telecom) is their consumer ISP and is excluded.
var ovhASNs = []asnSite{
	{"AS16276", ""},
}

// FetchOVH returns IPv4 ranges announced by OVHcloud's ASN.
func FetchOVH() ([]IPRange, error) {
	return fetchASNRanges("OVH", ovhASNs)
}

// Latitude.sh (formerly Maxihost) ASNs.
var latitudeASNs = []asnSite{
	{"AS396356", ""},
	{"AS262287", ""},
}

// FetchLatitude returns IPv4 ranges announced by Latitude.sh's ASNs.
func FetchLatitude() ([]IPRange, error) {
	return fetchASNRanges("Latitude.sh", latitudeASNs)
}

// Namespace Labs (namespace.so, a CI runner provider) ASN.
var namespaceASNs = []asnSite{
	{"AS401483", ""},
}

// FetchNamespace returns IPv4 ranges announced by Namespace Labs' ASN.
func FetchNamespace() ([]IPRange, error) {
	return fetchASNRanges("Namespace", namespaceASNs)
}

// FetchDigitalOcean returns IPv4 ranges from DigitalOcean's published geofeed
// (RFC 8805 CSV: prefix, country, subdivision, city, postal code). The region
// is the city, matching the MacStadium convention.
func FetchDigitalOcean() ([]IPRange, error) {
	url := "https://www.digitalocean.com/geo/google.csv"
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch %s: status %s", url, resp.Status)
	}
	r := csv.NewReader(resp.Body)
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", url, err)
	}
	var out []IPRange
	for _, rec := range records {
		if len(rec) < 4 || isIPv6(rec[0]) {
			continue
		}
		region := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(rec[3]), " ", "-"))
		out = append(out, IPRange{Provider: "DigitalOcean", Region: region, CIDR: strings.TrimSpace(rec[0])})
	}
	return out, nil
}

// FetchOracle returns IPv4 ranges from Oracle Cloud's published range list.
func FetchOracle() ([]IPRange, error) {
	var data struct {
		Regions []struct {
			Region string `json:"region"`
			CIDRs  []struct {
				CIDR string `json:"cidr"`
			} `json:"cidrs"`
		} `json:"regions"`
	}
	if err := fetchJSON("https://docs.oracle.com/en-us/iaas/tools/public_ip_ranges.json", &data); err != nil {
		return nil, err
	}
	var out []IPRange
	for _, r := range data.Regions {
		for _, c := range r.CIDRs {
			if isIPv6(c.CIDR) {
				continue
			}
			out = append(out, IPRange{Provider: "Oracle", Region: r.Region, CIDR: c.CIDR})
		}
	}
	return out, nil
}
