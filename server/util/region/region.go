package region

import (
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"

	cfgpb "github.com/buildbuddy-io/buildbuddy/proto/config"
)

var (
	region         = flag.String("app.region", "", "The region in which the app is running. This value is only used/configured by apps.")
	regions        = flag.Slice("regions", []Region{}, "A list of regions that executors might be connected to.")
	subdomainRegex = regexp.MustCompile("^[a-zA-Z0-9-]+$")
)

// ConfiguredAppRegion returns the explicitly configured region name for the
// current app, e.g. "us-west1".
//
// Before using this function, make sure the relevant server has the
// "app.region" flag configured.
func ConfiguredAppRegion() string {
	return *region
}

type Region struct {
	Name       string `yaml:"name" json:"name" usage:"The user-friendly name of this region. Ex: Europe"`
	Server     string `yaml:"server" json:"server" usage:"The http endpoint for this server, with the protocol. Ex: https://app.europe.buildbuddy.io"`
	Subdomains string `yaml:"subdomains" json:"subdomains" usage:"The format for subdomain urls of with a single * wildcard. Ex: https://*.europe.buildbuddy.io"`
}

func Protos() []*cfgpb.Region {
	protos := []*cfgpb.Region{}
	for _, r := range *regions {
		protos = append(protos, &cfgpb.Region{
			Name:       r.Name,
			Server:     r.Server,
			Subdomains: r.Subdomains,
		})
	}
	return protos
}

// canonicalOrigin validates an HTTP origin and removes its default port.
// Configured server URLs may include a trailing slash; browser Origins may not.
func canonicalOrigin(server string, allowTrailingSlash bool) string {
	if strings.ContainsAny(server, "?#") {
		return ""
	}
	u, err := url.Parse(server)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Opaque != "" {
		return ""
	}
	if u.Path != "" && !(allowTrailingSlash && u.Path == "/") {
		return ""
	}
	host := strings.ToLower(u.Host)
	if strings.HasSuffix(host, ":") {
		return ""
	}
	if (u.Scheme == "https" && u.Port() == "443") || (u.Scheme == "http" && u.Port() == "80") {
		host = strings.TrimSuffix(host, ":"+u.Port())
	}
	return u.Scheme + "://" + host
}

func isRegionalServer(regions []Region, server string) bool {
	origin := canonicalOrigin(server, false)
	if origin == "" {
		return false
	}
	for _, region := range regions {
		if configuredOrigin := canonicalOrigin(region.Server, true); configuredOrigin != "" && configuredOrigin == origin {
			return true
		}
		// Match canonical origins so configured wildcard default ports agree
		// with the Origin header emitted by browsers. Invalid patterns stay
		// invalid rather than being reduced to their host.
		chunks := strings.Split(canonicalOrigin(region.Subdomains, true), "*")
		// Only one wildcard is allowed for the subdomain.
		if len(chunks) != 2 {
			continue
		}
		// Trim the http:// prefix bit and the top level domain suffix.
		if !strings.HasPrefix(origin, chunks[0]) || !strings.HasSuffix(origin, chunks[1]) {
			continue
		}
		subdomain := strings.TrimSuffix(strings.TrimPrefix(origin, chunks[0]), chunks[1])
		// Make sure the subdomain doesn't have any non alphanumeric or dash characters.
		if subdomainRegex.MatchString(subdomain) {
			return true
		}
	}
	return false
}

// Greatly simplified version of:
// https://github.com/gorilla/handlers/blob/main/cors.go
func CORS(next http.Handler) http.Handler {
	// If no regions are configured, we don't have to do anything.
	if len(*regions) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Support RPCs and authenticated file downloads.
		if r.Method != "POST" && r.Method != "GET" && r.Method != "HEAD" && r.Method != "OPTIONS" {
			next.ServeHTTP(w, r)
			return
		}

		// Responses vary by Origin, including when the origin is not allowed.
		w.Header().Add("Vary", "Origin")

		// If we're not dealing with a regional server, we don't have to set any CORS headers.
		if r.Header.Get("Origin") == "" || !isRegionalServer(*regions, r.Header.Get("Origin")) {
			next.ServeHTTP(w, r)
			return
		}

		// If we're dealing with a regional server, set CORS headers.
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Origin", r.Header.Get("Origin"))

		// If it's an OPTIONS request, we can exit early.
		if r.Method == "OPTIONS" {
			w.Header().Add("Vary", "Access-Control-Request-Method")
			w.Header().Add("Vary", "Access-Control-Request-Headers")
			w.Header().Set("Access-Control-Allow-Methods", r.Header.Get("Access-Control-Request-Method"))
			w.Header().Set("Access-Control-Allow-Headers", r.Header.Get("Access-Control-Request-Headers"))
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}
