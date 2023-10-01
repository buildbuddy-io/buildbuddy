package region

import (
	"net/http"
	"regexp"
	"strings"

	cfgpb "github.com/buildbuddy-io/buildbuddy/proto/config"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
)

var (
	regions        = flag.Slice("regions", []Region{}, "A list of regions that executors might be connected to.")
	subdomainRegex = regexp.MustCompile("^[a-zA-Z0-9-]+$")
)

type Region struct {
	Name       string `yaml:"name" json:"name" usage:"The user-friendly name of this region. Ex: Europe"`
	Server     string `yaml:"server" json:"server" usage:"The http endpoint for this server, with the protocol. Ex: https://app.europe.buildbuddy.io"`
	Subdomains string `yaml:"subdomains" json:"subdomains" usage:"The format for subdomain urls of with a single * wildcard. Ex: https://*.europe.buildbuddy.io"`
}

func Protos() []*cfgpb.Region {
	protos := []*cfgpb.Region{}
	for _, r := range *regions {
		protos = append(protos, &cfgpb.Region{
			Name:   r.Name,
			Server: r.Server,
		})
	}
	return protos
}

func isRegionalServer(regions []Region, server string) bool {
	for _, region := range regions {
		if region.Server == server {
			return true
		}
		chunks := strings.Split(region.Subdomains, "*")
		// Only one wildcard is allowed for the subdomain.
		if len(chunks) != 2 {
			continue
		}
		// Trim the http:// prefix bit and the top level domain suffix.
		if !strings.HasPrefix(server, chunks[0]) || !strings.HasSuffix(server, chunks[1]) {
			continue
		}
		subdomain := strings.TrimSuffix(strings.TrimPrefix(server, chunks[0]), chunks[1])
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
		// We only need CORS for POST and OPTIONS requests.
		if r.Method != "POST" && r.Method != "OPTIONS" {
			next.ServeHTTP(w, r)
			return
		}

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
			w.Header().Set("Access-Control-Allow-Methods", r.Header.Get("Access-Control-Request-Method"))
			w.Header().Set("Access-Control-Allow-Headers", r.Header.Get("Access-Control-Request-Headers"))
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}
