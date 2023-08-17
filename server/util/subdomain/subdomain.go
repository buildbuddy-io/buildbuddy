package subdomain

import (
	"context"
	"flag"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
)

const subdomainKey = "subdomain"

var (
	enableSubdomainMatching = flag.Bool("app.enable_subdomain_matching", false, "If true, request subdomain will be taken into account when determining what request restrictions should be applied.")
	defaultSubdomains       = flagutil.New("app.default_subdomains", []string{}, "List of subdomains that should not be handled as user-owned subdomains.")
)

// SetHost configures the subdomain in the context based on the specified
// request host information.
func SetHost(ctx context.Context, host string) context.Context {
	if !*enableSubdomainMatching {
		return ctx
	}
	parts := strings.Split(host, ".")
	if len(parts) < 3 {
		return ctx
	}
	subdomain := parts[0]
	for _, ds := range *defaultSubdomains {
		if ds == subdomain {
			return ctx
		}
	}
	return context.WithValue(ctx, subdomainKey, subdomain)
}

// Get returns the subdomain restriction that should be applied or an empty
// string if no subdomain restrictions should be applied.
func Get(ctx context.Context) string {
	v, _ := ctx.Value(subdomainKey).(string)
	return v
}

func Enabled() bool {
	return *enableSubdomainMatching
}
