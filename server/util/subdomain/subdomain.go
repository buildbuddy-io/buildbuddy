package subdomain

import (
	"context"
	"net/url"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/urlutil"
)

const subdomainKey = "subdomain"

var (
	enableSubdomainMatching = flag.Bool("app.enable_subdomain_matching", false, "If true, request subdomain will be taken into account when determining what request restrictions should be applied.")
	defaultSubdomains       = flag.Slice("app.default_subdomains", []string{}, "List of subdomains that should not be handled as user-owned subdomains.")
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

	hostDomain := urlutil.GetDomain(host)
	if build_buddy_url.Domain() != hostDomain {
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

func DefaultSubdomains() []string {
	return *defaultSubdomains
}

// ReplaceURLSubdomain substitutes the subdomain on the given URL with
// the group subdomain of the authenticated user. If subdomains are not enabled
// or the group does not have a URL identifier, then the original URL is
// returned.
func ReplaceURLSubdomain(ctx context.Context, env environment.Env, rawURL string) (string, error) {
	if !Enabled() {
		return rawURL, nil
	}

	u, err := perms.AuthenticatedUser(ctx, env)
	if err != nil {
		return "", err
	}

	g, err := env.GetUserDB().GetGroupByID(ctx, u.GetGroupID())
	if err != nil {
		return "", err
	}
	return ReplaceURLSubdomainForGroup(rawURL, g), nil
}

// ReplaceURLSubdomainForGroup substitutes the subdomain on the given URL with
// the group subdomain. If subdomains are not enabled or the group does not
// have a URL identifier, then the original URL is returned.
func ReplaceURLSubdomainForGroup(rawURL string, g *tables.Group) string {
	if !Enabled() {
		return rawURL
	}

	if g.URLIdentifier == nil || *g.URLIdentifier == "" {
		return rawURL
	}

	pURL, err := url.Parse(rawURL)
	// Input URLs should already be validated so this is not supposed to happen.
	if err != nil {
		alert.UnexpectedEvent("invalid_config_url", rawURL)
		return rawURL
	}

	domain := urlutil.GetDomain(pURL.Hostname())
	newHost := *g.URLIdentifier + "." + domain
	if pURL.Port() != "" {
		newHost += ":" + pURL.Port()
	}
	pURL.Host = newHost

	return pURL.String()
}
