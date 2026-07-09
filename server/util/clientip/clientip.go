package clientip

import (
	"context"
	"net/netip"
	"strings"
	"sync/atomic"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const ContextKey = "clientIP"

// HeaderName is the gRPC metadata key a trusted forwarding proxy uses to assert
// the original caller's client IP. Its "x-buildbuddy-internal-" prefix marks it
// as an internal-only trust header: the gRPC server strips any such header from
// untrusted callers (see the interceptors package), and the backend only honors
// this one when it carries a verified ClientIdentityGRPCProxy client identity.
const HeaderName = "x-buildbuddy-internal-client-ip"

var (
	trustXForwardedForHeader  = flag.Bool("auth.trust_xforwardedfor_header", false, "If true, client IP information will be retrieved from the X-Forwarded-For header. Should only be enabled if the BuildBuddy server is only accessible behind a trusted proxy.")
	trustedXForwardedForPeers = flag.Slice("auth.trusted_xforwardedfor_peers", []string{}, "CIDRs of peers from which the X-Forwarded-For header is trusted. The CIDRs in this flag are ORed with auth.trust_xforwardedfor_header. If that flag is true, all peers are trusted, otherwise, only peers matching CIDRs in this slice are trusted.")
)

func Get(ctx context.Context) string {
	if v, ok := ctx.Value(ContextKey).(string); ok {
		return v
	}
	return ""
}

var trustedPeerPrefixes atomic.Pointer[[]netip.Prefix]

// Init validates the auth.trusted_xforwardedfor_peers flag. It is safe to
// call multiple times; each call re-reads the flag.
func Init() error {
	prefixes := make([]netip.Prefix, 0, len(*trustedXForwardedForPeers))
	for _, cidr := range *trustedXForwardedForPeers {
		prefix, err := netip.ParsePrefix(cidr)
		if err != nil {
			return status.InvalidArgumentErrorf("invalid auth.trusted_xforwardedfor_peers CIDR %q: %s", cidr, err)
		}
		prefixes = append(prefixes, prefix)
	}
	trustedPeerPrefixes.Store(&prefixes)
	return nil
}

func peerIsTrusted(peerIP string) bool {
	if *trustXForwardedForHeader {
		return true
	}

	prefixes := trustedPeerPrefixes.Load()
	if prefixes == nil {
		log.Warningf("--auth.trusted_xforwardedfor_peers is unparsed, did you remember to call clientip.Init()?")
		return false
	}
	addr, err := netip.ParseAddr(peerIP)
	if err != nil {
		return false
	}
	addr = addr.Unmap()
	for _, prefix := range *prefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

func SetFromXForwardedForHeader(ctx context.Context, header, peerIP string) (context.Context, bool) {
	if header == "" || (!peerIsTrusted(peerIP)) {
		return ctx, false
	}

	ips := strings.Split(header, ",")

	// If there's only a single IP in the header, return it directly.
	// This handles the header format set by NGINX.
	if len(ips) == 1 {
		return context.WithValue(ctx, ContextKey, strings.TrimSpace(ips[0])), true
	}

	// For GCLB, the header format is [client supplied IP,]client IP, LB IP
	// We always look at the client IP as seen by GCLB as the client supplied
	// value can't be trusted if it's present.
	return context.WithValue(ctx, ContextKey, strings.TrimSpace(ips[len(ips)-2])), true
}
