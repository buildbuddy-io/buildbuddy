package clientip

import (
	"context"
	"net/netip"
	"slices"
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
	xForwardedForTrustedHops  = flag.Int("auth.xforwardedfor_trusted_hops", 0, "If greater than 0, the client IP is taken from the Nth entry from the right of the X-Forwarded-For header, where N is the number of trusted proxies that append to it. If the header has fewer than N entries, the peer address is used. If 0, the second-to-last entry is used (or the only entry, if there is just one).")
	trustedXForwardedForPeers = flag.Slice("auth.trusted_xforwardedfor_peers", []string{}, "CIDRs of peers from which the X-Forwarded-For header is trusted. The CIDRs in this flag are ORed with auth.trust_xforwardedfor_header. If that flag is true, all peers are trusted, otherwise, only peers matching CIDRs in this slice are trusted.")
)

func Get(ctx context.Context) string {
	if v, ok := ctx.Value(ContextKey).(string); ok {
		return v
	}
	return ""
}

var trustedPeerPrefixes atomic.Pointer[[]netip.Prefix]

// Init validates the X-Forwarded-For flags. It is safe to call multiple times;
// each call re-reads the flags.
func Init() error {
	if *xForwardedForTrustedHops < 0 {
		return status.InvalidArgumentErrorf("auth.xforwardedfor_trusted_hops must not be negative, got %d", *xForwardedForTrustedHops)
	}
	if *xForwardedForTrustedHops > 0 && !*trustXForwardedForHeader && len(*trustedXForwardedForPeers) == 0 {
		return status.InvalidArgumentError("auth.xforwardedfor_trusted_hops requires auth.trust_xforwardedfor_header or auth.trusted_xforwardedfor_peers to be set")
	}
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

// SetFromXForwardedForHeader sets the client IP from the values of the
// X-Forwarded-For header, one per header line in the order received.
func SetFromXForwardedForHeader(ctx context.Context, values []string, peerIP string) (context.Context, bool) {
	if len(values) == 0 || (!peerIsTrusted(peerIP)) {
		return ctx, false
	}

	n := *xForwardedForTrustedHops
	if n == 0 {
		// For GCLB, the header format is [client supplied IP,]client IP, LB IP
		// We always look at the client IP as seen by GCLB as the client supplied
		// value can't be trusted if it's present.
		if ip, ok := nthEntryFromRight(values, 2); ok {
			return context.WithValue(ctx, ContextKey, ip), true
		}
		// If there's only a single IP in the header, return it directly.
		// This handles the header format set by NGINX.
		n = 1
	}
	ip, ok := nthEntryFromRight(values, n)
	if !ok {
		return ctx, false
	}
	return context.WithValue(ctx, ContextKey, ip), true
}

// nthEntryFromRight returns the nth comma-separated entry counting from the
// right (1 is the last entry), treating the header values as if they were
// joined with commas in order (RFC 9110 5.3). It does not allocate.
func nthEntryFromRight(values []string, n int) (string, bool) {
	for _, v := range slices.Backward(values) {
		for {
			j := strings.LastIndexByte(v, ',')
			n--
			if n == 0 {
				ip := strings.TrimSpace(v[j+1:])
				return ip, ip != ""
			}
			if j < 0 {
				break
			}
			v = v[:j]
		}
	}
	return "", false
}
