package httpclient

import (
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/publicsuffix"
)

// Tests often need to make HTTP requests to localhost -- set this flag to permit those requests.
var allowLocalhost = flag.Bool("http.client.allow_localhost", false, "Allow HTTP requests to localhost")

// New creates an HTTP client that blocks connections to private IPs and records
// metrics on any requests made.
func New(allowedPrivateIPNets []*net.IPNet, clientName string) *http.Client {
	inner := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: 30 * time.Second,
			Control: blockingDialerControl(allowedPrivateIPNets),
		}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
		Proxy:               http.ProxyFromEnvironment,
	}
	tp := newMetricsTransport(inner, clientName)
	return &http.Client{
		Transport: tp,
	}
}

type dialerControl = func(network, address string, conn syscall.RawConn) error

func blockingDialerControl(allowed []*net.IPNet) dialerControl {
	return func(network, address string, conn syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return err
		}
		ip := net.ParseIP(host)
		if ipAllowed(ip, allowed) {
			return nil
		}
		if (!ip.IsGlobalUnicast() || ip.IsPrivate()) && !(ip.IsLoopback() && *allowLocalhost) {
			log.Infof("Dialer control blocked address %s", address)
			return errors.New("IP address not allowed")
		}
		return nil
	}
}

func ipAllowed(ip net.IP, allowed []*net.IPNet) bool {
	for _, ipNet := range allowed {
		if ipNet.Contains(ip) {
			return true
		}
	}
	return false
}

// FollowRedirectsToAllowedIPs wraps inner so that it follows redirects to IP literals in allowed.
func FollowRedirectsToAllowedIPs(inner http.RoundTripper, allowed []*net.IPNet) http.RoundTripper {
	if len(allowed) == 0 {
		return inner
	}
	return &allowedIPRedirectTransport{client: &http.Client{
		Transport: inner,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			// Deliberate: ggcr refuses a token endpoint downgraded to http, so https
			// registries that redirect to plain-http storage on an allowlisted IP are
			// not supported.
			if via[len(via)-1].URL.Scheme == "https" && req.URL.Scheme != "https" {
				return http.ErrUseLastResponse
			}
			// ggcr allows same-hostname redirects itself and handles their bearer
			// challenges per host, so leave those for it, as on master.
			if strings.EqualFold(req.URL.Hostname(), via[0].URL.Hostname()) {
				return http.ErrUseLastResponse
			}
			if !ipAllowed(net.ParseIP(req.URL.Hostname()), allowed) {
				return http.ErrUseLastResponse
			}
			// Go keeps credentials when only the port changes, so drop them on any origin change.
			if !sameOrigin(req.URL, via[0].URL) {
				for _, h := range []string{"Authorization", "Cookie", "Cookie2"} {
					req.Header.Del(h)
				}
			}
			return nil
		},
	}}
}

// sameOrigin reports whether a and b have the same scheme, host and effective port.
func sameOrigin(a, b *url.URL) bool {
	return strings.EqualFold(a.Scheme, b.Scheme) &&
		strings.EqualFold(a.Hostname(), b.Hostname()) &&
		effectivePort(a) == effectivePort(b)
}

func effectivePort(u *url.URL) string {
	if p := u.Port(); p != "" {
		return p
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
		return "443"
	case "http":
		return "80"
	}
	return ""
}

type allowedIPRedirectTransport struct {
	client *http.Client
}

func (t *allowedIPRedirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.client.Do(req)
	if err != nil {
		if urlErr, ok := err.(*url.Error); ok {
			return nil, urlErr.Err
		}
		return nil, err
	}
	// ggcr's bearer transport only sees the original request, so it would treat a
	// 401 challenge from a redirected-to origin (e.g. blob storage) as coming from
	// the registry and add that origin's scope to the registry token for good.
	// That only happens for requests the bearer transport already authorized, so
	// strip the challenge from those and let ggcr see a plain 401. ggcr's /v2/
	// ping is unauthenticated and needs the challenge to learn the token realm.
	if resp.StatusCode == http.StatusUnauthorized && resp.Request != nil && req.Header.Get("Authorization") != "" && !sameOrigin(resp.Request.URL, req.URL) {
		resp.Header.Del("WWW-Authenticate")
	}
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		if loc, err := resp.Location(); err == nil {
			resp.Header.Set("Location", loc.String())
		}
	}
	return resp, nil
}

// verify that metricsTransport implements the RoundTripper interface
var _ http.RoundTripper = (*metricsTransport)(nil)

type metricsTransport struct {
	inner      http.RoundTripper
	clientName string
}

func newMetricsTransport(inner http.RoundTripper, clientName string) http.RoundTripper {
	return &metricsTransport{
		inner:      inner,
		clientName: clientName,
	}
}

// HostLabel returns a metrics-safe label for the given hostname.
// IP addresses are replaced with "[IP_ADDRESS]", and domain names are
// reduced to their eTLD+1 (e.g. "us.gcr.io" → "gcr.io"). Returns
// "[UNKNOWN]" if the eTLD+1 cannot be determined.
func HostLabel(host string) string {
	if net.ParseIP(host) != nil {
		return "[IP_ADDRESS]"
	}
	etld1, err := publicsuffix.EffectiveTLDPlusOne(host)
	if err != nil {
		return "[UNKNOWN]"
	}
	return etld1
}

func (t *metricsTransport) RoundTrip(in *http.Request) (out *http.Response, err error) {
	hostLabel := HostLabel(in.URL.Hostname())
	metrics.HTTPClientRequestCount.With(prometheus.Labels{
		metrics.HTTPHostLabel:   hostLabel,
		metrics.HTTPMethodLabel: in.Method,
		metrics.ClientNameLabel: t.clientName,
	}).Inc()
	resp, err := t.inner.RoundTrip(in)
	if resp == nil || resp.Body == nil {
		return resp, err
	}
	resp.Body = &instrumentedReadCloser{
		ReadCloser: resp.Body,
		host:       hostLabel,
		method:     in.Method,
		statusCode: resp.StatusCode,
		clientName: t.clientName,
	}
	return resp, err
}

type instrumentedReadCloser struct {
	io.ReadCloser

	host       string
	method     string
	statusCode int
	bytesRead  int
	clientName string
}

func (rc *instrumentedReadCloser) Read(p []byte) (int, error) {
	n, err := rc.ReadCloser.Read(p)
	rc.bytesRead += n
	return n, err
}

func (rc *instrumentedReadCloser) Close() error {
	err := rc.ReadCloser.Close()
	metrics.HTTPClientResponseSizeBytes.With(prometheus.Labels{
		metrics.HTTPHostLabel:         rc.host,
		metrics.HTTPMethodLabel:       rc.method,
		metrics.HTTPResponseCodeLabel: strconv.Itoa(rc.statusCode),
		metrics.ClientNameLabel:       rc.clientName,
	}).Observe(float64(rc.bytesRead))
	return err
}
