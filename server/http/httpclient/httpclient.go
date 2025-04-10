package httpclient

import (
	"errors"
	"flag"
	"net"
	"net/http"
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
// metrics on any requests made,.
//
// If you just want to make an HTTP request, this is the client to use.
func New() *http.Client {
	return NewWithAllowedPrivateIPs([]*net.IPNet{})
}

// NewWithAllowPrivateIPs creates an HTTP client blocks connections to all but
// the specified private IPs and records metrics on any requests made.
func NewWithAllowedPrivateIPs(allowedPrivateIPNets []*net.IPNet) *http.Client {
	inner := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: 30 * time.Second,
			Control: blockingDialerControl(allowedPrivateIPNets),
		}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
		Proxy:               http.ProxyFromEnvironment,
	}
	tp := newMetricsTransport(inner)

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
		for _, ipNet := range allowed {
			if ipNet.Contains(ip) {
				return nil
			}
		}
		if (ip.IsLoopback() && !*allowLocalhost) || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
			log.Infof("Blocked Fetch for address %s", address)
			return errors.New("IP address not allowed")
		}
		return nil
	}
}

// verify that metricsTransport implements the RoundTripper interface
var _ http.RoundTripper = (*metricsTransport)(nil)

type metricsTransport struct {
	inner http.RoundTripper
}

func newMetricsTransport(inner http.RoundTripper) http.RoundTripper {
	return &metricsTransport{
		inner: inner,
	}
}

func (t *metricsTransport) RoundTrip(in *http.Request) (out *http.Response, err error) {
	host := in.URL.Hostname()
	var hostLabel string
	if net.ParseIP(host) != nil {
		hostLabel = "[IP_ADDRESS]"
	} else {
		hostLabel, err = publicsuffix.EffectiveTLDPlusOne(host)
		if err != nil {
			hostLabel = "[UNKNOWN]"
		}
	}
	metrics.HTTPClientRequestCount.With(prometheus.Labels{
		metrics.HTTPHostLabel:   hostLabel,
		metrics.HTTPMethodLabel: in.Method,
	}).Inc()
	return t.inner.RoundTrip(in)
}
