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

const maxHTTPTimeout = 60 * time.Minute

// New creates an HTTP client that blocks connections to private IPs, records metrics on any requests made,
// and has a consistent timeout (see maxHTTPTimeout).
//
// If you just want to make an HTTP request, this is the client to use.
func New() *http.Client {
	return NewWithAllowedPrivateIPs(maxHTTPTimeout, []*net.IPNet{})
}

// NewWithAllowPrivateIPs creates an HTTP client blocks connections to all but the specified private IPs, records metrics on any requests made,
// and uses the specified timeout (passing a timeout of 0 will use maxHTTPTimeout),
func NewWithAllowedPrivateIPs(timeout time.Duration, allowedPrivateIPNets []*net.IPNet) *http.Client {
	dialerTimeout := timeout
	if timeout == 0 || timeout > maxHTTPTimeout {
		dialerTimeout = maxHTTPTimeout
	}

	inner := &http.Transport{
		Dial: (&net.Dialer{
			Timeout: dialerTimeout,
			Control: blockingDialerControl(allowedPrivateIPNets),
		}).Dial,
		TLSHandshakeTimeout: timeout,
		Proxy:               http.ProxyFromEnvironment,
	}
	tp := newMetricsTransport(inner)

	return &http.Client{
		Timeout:   timeout,
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
