package httpclient

import (
	"errors"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/publicsuffix"
)

const maxHTTPTimeout = 60 * time.Minute

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
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
			log.Infof("Blocked Fetch for address %s", address)
			return errors.New("IP address not allowed")
		}
		return nil
	}
}

func NewClient(timeout time.Duration, allowedPrivateIPNets []*net.IPNet) *http.Client {
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

func NewClientWithLocalhost(timeout time.Duration) *http.Client {
	allowedPrivateIPNets := []*net.IPNet{
		&net.IPNet{
			IP:   net.IPv4(127, 0, 0, 0),
			Mask: net.CIDRMask(8, 32),
		},
		&net.IPNet{
			IP:   net.ParseIP("::1"),
			Mask: net.CIDRMask(128, 128),
		},
	}
	return NewClient(timeout, allowedPrivateIPNets)
}

func NewClientNoPrivateIPs(timeout time.Duration) *http.Client {
	return NewClient(timeout, []*net.IPNet{})
}

type metricsTransport struct {
	inner http.RoundTripper
}

func newMetricsTransport(inner http.RoundTripper) http.RoundTripper {
	return &metricsTransport{
		inner: inner,
	}
}

func sanitizeHost(host string) string {
	if net.ParseIP(host) != nil {
		return "ip_address"
	}

	parts := strings.Split(host, ".")
	if len(parts) >= 2 {
		return strings.Join(parts[len(parts)-2:], ".")
	}

	return host
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
