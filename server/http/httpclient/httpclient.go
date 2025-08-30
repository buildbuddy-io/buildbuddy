package httpclient

import (
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"strconv"
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
	inner      http.RoundTripper
	clientName string
}

func newMetricsTransport(inner http.RoundTripper, clientName string) http.RoundTripper {
	return &metricsTransport{
		inner:      inner,
		clientName: clientName,
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
