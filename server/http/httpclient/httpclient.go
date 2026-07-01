package httpclient

import (
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
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

// StatusRecorder records the HTTP status code of the most recent HTTP exchange
// observed by a statusRecorderTransport. Each round trip overwrites the recorded
// code with its own outcome (0 if the round trip produced no response), so the
// recorded code always reflects the immediately-preceding request rather than
// the last error seen at any point.
type StatusRecorder struct {
	mu         sync.Mutex
	statusCode int
}

// NewStatusRecorder returns a recorder for HTTP status codes observed by
// NewStatusRecorderTransport.
func NewStatusRecorder() *StatusRecorder {
	return &StatusRecorder{}
}

// HTTPStatusCode returns the status code of the most recent HTTP exchange if it
// was an error status (>= 400), reporting false otherwise (including for a
// successful response or a round trip that never received a response).
func (r *StatusRecorder) HTTPStatusCode() (int, bool) {
	if r == nil {
		return 0, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.statusCode < http.StatusBadRequest {
		return 0, false
	}
	return r.statusCode, true
}

func (r *StatusRecorder) record(statusCode int) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statusCode = statusCode
}

// NewStatusRecorderTransport wraps inner and records HTTP response status codes
// without modifying the response.
func NewStatusRecorderTransport(inner http.RoundTripper, statusRecorder *StatusRecorder) http.RoundTripper {
	return &statusRecorderTransport{
		inner:          inner,
		statusRecorder: statusRecorder,
	}
}

type statusRecorderTransport struct {
	inner          http.RoundTripper
	statusRecorder *StatusRecorder
}

func (t *statusRecorderTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.inner.RoundTrip(req)
	if resp != nil {
		t.statusRecorder.record(resp.StatusCode)
	} else {
		// The round trip produced no response (e.g. a connection error), so
		// clear any status recorded by a previous request.
		t.statusRecorder.record(0)
	}
	return resp, err
}
