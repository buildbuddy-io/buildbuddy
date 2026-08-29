package xds

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"text/template"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

// userAgent identifies this client in API server audit logs by binary name
// and platform. client-go's DefaultKubernetesUserAgent additionally appends
// its own version and git commit ("kubernetes/<commit>"); those are not
// meaningful here and are deliberately omitted.
var userAgent = filepath.Base(os.Args[0]) + " (" + runtime.GOOS + "/" + runtime.GOARCH + ")"

const (
	// xdsBootstrapFileEnv is the env var grpc-go reads the xDS bootstrap
	// file path from. Its value is captured by grpc-go at package init. The
	// file contents are read lazily on first xDS use.
	xdsBootstrapFileEnv = "GRPC_XDS_BOOTSTRAP"

	// zoneLabel is the well-known Kubernetes Node label for failure-domain
	// zone.
	zoneLabel = "topology.kubernetes.io/zone"
)

var (
	xdsBootstrapEnabled = flag.Bool("grpc_client.xds.bootstrap", false, "Whether to dynamically generate the XDS bootstrap to the file at path GRPC_XDS_BOOTSTRAP.", flag.Internal)
	xdsServerURI        = flag.String("grpc_client.xds.server_uri", "", "URI of the xDS control plane (host:port). Required if xds bootstrap is enabled.", flag.Internal)
	xdsSubZoneLabel     = flag.String("grpc_client.xds.sub_zone_label", "", "Optional Kubernetes Node label whose value populates locality.sub_zone in the generated xDS bootstrap config. If empty or the label is absent on the Node, sub_zone is omitted.", flag.Internal)
)

var xdsBootstrapTmpl = template.Must(template.New("xds-bootstrap").Parse(`{
  "xds_servers": [
    {
      "server_uri": "{{.ServerURI}}",
      "channel_creds": [{"type": "insecure"}],
      "server_features": ["xds_v3"]
    }
  ],
  "node": {
    "id": "{{.NodeID}}",
    "locality": {
      "zone": "{{.Zone}}",
      "sub_zone": "{{.SubZone}}"
    }
  }
}
`))

// Bootstrap generates the grpc-go xDS bootstrap config for the current pod.
//
// This function must be called early before any gRPC clients are created.
//
// GRPC_XDS_BOOTSTRAP environment is expected to be set to a writable path.
//
// MY_NODE_NAME environment variable is expected to be set to the kubernetes
// node on which the pod is running. It should be set to the pod node using a
// fieldRef:
//   - name: MY_NODE_NAME
//     valueFrom:
//     fieldRef:
//     fieldPath: spec.nodeName
//
// MY_POD_NAME is used to identify the client to the xDS control plane. It
// should be set using a fieldRef:
//   - name: MY_POD_NAME
//     valueFrom:
//     fieldRef:
//     fieldPath: metadata.name
//
// The locality information for the xDS bootstrap config is obtained by querying
// the node information using the in-cluster kubernetes API.
//
// The zone is read from the Node's "topology.kubernetes.io/zone" label.
// if --grpc_client.xds_sub_zone_label is set and the Node carries that label,
// its value is written to locality.sub_zone.
//
// If client is nil, an in-cluster Kubernetes client is created.
func Bootstrap(ctx context.Context, client NodeLabelGetter) error {
	if !*xdsBootstrapEnabled {
		return nil
	}
	if client == nil {
		var err error
		client, err = NewInClusterNodeLabelGetter()
		if err != nil {
			return status.UnavailableErrorf("create in-cluster k8s client: %s", err)
		}
	}
	bootstrapPath := os.Getenv(xdsBootstrapFileEnv)
	if bootstrapPath == "" {
		return status.FailedPreconditionErrorf("%s env var is not set", xdsBootstrapFileEnv)
	}
	if *xdsServerURI == "" {
		return status.FailedPreconditionError("--grpc_client.xds.server_uri flag is not set")
	}
	nodeName := resources.GetK8sNodeName()
	if nodeName == "" {
		return status.FailedPreconditionError("node name is not set (expose spec.nodeName as MY_NODE_NAME via downward API)")
	}
	podName := resources.GetK8sPodName()
	if podName == "" {
		return status.FailedPreconditionError("pod name is not set (expose metadata.name as MY_POD_NAME via downward API)")
	}

	labels, err := client.NodeLabels(ctx, nodeName)
	if err != nil {
		return status.UnavailableErrorf("get node %q: %s", nodeName, err)
	}

	zone := labels[zoneLabel]
	if zone == "" {
		return status.FailedPreconditionErrorf("node %q has no %q label", nodeName, zoneLabel)
	}
	subZone := ""
	if *xdsSubZoneLabel != "" {
		subZone = labels[*xdsSubZoneLabel]
	}

	var buf bytes.Buffer
	err = xdsBootstrapTmpl.Execute(&buf, struct {
		ServerURI, NodeID, Zone, SubZone string
	}{
		ServerURI: *xdsServerURI,
		NodeID:    podName,
		Zone:      zone,
		SubZone:   subZone,
	})
	if err != nil {
		return status.InternalErrorf("render xDS bootstrap config: %s", err)
	}
	if err := os.WriteFile(bootstrapPath, buf.Bytes(), 0644); err != nil {
		return status.UnavailableErrorf("write xDS bootstrap file %q: %s", bootstrapPath, err)
	}
	log.Infof("Wrote xDS bootstrap config to %q:\n%s", bootstrapPath, buf.String())
	return nil
}

// NodeLabelGetter returns the labels of a Kubernetes Node.
//
// This is the only piece of the Kubernetes API that xDS bootstrapping needs.
// It is deliberately a narrow interface rather than kubernetes.Interface: the
// typed clientset (and k8s.io/api behind it) adds ~200 packages to every binary
// that links this package, including the executor.
type NodeLabelGetter interface {
	NodeLabels(ctx context.Context, nodeName string) (map[string]string, error)
}

// In-cluster service account credentials, as mounted by the kubelet. These
// are the same paths and env vars client-go's rest.InClusterConfig uses.
const (
	inClusterTokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	inClusterCAFile    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	inClusterHostEnv   = "KUBERNETES_SERVICE_HOST"
	inClusterPortEnv   = "KUBERNETES_SERVICE_PORT"
)

// NewInClusterNodeLabelGetter returns a NodeLabelGetter for the API server of
// the cluster this process runs in, authenticated with the pod's service
// account. It replaces rest.InClusterConfig + rest.HTTPClientFor: the API
// server address comes from the KUBERNETES_SERVICE_{HOST,PORT} env vars, the
// server is verified against the mounted CA bundle, and the mounted bearer
// token is re-read on every request so that projected token rotation works
// without a client-go reloading transport.
//
// Deliberate differences from client-go, all fail-closed or irrelevant for
// the single GET this package makes at startup: an unreadable or empty CA
// bundle is an error (client-go logs and falls back to system roots); a
// token file read error fails the request instead of reusing a cached
// token; requests have a 30s timeout. Proxy env vars, SNI (derived from the
// request host, as client-go) and HTTP/2 behave the same.
func NewInClusterNodeLabelGetter() (NodeLabelGetter, error) {
	return newInClusterNodeLabelGetter(os.Getenv(inClusterHostEnv), os.Getenv(inClusterPortEnv), inClusterTokenFile, inClusterCAFile)
}

func newInClusterNodeLabelGetter(host, port, tokenFile, caFile string) (NodeLabelGetter, error) {
	if host == "" || port == "" {
		return nil, fmt.Errorf("%s and %s must be defined (not running in a Kubernetes pod?)", inClusterHostEnv, inClusterPortEnv)
	}
	if _, err := os.ReadFile(tokenFile); err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read service account CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("no certificates found in %s", caFile)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	client := &http.Client{
		Timeout:   30 * time.Second,
		Transport: &bearerTokenFileTransport{base: transport, tokenFile: tokenFile},
	}
	return NewNodeLabelGetter("https://"+net.JoinHostPort(host, port), client)
}

// bearerTokenFileTransport adds "Authorization: Bearer <token>" to every
// request, reading the token file each time (it is small and the kubelet
// rotates projected tokens in place).
type bearerTokenFileTransport struct {
	base      http.RoundTripper
	tokenFile string
}

func (t *bearerTokenFileTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	token, err := os.ReadFile(t.tokenFile)
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	return t.base.RoundTrip(req)
}

// NewNodeLabelGetter returns a NodeLabelGetter that reads Nodes from the API
// server at host (a URL, or a bare host:port which is treated as https, as
// client-go does) using the given HTTP client, which is responsible for
// authentication and TLS. A nil client uses http.DefaultClient.
func NewNodeLabelGetter(host string, client *http.Client) (NodeLabelGetter, error) {
	if client == nil {
		client = http.DefaultClient
	}
	if !strings.Contains(host, "://") {
		host = "https://" + host
	}
	base, err := url.Parse(host)
	if err != nil {
		return nil, fmt.Errorf("parse API server host %q: %w", host, err)
	}
	if base.Host == "" {
		return nil, fmt.Errorf("parse API server host %q: missing host", host)
	}
	return &restNodeLabelGetter{client: client, base: base}, nil
}

type restNodeLabelGetter struct {
	client *http.Client
	base   *url.URL
}

func (g *restNodeLabelGetter) NodeLabels(ctx context.Context, nodeName string) (map[string]string, error) {
	// Equivalent to the typed clientset's CoreV1().Nodes().Get: the core API
	// group lives under /api/v1 relative to the configured host (which may
	// itself carry a path prefix).
	u := g.base.JoinPath("api", "v1", "nodes", nodeName)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)
	resp, err := g.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		// The API server returns a metav1.Status document on errors; surface
		// its message rather than the raw JSON when we can.
		var st struct {
			Message string `json:"message"`
		}
		msg := strings.TrimSpace(string(body))
		if json.Unmarshal(body, &st) == nil && st.Message != "" {
			msg = st.Message
		}
		if len(msg) > 512 {
			msg = msg[:512]
		}
		return nil, fmt.Errorf("GET %s: HTTP %d: %s", u.Path, resp.StatusCode, msg)
	}
	var node struct {
		Metadata struct {
			Labels map[string]string `json:"labels"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(body, &node); err != nil {
		return nil, fmt.Errorf("decode node %q: %w", nodeName, err)
	}
	return node.Metadata.Labels, nil
}
