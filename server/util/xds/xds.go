package xds

import (
	"bytes"
	"context"
	"os"
	"text/template"

	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

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
func Bootstrap(ctx context.Context, client kubernetes.Interface) error {
	if !*xdsBootstrapEnabled {
		return nil
	}
	if client == nil {
		cfg, err := rest.InClusterConfig()
		if err != nil {
			return status.UnavailableErrorf("in-cluster config: %s", err)
		}
		client, err = kubernetes.NewForConfig(cfg)
		if err != nil {
			return status.UnavailableErrorf("create k8s client: %s", err)
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

	node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return status.UnavailableErrorf("get node %q: %s", nodeName, err)
	}

	zone := node.Labels[zoneLabel]
	if zone == "" {
		return status.FailedPreconditionErrorf("node %q has no %q label", nodeName, zoneLabel)
	}
	subZone := ""
	if *xdsSubZoneLabel != "" {
		subZone = node.Labels[*xdsSubZoneLabel]
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
