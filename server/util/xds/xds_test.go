package xds_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/xds"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func fakeNode(name string, labels map[string]string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
	}
}

func TestBootstrap(t *testing.T) {
	const (
		nodeName     = "node-a"
		podName      = "executor-xyz"
		zone         = "us-west1-b"
		subZoneLabel = "topology.buildbuddy.io/sub-zone"
	)

	type wantLocality struct {
		zone    string
		subZone string
	}
	for _, tc := range []struct {
		name         string
		nodeLabels   map[string]string
		subZoneLabel string
		want         wantLocality
	}{
		{
			name:       "zone_only",
			nodeLabels: map[string]string{"topology.kubernetes.io/zone": zone},
			want:       wantLocality{zone: zone},
		},
		{
			name: "zone_and_sub_zone",
			nodeLabels: map[string]string{
				"topology.kubernetes.io/zone": zone,
				subZoneLabel:                  "sz42",
			},
			subZoneLabel: subZoneLabel,
			want:         wantLocality{zone: zone, subZone: "sz42"},
		},
		{
			name:         "sub_zone_label_configured_but_absent_on_node",
			nodeLabels:   map[string]string{"topology.kubernetes.io/zone": zone},
			subZoneLabel: subZoneLabel,
			want:         wantLocality{zone: zone},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bootstrapPath := filepath.Join(t.TempDir(), "bootstrap.json")
			t.Setenv("MY_NODE_NAME", nodeName)
			t.Setenv("MY_POD_NAME", podName)
			t.Setenv("GRPC_XDS_BOOTSTRAP", bootstrapPath)
			flags.Set(t, "grpc_client.xds.bootstrap", true)
			flags.Set(t, "grpc_client.xds.server_uri", "xds.example.com:5000")
			flags.Set(t, "grpc_client.xds.sub_zone_label", tc.subZoneLabel)

			client := fake.NewClientset(fakeNode(nodeName, tc.nodeLabels))
			require.NoError(t, xds.Bootstrap(context.Background(), client))

			data, err := os.ReadFile(bootstrapPath)
			require.NoError(t, err)

			var got struct {
				XDSServers []struct {
					ServerURI      string   `json:"server_uri"`
					ServerFeatures []string `json:"server_features"`
				} `json:"xds_servers"`
				Node struct {
					ID       string `json:"id"`
					Locality struct {
						Zone    string `json:"zone"`
						SubZone string `json:"sub_zone"`
					} `json:"locality"`
				} `json:"node"`
			}
			require.NoError(t, json.Unmarshal(data, &got))

			require.Len(t, got.XDSServers, 1)
			require.Equal(t, "xds.example.com:5000", got.XDSServers[0].ServerURI)
			require.Equal(t, []string{"xds_v3"}, got.XDSServers[0].ServerFeatures)
			require.Equal(t, podName, got.Node.ID)
			require.Equal(t, tc.want.zone, got.Node.Locality.Zone)
			require.Equal(t, tc.want.subZone, got.Node.Locality.SubZone)
		})
	}
}

func TestBootstrapXDS_MissingZoneLabel(t *testing.T) {
	bootstrapPath := filepath.Join(t.TempDir(), "bootstrap.json")
	t.Setenv("MY_NODE_NAME", "node-a")
	t.Setenv("MY_POD_NAME", "executor-xyz")
	t.Setenv("GRPC_XDS_BOOTSTRAP", bootstrapPath)
	flags.Set(t, "grpc_client.xds.bootstrap", true)
	flags.Set(t, "grpc_client.xds.server_uri", "xds.example.com:5000")
	flags.Set(t, "grpc_client.xds.sub_zone_label", "")

	client := fake.NewClientset(fakeNode("node-a", map[string]string{"other": "label"}))
	err := xds.Bootstrap(context.Background(), client)
	require.Error(t, err)
	require.NoFileExists(t, bootstrapPath)
}

func TestBootstrapXDS_MissingPodName(t *testing.T) {
	bootstrapPath := filepath.Join(t.TempDir(), "bootstrap.json")
	t.Setenv("MY_NODE_NAME", "node-a")
	t.Setenv("MY_POD_NAME", "")
	t.Setenv("GRPC_XDS_BOOTSTRAP", bootstrapPath)
	flags.Set(t, "grpc_client.xds.bootstrap", true)
	flags.Set(t, "grpc_client.xds.server_uri", "xds.example.com:5000")
	flags.Set(t, "grpc_client.xds.sub_zone_label", "")

	client := fake.NewClientset(fakeNode("node-a", map[string]string{"topology.kubernetes.io/zone": "us-west1-b"}))
	err := xds.Bootstrap(context.Background(), client)
	require.Error(t, err)
	require.NoFileExists(t, bootstrapPath)
}
