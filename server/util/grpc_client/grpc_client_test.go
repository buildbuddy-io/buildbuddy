package grpc_client_test

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"

	pspb "github.com/buildbuddy-io/buildbuddy/proto/ping_service"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"fmt"
)

type TestService struct {
	client   *grpc_client.ClientConnPool
	requests atomic.Int64
}

func (ts *TestService) Ping(ctx context.Context, req *pspb.PingRequest) (*pspb.PingResponse, error) {
	ts.requests.Add(1)
	return &pspb.PingResponse{Tag: req.GetTag()}, nil
}

func startServer(t *testing.T, env environment.Env) *TestService {
	port := testport.FindFree(t)
	server, err := grpc_server.New(env, port, false /*=ssl*/, grpc_server.GRPCServerConfig{})
	require.NoError(t, err)
	ts := TestService{requests: atomic.Int64{}}
	pspb.RegisterApiServer(server.GetServer(), &ts)
	require.NoError(t, server.Start())
	client, err := grpc_client.DialInternal(env, fmt.Sprintf("grpc://localhost:%d", port))
	require.NoError(t, err)
	ts.client = client
	return &ts
}

func requireTraffic(t *testing.T, percent, numRequests, margin, actual int) {
	if percent == 0 {
		require.Equal(t, 0, actual, fmt.Sprintf("Expected server receiving 0%% of traffic to receive 0 requests (actually received %d)", actual))
	} else if percent == 100 {
		require.Equal(t, numRequests, actual, fmt.Sprintf("Expected server receiving 100%% of traffic to receive %d requests (actually received %d)", numRequests, actual))
	} else {
		lowerBound := int(math.Floor(float64(numRequests)*float64(percent)/100.0)) - margin
		if lowerBound < 0 {
			lowerBound = 0
		}
		upperBound := int(math.Ceil(float64(numRequests)*float64(percent)/100.0)) + margin
		if upperBound > numRequests {
			upperBound = numRequests
		}
		require.True(t, actual <= upperBound && actual >= lowerBound,
			fmt.Sprintf("Expected server receiving %d%% of traffic to receive between [%d, %d] requests (actually received %d)", percent, lowerBound, upperBound, actual))
	}
}

func TestClientConnPoolSplitter(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	first := startServer(t, te)
	second := startServer(t, te)

	type testCase struct {
		firstPercent  int
		secondPercent int
	}

	testCases := []testCase{
		{firstPercent: 0, secondPercent: 100},
		{firstPercent: 1, secondPercent: 99},
		{firstPercent: 10, secondPercent: 90},
		{firstPercent: 50, secondPercent: 50},
		{firstPercent: 100, secondPercent: 0},
	}

	numRequests := 2_500
	margin := 150

	for _, tc := range testCases {
		splitter, err := grpc_client.NewClientConnPoolSplitter(
			map[*grpc_client.ClientConnPool]int{
				first.client:  tc.firstPercent,
				second.client: tc.secondPercent,
			})
		require.NoError(t, err)

		splitterClient := pspb.NewApiClient(splitter)
		for i := 0; i < numRequests; i++ {
			_, err := splitterClient.Ping(ctx, &pspb.PingRequest{})
			require.NoError(t, err)
		}

		requireTraffic(t, tc.firstPercent, numRequests, margin, int(first.requests.Load()))
		requireTraffic(t, tc.secondPercent, numRequests, margin, int(second.requests.Load()))

		first.requests.Store(int64(0))
		second.requests.Store(int64(0))
	}
}

func fakeNode(name string, labels map[string]string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
	}
}

func TestBootstrapXDS(t *testing.T) {
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
			require.NoError(t, grpc_client.BootstrapXDS(context.Background(), client))

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
	err := grpc_client.BootstrapXDS(context.Background(), client)
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
	err := grpc_client.BootstrapXDS(context.Background(), client)
	require.Error(t, err)
	require.NoFileExists(t, bootstrapPath)
}
