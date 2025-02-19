package grpc_client_test

import (
	"context"
	"math"
	"sync/atomic"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/stretchr/testify/require"

	pspb "github.com/buildbuddy-io/buildbuddy/proto/ping_service"

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
