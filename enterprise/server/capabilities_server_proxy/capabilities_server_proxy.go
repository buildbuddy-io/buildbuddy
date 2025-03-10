package capabilities_server_proxy

import (
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gstatus "google.golang.org/grpc/status"
)

type CapabilitiesServerProxy struct {
	env    environment.Env
	remote repb.CapabilitiesClient
}

func Register(env *real_environment.RealEnv) error {
	capabilitiesServer, err := NewCapabilitiesServerProxy(env)
	if err != nil {
		return status.InternalErrorf("Error initializing CapabilitiesServerProxy: %s", err)
	}
	env.SetCapabilitiesServer(capabilitiesServer)
	return nil
}

func NewCapabilitiesServerProxy(env environment.Env) (*CapabilitiesServerProxy, error) {
	if env.GetCapabilitiesClient() == nil {
		return nil, fmt.Errorf("A CapabilitiesClient is required to enable the CapabilitiesServerProxy")
	}
	return &CapabilitiesServerProxy{
		env:    env,
		remote: env.GetCapabilitiesClient(),
	}, nil
}

func (s *CapabilitiesServerProxy) GetCapabilities(ctx context.Context, req *repb.GetCapabilitiesRequest) (*repb.ServerCapabilities, error) {
	resp, err := s.remote.GetCapabilities(ctx, req)
	labels := prometheus.Labels{
		metrics.StatusLabel:        fmt.Sprintf("%d", gstatus.Code(err)),
		metrics.CacheHitMissStatus: "miss",
	}
	metrics.CapabilitiesProxiedRequests.With(labels).Inc()
	metrics.CapabilitiesProxiedBytes.With(labels).Add(float64(proto.Size(resp)))
	return resp, err
}
