package capabilities_server_proxy

import (
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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
	return s.remote.GetCapabilities(ctx, req)
}
