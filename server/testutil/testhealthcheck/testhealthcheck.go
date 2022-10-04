package testhealthcheck

import (
	"context"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	hlpb "github.com/buildbuddy-io/buildbuddy/proto/health"
)

type TestingHealthChecker struct{}

func NewTestingHealthChecker() interfaces.HealthChecker {
	return &TestingHealthChecker{}
}

func (t *TestingHealthChecker) RegisterShutdownFunction(hc interfaces.CheckerFunc) {}
func (t *TestingHealthChecker) AddHealthCheck(name string, f interfaces.Checker)   {}
func (t *TestingHealthChecker) WaitForGracefulShutdown() {
	select {}
}
func (t *TestingHealthChecker) Shutdown() {
}
func (t *TestingHealthChecker) LivenessHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})
}
func (t *TestingHealthChecker) ReadinessHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})
}

func (t *TestingHealthChecker) Check(ctx context.Context, req *hlpb.HealthCheckRequest) (*hlpb.HealthCheckResponse, error) {
	return &hlpb.HealthCheckResponse{
		Status: hlpb.HealthCheckResponse_SERVING,
	}, nil
}

func (t *TestingHealthChecker) Watch(req *hlpb.HealthCheckRequest, stream hlpb.Health_WatchServer) error {
	return status.UnimplementedError("Watch not implemented")
}
