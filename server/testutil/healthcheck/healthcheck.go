package healthcheck

import (
	"context"
	"net/http"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

type TestingHealthChecker struct {
	t testing.TB
}

func NewTestingHealthChecker(t testing.TB) interfaces.HealthChecker {
	return &TestingHealthChecker{t}
}

func (hc *TestingHealthChecker) RegisterShutdownFunction(f interfaces.CheckerFunc) {
	hc.t.Cleanup(func() {
		if err := f(context.Background()); err != nil {
			hc.t.Fatal(err)
		}
	})
}
func (hc *TestingHealthChecker) AddHealthCheck(name string, f interfaces.Checker) {}
func (hc *TestingHealthChecker) WaitForGracefulShutdown() {
	select {}
}
func (hc *TestingHealthChecker) LivenessHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})
}
func (hc *TestingHealthChecker) ReadinessHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})
}
