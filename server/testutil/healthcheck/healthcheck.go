package healthcheck

import (
	"context"
	"net/http"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

type TestingHealthChecker struct {
	t testing.TB

	done          chan bool
	shutdownFuncs []interfaces.CheckerFunc
}

func NewTestingHealthChecker(t testing.TB) interfaces.HealthChecker {
	hc := &TestingHealthChecker{
		t:             t,
		done:          make(chan bool),
		shutdownFuncs: []interfaces.CheckerFunc{},
	}
	t.Cleanup(func() {
		for _, fn := range hc.shutdownFuncs {
			if err := fn(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		hc.done <- true
	})
	return hc
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
	<-hc.done
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
