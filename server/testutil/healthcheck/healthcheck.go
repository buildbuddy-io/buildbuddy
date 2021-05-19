package healthcheck

import (
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
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
