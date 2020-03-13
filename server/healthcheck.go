package healthcheck

import (
	"fmt"
	"log"
	"net/http"
)

type HealthChecker struct {
	serverType string
}

func NewHealthChecker(serverType string) *HealthChecker {
	return &HealthChecker{
		serverType: serverType,
	}
}

// TODO(tylerw): Configure healthchecker to wait on backend service status.

func (h *HealthChecker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqServerType := r.Header.Get("server-type")
	if reqServerType == h.serverType {
		w.Write([]byte("OK"))
		return
	}
	err := fmt.Errorf("Server type: '%s' unknown (did not match: '%s'", reqServerType, h.serverType)
	log.Printf("Health check returning error: %s", err)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}
