package healthcheck

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"golang.org/x/sync/errgroup"
)

var (
	maxShutdownDuration = flag.Duration("max_shutdown_duration", 25*time.Second, "Time to wait for shutdown")
)

const (
	healthCheckPeriod  = 3 * time.Second // The time to wait between health checks.
	healthCheckTimeout = 2 * time.Second // How long a health check may take, max.
)

type serviceStatus struct {
	Name  string
	Error error
}

type HealthChecker struct {
	done          chan bool
	quit          chan struct{}
	checkers      map[string]interfaces.Checker
	lastStatus    []*serviceStatus
	serverType    string
	shutdownFuncs []interfaces.CheckerFunc
	mu            sync.RWMutex // protects: readyToServe, shuttingDown
	readyToServe  bool
	shuttingDown  bool
}

func NewHealthChecker(serverType string) *HealthChecker {
	hc := HealthChecker{
		serverType:    serverType,
		done:          make(chan bool),
		quit:          make(chan struct{}),
		shutdownFuncs: make([]interfaces.CheckerFunc, 0),
		readyToServe:  true,
		checkers:      make(map[string]interfaces.Checker, 0),
		lastStatus:    make([]*serviceStatus, 0),
	}
	sigTerm := make(chan os.Signal)
	go func() {
		<-sigTerm
		close(hc.quit)
	}()
	signal.Notify(sigTerm, os.Interrupt, syscall.SIGTERM)
	go hc.handleShutdownFuncs()
	go func() {
		for {
			select {
			case <-hc.quit:
				return
			case <-time.After(healthCheckPeriod):
				hc.runHealthChecks(context.Background())
			}
		}
	}()
	statusz.AddSection("healthcheck", "Backend service health checks", &hc)
	return &hc
}

func (h *HealthChecker) Statusz(ctx context.Context) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	buf := `<table style="width: 150px;"><tr><th>Name</th><th>Status</th></tr>`
	for _, serviceStatus := range h.lastStatus {
		statusString := "OK"
		if serviceStatus.Error != nil {
			statusString = serviceStatus.Error.Error()
		}
		buf += fmt.Sprintf("<tr><td>%s</td><td>%s</td></tr>", serviceStatus.Name, statusString)
	}
	buf += "</table>"
	return buf
}

func (h *HealthChecker) handleShutdownFuncs() {
	<-h.quit

	h.mu.Lock()
	h.readyToServe = false
	h.shuttingDown = true
	h.mu.Unlock()

	// We use fmt here and below because this code is called from the
	// signal handler and log.Printf can be a little wonky.
	fmt.Printf("Caught interrupt signal; shutting down...\n")
	ctx, cancel := context.WithTimeout(context.Background(), *maxShutdownDuration)
	defer cancel()

	eg, egCtx := errgroup.WithContext(ctx)
	for _, fn := range h.shutdownFuncs {
		f := fn
		eg.Go(func() error {
			if err := f(egCtx); err != nil {
				fmt.Printf("Error gracefully shutting down: %s\n", err)
			}
			return nil
		})
	}
	eg.Wait()
	if err := ctx.Err(); err != nil {
		fmt.Printf("MaxShutdownDuration exceeded. Non-graceful exit.\n")
	}
	time.Sleep(10 * time.Millisecond)
	fmt.Printf("Server %q stopped.\n", h.serverType)
	close(h.done)
}

func (h *HealthChecker) RegisterShutdownFunction(f interfaces.CheckerFunc) {
	h.shutdownFuncs = append(h.shutdownFuncs, f)
}

func (h *HealthChecker) AddHealthCheck(name string, f interfaces.Checker) {
	// Mark the service as unhealthy until the healthcheck runs
	// and it becomes healthy.
	h.mu.Lock()
	h.checkers[name] = f
	h.readyToServe = false
	h.mu.Unlock()
}

func (h *HealthChecker) WaitForGracefulShutdown() {
	h.runHealthChecks(context.Background())
	<-h.done
}

func (h *HealthChecker) Shutdown() {
	close(h.quit)
}

func (h *HealthChecker) runHealthChecks(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, healthCheckTimeout)
	defer cancel()

	statusData := make([]*serviceStatus, 0)
	statusDataMu := sync.Mutex{}

	eg, ctx := errgroup.WithContext(ctx)
	for name, ck := range h.checkers {
		name := name
		checkFn := ck
		eg.Go(func() error {
			err := checkFn.Check(ctx)

			// Update per-service statusData
			statusDataMu.Lock()
			statusData = append(statusData, &serviceStatus{name, err})
			statusDataMu.Unlock()

			if err != nil {
				return status.UnavailableErrorf("Service %s is unhealthy: %s", name, err)
			}
			return nil
		})
	}
	err := eg.Wait()
	newReadinessState := true
	if err != nil {
		newReadinessState = false
		log.Warningf("Checker err: %s", err)
	}

	previousReadinessState := false
	h.mu.Lock()
	if !h.shuttingDown {
		previousReadinessState = h.readyToServe
		h.readyToServe = newReadinessState
		h.lastStatus = statusData
	}
	h.mu.Unlock()

	if newReadinessState != previousReadinessState {
		log.Infof("HealthChecker transitioning from ready: %t => ready: %t", previousReadinessState, newReadinessState)
	}
}

func (h *HealthChecker) ReadinessHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqServerType := serverType(r)
		if reqServerType == h.serverType {
			h.mu.RLock()
			ready := h.readyToServe
			h.mu.RUnlock()

			if ready {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("OK"))
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			return
		}
		err := fmt.Errorf("Server type: '%s' unknown (did not match: %q)", reqServerType, h.serverType)
		log.Warningf("Readiness check returning error: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	})
}

func (h *HealthChecker) LivenessHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqServerType := serverType(r)
		if reqServerType == h.serverType {
			w.Write([]byte("OK"))
			return
		}
		err := fmt.Errorf("Server type: '%s' unknown (did not match: %q)", reqServerType, h.serverType)
		log.Warningf("Liveness check returning error: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	})
}

// serverType is dervied from either the headers or a query parameter
func serverType(r *http.Request) string {
	if r.Header.Get("server-type") != "" {
		return r.Header.Get("server-type")
	}
	// GCP load balancer healthchecks do not allow sending headers.
	return r.URL.Query().Get("server-type")
}
