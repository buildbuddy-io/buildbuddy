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
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	hlpb "github.com/buildbuddy-io/buildbuddy/proto/health"
)

var (
	maxShutdownDuration      = flag.Duration("max_shutdown_duration", 25*time.Second, "Time to wait for shutdown")
	shutdownLameduckDuration = flag.Duration("shutdown_lameduck_duration", 0, "If set, the server will be marked unready but not run shutdown functions until this period passes.")
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
	checkersMu    sync.Mutex
	checkers      map[string]interfaces.Checker
	lastStatus    []*serviceStatus
	serverType    string
	shutdownFuncs []interfaces.CheckerFunc
	mu            sync.RWMutex // protects: readyToServe, shuttingDown
	readyToServe  bool
	shuttingDown  bool
}

// Creates a new health checker function that checks the provided GRPC client
// connection, returning: no error (healthy) when the connection is ready or
// idle, and unhealthy otherwise. If the connection is idle when the health
// checker runs, the health checker attempts to reconnect it.
func NewGRPCHealthCheck(conn *grpc.ClientConn) interfaces.CheckerFunc {
	return interfaces.CheckerFunc(
		func(ctx context.Context) error {
			connState := conn.GetState()
			if connState == connectivity.Ready {
				return nil
			} else if connState == connectivity.Idle {
				conn.Connect()
				return nil
			}
			return fmt.Errorf("gRPC connection not yet ready (state: %s)", connState)
		},
	)
}

func NewHealthChecker(serverType string) *HealthChecker {
	hc := HealthChecker{
		serverType:    serverType,
		done:          make(chan bool),
		quit:          make(chan struct{}),
		shutdownFuncs: make([]interfaces.CheckerFunc, 0),
		readyToServe:  true,
		checkersMu:    sync.Mutex{},
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

	time.Sleep(*shutdownLameduckDuration)

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
	h.checkersMu.Lock()
	h.checkers[name] = f
	h.checkersMu.Unlock()

	// Mark the service as unhealthy until the healthcheck runs
	// and it becomes healthy.
	h.mu.Lock()
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

	h.checkersMu.Lock()
	checkers := make(map[string]interfaces.Checker, len(h.checkers))
	for k, v := range h.checkers {
		checkers[k] = v
	}
	h.checkersMu.Unlock()

	eg, ctx := errgroup.WithContext(ctx)
	for name, ck := range checkers {
		name := name
		checkFn := ck
		eg.Go(func() error {
			err := checkFn.Check(ctx)

			// Update per-service statusData
			statusDataMu.Lock()
			statusData = append(statusData, &serviceStatus{name, err})
			statusDataMu.Unlock()

			if err != nil {
				metrics.HealthCheck.With(prometheus.Labels{
					metrics.HealthCheckName: name,
				}).Set(0)
				return status.UnavailableErrorf("Service %s is unhealthy: %s", name, err)
			}

			metrics.HealthCheck.With(prometheus.Labels{
				metrics.HealthCheckName: name,
			}).Set(1)
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

// serverType is derived from either the headers or a query parameter
func serverType(r *http.Request) string {
	if r.Header.Get("server-type") != "" {
		return r.Header.Get("server-type")
	}
	// GCP load balancer healthchecks do not allow sending headers.
	return r.URL.Query().Get("server-type")
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

func (h *HealthChecker) Check(ctx context.Context, req *hlpb.HealthCheckRequest) (*hlpb.HealthCheckResponse, error) {
	// GRPC does not have indepenent health and readiness checks like HTTP does.
	// An additional wrinkle is that AWS ALB's do not support sending a service
	// name to the GRPC health check. To maximize compatibility and usefulness
	// we ignore the service name for now (sad face), and return:
	//   - SERVING when the service is ready
	//   - NOT_SERVING when the service is not ready
	//   - UNKNOWN when the service is shutting down.
	h.mu.RLock()
	ready := h.readyToServe
	shuttingDown := h.shuttingDown
	h.mu.RUnlock()
	rsp := &hlpb.HealthCheckResponse{}
	if ready {
		rsp.Status = hlpb.HealthCheckResponse_SERVING
	} else {
		rsp.Status = hlpb.HealthCheckResponse_NOT_SERVING
	}

	if shuttingDown {
		rsp.Status = hlpb.HealthCheckResponse_UNKNOWN
	}
	return rsp, nil
}

func (h *HealthChecker) Watch(req *hlpb.HealthCheckRequest, stream hlpb.Health_WatchServer) error {
	return status.UnimplementedError("Watch not implemented")
}
