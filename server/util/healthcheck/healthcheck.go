package healthcheck

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	maxShutdownDuration = 60 * time.Second
)

type ShutDownFunc func(ctx context.Context) error

type HealthChecker struct {
	serverType    string
	done          chan bool
	quit          chan os.Signal
	shutdownFuncs []ShutDownFunc
	lock          sync.RWMutex // protects: readyToServe
	readyToServe  bool
}

func NewHealthChecker(serverType string) *HealthChecker {
	hc := HealthChecker{
		serverType:    serverType,
		done:          make(chan bool),
		quit:          make(chan os.Signal, 1),
		shutdownFuncs: make([]ShutDownFunc, 0),
		// TODO(tylerw): Configure healthchecker to wait on backend service status.
		readyToServe: true,
	}
	signal.Notify(hc.quit, os.Interrupt)
	go hc.handleShutdownFuncs()
	return &hc
}

func (h *HealthChecker) handleShutdownFuncs() {
	<-h.quit

	h.lock.Lock()
	h.readyToServe = false
	h.lock.Unlock()

	log.Printf("Caught interrupt signal; shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), maxShutdownDuration)
	defer cancel()

	eg, egCtx := errgroup.WithContext(ctx)
	for _, fn := range h.shutdownFuncs {
		eg.Go(func() error {
			if err := fn(egCtx); err != nil {
				log.Printf("Error gracefully shutting down: %s", err)
			}
			return nil
		})
	}
	eg.Wait()
	if err := ctx.Err(); err != nil {
		log.Printf("MaxShutdownDuration exceeded. Exiting anyway...")
	}
	close(h.done)
	log.Printf("Server %q stopped.", h.serverType)
}

func (h *HealthChecker) RegisterShutdownFunction(f ShutDownFunc) {
	h.shutdownFuncs = append(h.shutdownFuncs, f)
}

func (h *HealthChecker) WaitForGracefulShutdown() {
	<-h.done
}

func (h *HealthChecker) ReadinessHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqServerType := r.Header.Get("server-type")
		if reqServerType == h.serverType {
			h.lock.RLock()
			defer h.lock.RUnlock()
			if h.readyToServe {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("OK"))
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			return
		}
		err := fmt.Errorf("Server type: '%s' unknown (did not match: %q)", reqServerType, h.serverType)
		log.Printf("Readiness check returning error: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	})
}

func (h *HealthChecker) LivenessHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqServerType := r.Header.Get("server-type")
		if reqServerType == h.serverType {
			w.Write([]byte("OK"))
			return
		}
		err := fmt.Errorf("Server type: '%s' unknown (did not match: %q)", reqServerType, h.serverType)
		log.Printf("Liveness check returning error: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	})
}
