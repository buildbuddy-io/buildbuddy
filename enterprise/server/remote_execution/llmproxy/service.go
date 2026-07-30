package llmproxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

type Service struct {
	registry *SessionRegistry
	reporter EventReporter
	server   *http.Server
	listener net.Listener
	baseURL  string
}

// NewService starts an executor-local proxy bound to listenIP. A port of zero
// asks the kernel to select an available port.
func NewService(listenIP string, port int, reporters ...EventReporter) (*Service, error) {
	registry := NewSessionRegistry()
	var reporter EventReporter
	if len(reporters) > 0 {
		reporter = reporters[0]
	}
	handler, err := NewHandler(Options{
		SessionResolver: registry,
		EventReporter:   reporter,
	})
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(listenIP, strconv.Itoa(port)))
	if err != nil {
		return nil, fmt.Errorf("listen for LLM proxy: %w", err)
	}
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       90 * time.Second,
	}
	service := &Service{
		registry: registry,
		reporter: reporter,
		server:   server,
		listener: listener,
		baseURL:  "http://" + listener.Addr().String(),
	}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Errorf("LLM proxy server stopped unexpectedly: %s", err)
		}
	}()
	return service, nil
}

func (s *Service) BaseURL() string {
	return s.baseURL
}

func (s *Service) Port() int {
	return s.listener.Addr().(*net.TCPAddr).Port
}

func (s *Service) RegisterSession(sourceIP string, session *Session) (func(), error) {
	return s.registry.RegisterSession(sourceIP, session)
}

func (s *Service) Shutdown(ctx context.Context) error {
	serverErr := s.server.Shutdown(ctx)
	if reporter, ok := s.reporter.(interface {
		Shutdown(context.Context) error
	}); ok {
		if err := reporter.Shutdown(ctx); serverErr == nil {
			serverErr = err
		}
	}
	return serverErr
}
