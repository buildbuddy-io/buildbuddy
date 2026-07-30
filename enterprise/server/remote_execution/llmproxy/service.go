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
	server   *http.Server
	listener net.Listener
	baseURL  string
}

// NewService starts an executor-local proxy bound to listenIP. A port of zero
// asks the kernel to select an available port.
func NewService(listenIP string, port int) (*Service, error) {
	registry := NewSessionRegistry()
	handler, err := NewHandler(Options{
		SessionResolver: registry,
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
	return s.server.Shutdown(ctx)
}
