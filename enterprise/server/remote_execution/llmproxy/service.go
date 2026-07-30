package llmproxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/llmproxy/ports"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

// SessionRegistrar binds an execution-scoped session to a listener belonging
// to exactly one VM. The returned function closes the listener and all accepted
// connections, and is safe to call more than once.
type SessionRegistrar interface {
	RegisterSession(listener net.Listener, session *Session) (revoke func(), err error)
}

// Service owns the execution-scoped HTTP servers running over per-VM vsock
// listeners. It does not expose a host TCP listener.
type Service struct {
	mu      sync.Mutex
	closed  bool
	servers map[*http.Server]struct{}
}

func NewService() (*Service, error) {
	return &Service{
		servers: make(map[*http.Server]struct{}),
	}, nil
}

// BaseURL is the guest-loopback HTTP origin served by goinit. goinit forwards
// each connection over the VM's vsock device to its executor-side listener.
func (s *Service) BaseURL() string {
	return "http://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(ports.GuestHTTP))
}

func (s *Service) RegisterSession(listener net.Listener, session *Session) (func(), error) {
	if listener == nil {
		return nil, errors.New("listener is required")
	}
	handler, err := NewHandler(Options{
		Session: session,
	})
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       90 * time.Second,
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = listener.Close()
		return nil, errors.New("LLM proxy service is shut down")
	}
	s.servers[server] = struct{}{}
	s.mu.Unlock()

	var once sync.Once
	revoke := func() {
		once.Do(func() {
			// Close, rather than Shutdown, so that keepalive and streaming
			// connections cannot survive into the next execution on a recycled
			// VM.
			if err := server.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				log.Warningf("Could not close VM LLM proxy server: %s", err)
			}
			handler.CloseIdleConnections()
			s.mu.Lock()
			delete(s.servers, server)
			s.mu.Unlock()
		})
	}

	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			log.Errorf("VM LLM proxy server stopped unexpectedly: %s", err)
		}
		revoke()
	}()
	return revoke, nil
}

func (s *Service) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	servers := make([]*http.Server, 0, len(s.servers))
	for server := range s.servers {
		servers = append(servers, server)
	}
	s.mu.Unlock()

	var shutdownErr error
	for _, server := range servers {
		if err := server.Shutdown(ctx); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("shut down VM LLM proxy server: %w", err))
			_ = server.Close()
		}
	}
	return shutdownErr
}
