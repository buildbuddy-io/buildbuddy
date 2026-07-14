// Package git_trace2 processes Git Trace2 events in real time.
package git_trace2

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// maxEventSize limits the memory allocated for a single newline-delimited
	// Trace2 JSON event.
	maxEventSize = 1 << 20
	// acceptDrainPeriod allows the server to accept child Git connections that
	// were queued immediately before the parent command exited.
	acceptDrainPeriod = 10 * time.Millisecond
	// connectionDrainTimeout limits how long shutdown waits for active Git
	// processes to close their Trace2 streams before closing them forcibly.
	connectionDrainTimeout = time.Second
	// trace2TargetPrefix tells Git to connect to the target as a Unix stream
	// socket rather than open it as a file path.
	trace2TargetPrefix = "af_unix:stream:"
)

// Event is a decoded Git Trace2 event.
type Event struct {
	// Type identifies the Trace2 event type, such as data or region_enter.
	Type string `json:"event"`
	// SessionID identifies the Git process and its ancestry.
	SessionID string `json:"sid"`
	// Thread identifies the thread that emitted the event.
	Thread string `json:"thread"`
	// Category groups related data and region events.
	Category string `json:"category"`
	// Label identifies a Trace2 region.
	Label string `json:"label"`
	// Key identifies a Trace2 data value.
	Key string `json:"key"`
	// Value contains an untyped Trace2 data value.
	Value json.RawMessage `json:"value"`
}

// Handler processes a decoded Trace2 event. It may be called concurrently for
// events emitted by different Git processes.
type Handler func(Event)

// Server receives Trace2 event streams from Git processes over a Unix socket.
type Server struct {
	dir        string
	listener   *net.UnixListener
	handler    Handler
	acceptDone chan struct{}
	stopOnce   sync.Once

	mu           sync.Mutex
	connections  map[net.Conn]struct{}
	connectionWG sync.WaitGroup
}

// NewServer starts a Trace2 event server that calls handler for each valid
// event it receives.
func NewServer(handler Handler) (*Server, error) {
	if handler == nil {
		return nil, errors.New("handler is required")
	}
	dir, err := os.MkdirTemp("", "git-trace2-")
	if err != nil {
		return nil, fmt.Errorf("create trace2 temp dir: %w", err)
	}
	socketPath := filepath.Join(dir, "trace2.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("listen on trace2 socket: %w", err)
	}
	s := &Server{
		dir:         dir,
		listener:    listener,
		handler:     handler,
		acceptDone:  make(chan struct{}),
		connections: make(map[net.Conn]struct{}),
	}
	go s.accept()
	return s, nil
}

// Target returns a value suitable for the GIT_TRACE2_EVENT environment
// variable.
func (s *Server) Target() string {
	return trace2TargetPrefix + s.listener.Addr().String()
}

func (s *Server) accept() {
	defer close(s.acceptDone)
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.connections[conn] = struct{}{}
		s.connectionWG.Add(1)
		s.mu.Unlock()
		go s.process(conn)
	}
}

func (s *Server) process(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		s.mu.Lock()
		delete(s.connections, conn)
		s.mu.Unlock()
		s.connectionWG.Done()
	}()

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 64*1024), maxEventSize)
	for scanner.Scan() {
		var event Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			continue
		}
		s.handler(event)
	}
}

// Stop drains queued and active Trace2 streams, then removes the Unix socket.
// It is safe to call more than once.
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		// A Git command may exit after a short-lived child connects but before
		// the accept goroutine is scheduled. Keep accepting briefly so queued
		// child event streams are not dropped during shutdown.
		_ = s.listener.SetDeadline(time.Now().Add(acceptDrainPeriod))
		<-s.acceptDone
		_ = s.listener.Close()

		connectionsDone := make(chan struct{})
		go func() {
			s.connectionWG.Wait()
			close(connectionsDone)
		}()
		select {
		case <-connectionsDone:
		case <-time.After(connectionDrainTimeout):
			s.mu.Lock()
			for conn := range s.connections {
				_ = conn.Close()
			}
			s.mu.Unlock()
			<-connectionsDone
		}
		_ = os.RemoveAll(s.dir)
	})
}
