package llmproxy

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"sync/atomic"
)

// SessionRegistrar registers the trusted network identity assigned to an
// execution. The returned function revokes the registration and is safe to
// call more than once.
type SessionRegistrar interface {
	RegisterSession(sourceIP string, session *Session) (revoke func(), err error)
}

type registeredSession struct {
	id      uint64
	session Session
}

// SessionRegistry resolves proxy requests using the source IP assigned by the
// executor's outer network namespace. Firecracker guests cannot select or
// spoof this address: guest traffic is SNATed to it before reaching the
// executor network.
type SessionRegistry struct {
	nextID atomic.Uint64

	mu       sync.RWMutex
	sessions map[netip.Addr]registeredSession
}

func NewSessionRegistry() *SessionRegistry {
	return &SessionRegistry{
		sessions: make(map[netip.Addr]registeredSession),
	}
}

func (r *SessionRegistry) RegisterSession(sourceIP string, session *Session) (func(), error) {
	if session == nil {
		return nil, errors.New("session is required")
	}
	addr, err := netip.ParseAddr(sourceIP)
	if err != nil {
		return nil, errors.New("invalid session source IP")
	}
	addr = addr.Unmap()
	id := r.nextID.Add(1)
	entry := registeredSession{
		id:      id,
		session: cloneSession(session),
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.sessions[addr]; ok {
		return nil, errors.New("source IP already has an active session")
	}
	r.sessions[addr] = entry

	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			current, ok := r.sessions[addr]
			if ok && current.id == id {
				delete(r.sessions, addr)
			}
		})
	}, nil
}

func (r *SessionRegistry) ResolveSession(ctx context.Context, req *http.Request) (*Session, error) {
	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		return nil, errors.New("invalid proxy client address")
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return nil, errors.New("invalid proxy client IP")
	}
	addr = addr.Unmap()

	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.sessions[addr]
	if !ok {
		return nil, errors.New("proxy client has no active execution")
	}
	session := cloneSession(&entry.session)
	return &session, nil
}

func cloneSession(session *Session) Session {
	clone := *session
	clone.RedactionValues = append([]string(nil), session.RedactionValues...)
	clone.NamedRedactionValues = append([]NamedRedactionValue(nil), session.NamedRedactionValues...)
	return clone
}
