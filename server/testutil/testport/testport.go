package testport

import (
	"net"
	"sync"
	"testing"
)

var (
	portLeaser freePortLeaser
)

func FindFree(t testing.TB) int {
	return portLeaser.Lease(t)
}

// ReleaseAllHeldPorts closes all held port listeners so that the ports can be
// bound by subprocesses (e.g. test servers). This should be called just before
// starting the subprocess that will bind to the allocated ports.
func ReleaseAllHeldPorts() {
	portLeaser.ReleaseAll()
}

type freePortLeaser struct {
	leasedPorts map[int]struct{}
	heldPorts   []net.Listener
	mu          sync.Mutex
}

func (p *freePortLeaser) findAPort(t testing.TB) (int, net.Listener) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	return l.Addr().(*net.TCPAddr).Port, l
}

func (p *freePortLeaser) Lease(t testing.TB) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.leasedPorts == nil {
		p.leasedPorts = make(map[int]struct{}, 0)
	}
	for {
		port, listener := p.findAPort(t)
		if _, ok := p.leasedPorts[port]; !ok {
			p.leasedPorts[port] = struct{}{}
			p.heldPorts = append(p.heldPorts, listener)
			return port
		}
		listener.Close()
	}
}

func (p *freePortLeaser) ReleaseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, l := range p.heldPorts {
		l.Close()
	}
	p.heldPorts = nil
}
