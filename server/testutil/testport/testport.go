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

func Listen(t testing.TB) (net.Listener, int) {
	return findAPort(t)
}

type freePortLeaser struct {
	leasedPorts map[int]struct{}
	mu          sync.Mutex
}

func findAPort(t testing.TB) (net.Listener, int) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	return l, l.Addr().(*net.TCPAddr).Port
}

func (p *freePortLeaser) Lease(t testing.TB) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.leasedPorts == nil {
		p.leasedPorts = make(map[int]struct{}, 0)
	}
	for {
		l, port := findAPort(t)
		l.Close()
		if _, ok := p.leasedPorts[port]; !ok {
			p.leasedPorts[port] = struct{}{}
			return port
		}
	}
}
