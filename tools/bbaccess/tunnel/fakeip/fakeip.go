// Package fakeip allocates local placeholder IPs for the DNS names the tunnel
// covers.
//
// The daemon answers DNS itself and hands out an address from a reserved range
// rather than the target's real IP. That address exists only inside the
// workstation: it is routed to the daemon's TUN, and when a connection arrives
// the daemon looks up which name it stood for and asks the gateway to dial that
// name.
//
// Allocation is a deterministic hash of the name, not a counter, so a given
// name gets the same address on every run. That matters because applications
// cache DNS answers far longer than the TTL we hand out — the JVM caches
// forever by default — and a restarted daemon would otherwise strand them on an
// address it no longer recognizes.
package fakeip

import (
	"fmt"
	"hash/fnv"
	"maps"
	"net/netip"
	"sync"
)

// Table maps fake IPs to the names they stand for.
type Table struct {
	prefix netip.Prefix
	// first and count describe the usable host range within prefix.
	first uint32
	count uint32

	mu     sync.RWMutex
	byName map[string]netip.Addr
	byAddr map[netip.Addr]string
}

// NewTable creates a Table allocating from prefix, which must be IPv4.
//
// IPv4 is deliberate: a workstation on a network without IPv6 will not route
// v6 fake addresses, and getaddrinfo ordering makes a mixed answer a coin flip.
func NewTable(prefix netip.Prefix) (*Table, error) {
	if !prefix.Addr().Is4() {
		return nil, fmt.Errorf("fakeip: prefix %s must be IPv4", prefix)
	}
	if prefix.Bits() > 30 {
		return nil, fmt.Errorf("fakeip: prefix %s is too small", prefix)
	}
	// A /0 would overflow the size calculation below, and claiming the entire
	// address space is never what anyone meant.
	if prefix.Bits() < 8 {
		return nil, fmt.Errorf("fakeip: prefix %s is too large; use a specific reserved range such as 198.18.0.0/16", prefix)
	}
	base := addrToUint32(prefix.Masked().Addr())
	size := uint32(1) << (32 - prefix.Bits())
	// Skip the network address, the address the TUN itself holds (.0.1) and,
	// on macOS, its point-to-point peer (.0.2); leave the broadcast address
	// alone.
	const reservedAtStart = 3
	return &Table{
		prefix: prefix.Masked(),
		first:  base + reservedAtStart,
		count:  size - reservedAtStart - 1,
		byName: make(map[string]netip.Addr),
		byAddr: make(map[netip.Addr]string),
	}, nil
}

// Prefix returns the range this table allocates from.
func (t *Table) Prefix() netip.Prefix { return t.prefix }

// Lookup returns the fake IP for name, allocating one if needed. The returned
// address is stable for a given name and table prefix.
func (t *Table) Lookup(name string) (netip.Addr, error) {
	t.mu.RLock()
	addr, ok := t.byName[name]
	t.mu.RUnlock()
	if ok {
		return addr, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if addr, ok := t.byName[name]; ok {
		return addr, nil
	}
	if uint32(len(t.byAddr)) >= t.count {
		return netip.Addr{}, fmt.Errorf("fakeip: %s is exhausted (%d names)", t.prefix, len(t.byAddr))
	}

	h := fnv.New32a()
	h.Write([]byte(name))
	start := h.Sum32() % t.count
	// Linear probing keeps collisions deterministic too: the same set of names
	// always lands on the same addresses, whatever order they are looked up in
	// — as long as the colliding name was inserted first, which is the usual
	// case for a stable name set.
	for i := uint32(0); i < t.count; i++ {
		candidate := uint32ToAddr(t.first + (start+i)%t.count)
		if _, taken := t.byAddr[candidate]; taken {
			continue
		}
		t.byName[name] = candidate
		t.byAddr[candidate] = name
		return candidate, nil
	}
	return netip.Addr{}, fmt.Errorf("fakeip: %s is exhausted", t.prefix)
}

// Name returns the name a fake IP stands for.
func (t *Table) Name(addr netip.Addr) (string, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	name, ok := t.byAddr[addr.Unmap()]
	return name, ok
}

// Contains reports whether addr is in the fake range.
func (t *Table) Contains(addr netip.Addr) bool { return t.prefix.Contains(addr.Unmap()) }

// Len returns the number of allocated names.
func (t *Table) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.byName)
}

// Entries returns a copy of the current allocations, for `bbaccess tunnel status`.
func (t *Table) Entries() map[string]netip.Addr {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return maps.Clone(t.byName)
}

func addrToUint32(a netip.Addr) uint32 {
	b := a.As4()
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func uint32ToAddr(v uint32) netip.Addr {
	return netip.AddrFrom4([4]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)})
}
