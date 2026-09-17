// Package summaries maintains in-memory summaries of resources, which search
// runs over.
//
// Ingesters parse external resources and feed them into a Store.
package summaries

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ResourceType identifies a resource type within one cluster.
type ResourceType struct {
	Cluster    string
	Group      string
	Version    string
	Resource   string // plural name, as used in API paths
	Kind       string
	Namespaced bool
}

func (r ResourceType) String() string {
	gv := r.Version
	if r.Group != "" {
		gv = r.Group + "/" + r.Version
	}
	return fmt.Sprintf("%s %s (%s)", r.Cluster, r.Resource, gv)
}

// Port is a named port on a pod or service.
type Port struct {
	Name     string
	Port     int32
	Protocol string // empty means TCP
	NodePort int32
}

// Entry is the indexed summary of a single object. Index updates create new
// Entry objects so it's safe to hold on to an Entry object.
type Entry struct {
	Cluster   string
	Group     string
	Version   string
	Resource  string
	Kind      string
	Namespace string
	Name      string
	UID       string

	Created time.Time
	Labels  map[string]string

	// Owner is the controlling owner as "Kind/name", e.g. "ReplicaSet/app-7d9f".
	Owner string

	// Phase is a short display status.
	Phase string
	// Ready is "ready/desired" for workloads and pods.
	Ready    string
	Restarts int64

	Node       string
	IPs        []string
	Images     []string
	Containers []string
	Ports      []Port
	Selector   map[string]string
	// Extra holds kind-specific display pairs (schedule, kubelet version, ...).
	Extra map[string]string

	// blob is the lowercase haystack search terms match against.
	blob string
}

// Key returns the entry's identity within its resource type.
func (e *Entry) Key() string {
	if e.Namespace == "" {
		return e.Name
	}
	return e.Namespace + "/" + e.Name
}

// SetExtra records a kind-specific display pair.
func (e *Entry) SetExtra(k, v string) {
	if e.Extra == nil {
		e.Extra = map[string]string{}
	}
	e.Extra[k] = v
}

func buildBlob(e *Entry) string {
	var b strings.Builder
	add := func(parts ...string) {
		for _, p := range parts {
			if p != "" {
				b.WriteString(strings.ToLower(p))
				b.WriteByte('\n')
			}
		}
	}
	add(e.Name, e.Namespace, e.Kind, e.Resource, e.Cluster, e.Owner, e.Phase, e.Node, e.UID)
	add(e.IPs...)
	add(e.Images...)
	add(e.Containers...)
	for k, v := range e.Labels {
		add(k + "=" + v)
	}
	for _, v := range e.Extra {
		add(v)
	}
	for _, p := range e.Ports {
		add(p.Name, strconv.Itoa(int(p.Port)))
	}
	return b.String()
}

// Store holds the entries of one resource type from one source.
type Store struct {
	res ResourceType

	mu      sync.RWMutex
	entries map[string]*Entry
	synced  bool
}

func (s *Store) ResourceType() ResourceType { return s.res }

func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

// Put adds or replaces an entry. The store takes ownership of e.
func (s *Store) Put(e *Entry) {
	e.blob = buildBlob(e)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[e.Key()] = e
}

// Delete removes the entry with the given key, if present.
func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, key)
}

// Replace substitutes the full contents, as an ingester does after listing
// its source in full, and marks the store synced.
func (s *Store) Replace(entries []*Entry) {
	m := make(map[string]*Entry, len(entries))
	for _, e := range entries {
		e.blob = buildBlob(e)
		m[e.Key()] = e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = m
	s.synced = true
}

// Synced reports whether the store has received its initial full contents.
func (s *Store) Synced() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.synced
}

// Get returns the entry with the given key.
func (s *Store) Get(key string) (*Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[key]
	return e, ok
}

// Entries returns the current entries, in no particular order.
func (s *Store) Entries() []*Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Entry, 0, len(s.entries))
	for _, e := range s.entries {
		out = append(out, e)
	}
	return out
}

// scan calls fn for every entry while holding the read lock.
func (s *Store) scan(fn func(*Entry)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, e := range s.entries {
		fn(e)
	}
}

// Index is the searchable registry of all stores across all clusters.
type Index struct {
	mu     sync.RWMutex
	stores []*Store
}

func New() *Index { return &Index{} }

// NewStore registers and returns the store for one resource type.
func (ix *Index) NewStore(res ResourceType) *Store {
	s := &Store{res: res, entries: map[string]*Entry{}}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.stores = append(ix.stores, s)
	return s
}

func (ix *Index) snapshot() []*Store {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return append([]*Store(nil), ix.stores...)
}

// Scan calls fn for every entry of the given kind (all kinds if empty),
// restricted to cluster if non-empty.
func (ix *Index) Scan(cluster, kind string, fn func(*Entry)) {
	for _, s := range ix.snapshot() {
		if cluster != "" && s.res.Cluster != cluster {
			continue
		}
		if kind != "" && s.res.Kind != kind {
			continue
		}
		s.scan(fn)
	}
}

// GetEntry looks up a single entry by identity.
func (ix *Index) GetEntry(cluster, resource, namespace, name string) (*Entry, bool) {
	for _, s := range ix.snapshot() {
		if s.res.Cluster != cluster || s.res.Resource != resource {
			continue
		}
		key := name
		if namespace != "" {
			key = namespace + "/" + name
		}
		if e, ok := s.Get(key); ok {
			return e, true
		}
	}
	return nil, false
}

// SelectorMatches reports whether every selector pair is present in labels.
// An empty selector matches nothing, mirroring how services treat it.
func SelectorMatches(selector, labels map[string]string) bool {
	if len(selector) == 0 {
		return false
	}
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}
