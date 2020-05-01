package memory_cache

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

// lru implements a non-thread safe fixed size LRU cache
type lru struct {
	maxSizeBytes int64
	sizeBytes    int64
	evictList    *list.List
	items        map[string]*list.Element
}

// entry is used to hold a value in the evictList
type entry struct {
	key   string
	value []byte
}

// NewLRU constructs an lru with maxSize
func newlru(maxSizeBytes int64) (*lru, error) {
	if maxSizeBytes <= 0 {
		return nil, errors.New("Must provide a positive size")
	}
	c := &lru{
		sizeBytes:    0,
		maxSizeBytes: maxSizeBytes,
		evictList:    list.New(),
		items:        make(map[string]*list.Element),
	}
	return c, nil
}

// Purge is used to completely clear the cache.
func (c *lru) Purge() {
	for k, _ := range c.items {
		delete(c.items, k)
	}
	c.evictList.Init()
}

// Add adds a value to the cache.  Returns true if an eviction occurred.
func (c *lru) Add(key string, value []byte) (evicted bool) {
	// Check for existing item. If it's there, re-up it and
	// overwrite the value.
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*entry).value = value
		return false
	}

	c.addElement(&entry{key, value})

	evicted = false
	for {
		evict := c.sizeBytes > c.maxSizeBytes
		if !evict {
			break
		}
		evicted = true
		c.removeOldest()
	}
	return evicted
}

// Get looks up a key's value from the cache.
func (c *lru) Get(key string) (value []byte, ok bool) {
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		if ent.Value.(*entry) == nil {
			return nil, false
		}
		return ent.Value.(*entry).value, true
	}
	return
}

// Contains checks if a key is in the cache, without updating the recent-ness
// or deleting it for being stale.
func (c *lru) Contains(key string) (ok bool) {
	_, ok = c.items[key]
	return ok
}

// Remove removes the provided key from the cache, returning if the
// key was contained.
func (c *lru) Remove(key string) (present bool) {
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
		return true
	}
	return false
}

// RemoveOldest removes the oldest item from the cache.
func (c *lru) RemoveOldest() (key string, value []byte, ok bool) {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
		kv := ent.Value.(*entry)
		return kv.key, kv.value, true
	}
	return "", nil, false
}

// Len returns the number of items in the cache.
func (c *lru) Len() int {
	return c.evictList.Len()
}

// Len returns the number of items in the cache.
func (c *lru) Size() int64 {
	return c.sizeBytes
}

// removeOldest removes the oldest item from the cache.
func (c *lru) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

func sizeOfEntry(e *entry) int64 {
	return int64(len([]byte(e.key)) + len(e.value))
}

// removeElement is used to remove a given list element from the cache
func (c *lru) addElement(el *entry) {
	// Add new item
	ent := c.evictList.PushFront(el)
	c.items[el.key] = ent
	c.sizeBytes += sizeOfEntry(el)
}

// removeElement is used to remove a given list element from the cache
func (c *lru) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*entry)
	delete(c.items, kv.key)
	c.sizeBytes -= sizeOfEntry(kv)
}

type MemoryCache struct {
	l    *lru
	lock sync.RWMutex
}

func NewMemoryCache(maxSizeBytes int64) (*MemoryCache, error) {
	lru, err := newlru(maxSizeBytes)
	if err != nil {
		return nil, err
	}
	return &MemoryCache{
		l: lru,
	}, nil
}

func (m *MemoryCache) PrefixKey(ctx context.Context, key string) (string, error) {
	return key, nil
}

// Normal cache-like operations.
func (m *MemoryCache) Contains(ctx context.Context, key string) (bool, error) {
	fullKey, err := m.PrefixKey(ctx, key)
	if err != nil {
		return false, err
	}
	m.lock.Lock()
	contains := m.l.Contains(fullKey)
	m.lock.Unlock()
	return contains, nil
}

func (m *MemoryCache) Get(ctx context.Context, key string) ([]byte, error) {
	fullKey, err := m.PrefixKey(ctx, key)
	if err != nil {
		return nil, err
	}
	m.lock.Lock()
	value, ok := m.l.Get(fullKey)
	m.lock.Unlock()
	if !ok {
		return nil, status.NotFoundError(fmt.Sprintf("Key %s not found", fullKey))
	}
	return value, nil
}

func (m *MemoryCache) Set(ctx context.Context, key string, data []byte) error {
	fullKey, err := m.PrefixKey(ctx, key)
	if err != nil {
		return err
	}
	m.lock.Lock()
	m.l.Add(fullKey, data)
	m.lock.Unlock()
	return nil
}

func (m *MemoryCache) Delete(ctx context.Context, key string) error {
	fullKey, err := m.PrefixKey(ctx, key)
	if err != nil {
		return err
	}
	m.lock.Lock()
	m.l.Remove(fullKey)
	m.lock.Unlock()
	return nil
}

// Low level interface used for seeking and stream-writing.
func (m *MemoryCache) Reader(ctx context.Context, key string, offset, length int64) (io.Reader, error) {
	// Locking and key prefixing are handled in Get.
	buf, err := m.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(buf)
	r.Seek(offset, 0)
	if length > 0 {
		return io.LimitReader(r, length), nil
	}
	return r, nil
}

type closeFn func(b *bytes.Buffer) error
type setOnClose struct {
	*bytes.Buffer
	c closeFn
}

func (d *setOnClose) Close() error {
	return d.c(d.Buffer)
}

func (m *MemoryCache) Writer(ctx context.Context, key string) (io.WriteCloser, error) {
	var buffer bytes.Buffer
	return &setOnClose{
		Buffer: &buffer,
		c: func(b *bytes.Buffer) error {
			// Locking and key prefixing are handled in Put.
			return m.Set(ctx, key, b.Bytes())
		},
	}, nil
}

func (m *MemoryCache) Start() error {
	return nil
}

func (m *MemoryCache) Stop() error {
	return nil
}
