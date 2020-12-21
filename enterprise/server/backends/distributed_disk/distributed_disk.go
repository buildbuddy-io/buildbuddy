package distributed_disk

import (
	"container/list"
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pubsub"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/diskproxy"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/consistent_hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	cacheEvictionCutoffPercentage = .8

	// This node will send heartbeats every this often.
	heartbeatPeriod = 1 * time.Second

	// After this timeout, a node will be removed from the set of active
	// nodes.
	heartbeatTimeout = 3 * heartbeatPeriod

	// How often this node will check if heartbeats are still valid.
	heartbeatCheckPeriod = 100 * time.Millisecond
)

type PeersUpdateFn func(peerSet ...string)
type HostAnnounceChannel struct {
	groupName string
	inetAddr  string
	peers     map[string]time.Time
	ps        interfaces.PubSub
	updateFn  PeersUpdateFn
}

func NewHostAnnounceChannel(redisServer, inetAddr, groupName string, updateFn PeersUpdateFn) *HostAnnounceChannel {
	hac := &HostAnnounceChannel{
		groupName: groupName,
		inetAddr:  inetAddr,
		peers:     make(map[string]time.Time, 0),
		ps:        pubsub.NewPubSub(redisServer),
		updateFn:  updateFn,
	}
	ctx := context.Background()
	go hac.sendHeartbeat(ctx)
	go hac.watchPeers(ctx)
	return hac
}
func (c *HostAnnounceChannel) sendHeartbeat(ctx context.Context) {
	for {
		err := c.ps.Publish(ctx, c.groupName, c.inetAddr)
		if err != nil {
			log.Printf("Error publishing: %s", err.Error())
		}
		time.Sleep(heartbeatPeriod)
	}
}
func (c *HostAnnounceChannel) notifySetChanged() {
	nodes := make([]string, 0, len(c.peers))
	for peer, _ := range c.peers {
		nodes = append(nodes, peer)
	}
	sort.Strings(nodes)
	log.Printf("Set of active ddisk nodes changed: %s", nodes)
	c.updateFn(nodes...)
}

func (c *HostAnnounceChannel) watchPeers(ctx context.Context) {
	subscriber := c.ps.Subscribe(ctx, c.groupName)
	defer subscriber.Close()
	pubsubChan := subscriber.Chan()
	for {
		select {
		case peer := <-pubsubChan:
			_, ok := c.peers[peer]
			c.peers[peer] = time.Now()
			if !ok {
				c.notifySetChanged()
			}
		case <-time.After(heartbeatCheckPeriod):
			updated := false
			for peer, lastBeat := range c.peers {
				if time.Since(lastBeat) > heartbeatTimeout {
					delete(c.peers, peer)
					updated = true
				}
			}
			if updated {
				c.notifySetChanged()
			}
		}
	}
}

// Blobstore is *almost* sufficient here, but blobstore doesn't handle record
// expirations in a way that make sense for a cache. So we keep a record (in
// memory) of file atime (Last Access Time) and size, and then when our cache
// reaches fullness we remove the most stale files. Rather than serialize this
// ledger, we regenerate it from scratch on startup by looking at the
// filesystem itself.
type DistributedDiskCache struct {
	rootDir      string
	maxSizeBytes int64

	lock      *sync.RWMutex // PROTECTS: evictList, entries, sizeBytes
	evictList *list.List
	entries   map[string]*list.Element
	sizeBytes *int64
	prefix    string

	diskProxy           *diskproxy.DiskProxy
	consistentHash      *consistent_hash.ConsistentHash
	listenAddr          string
	hostAnnounceChannel *HostAnnounceChannel
}

type fileRecord struct {
	key       string
	sizeBytes int64
	lastUse   time.Time
}

func getLastUse(info os.FileInfo) time.Time {
	stat := info.Sys().(*syscall.Stat_t)
	// Super Gross! https://github.com/golang/go/issues/31735
	value := reflect.ValueOf(stat)
	var ts syscall.Timespec
	if timeField := value.Elem().FieldByName("Atimespec"); timeField.IsValid() {
		ts = timeField.Interface().(syscall.Timespec)
	} else if timeField := value.Elem().FieldByName("Atim"); timeField.IsValid() {
		ts = timeField.Interface().(syscall.Timespec)
	} else {
		ts = syscall.Timespec{}
	}
	return time.Unix(ts.Sec, ts.Nsec)
}

func makeRecord(fullPath string, info os.FileInfo) *fileRecord {
	return &fileRecord{
		key:       fullPath,
		sizeBytes: info.Size(),
		lastUse:   getLastUse(info),
	}
}

func NewDistributedDiskCache(listenAddr, rootDir, redisTarget, groupName string, maxSizeBytes int64) (*DistributedDiskCache, error) {
	zeroSize := int64(0)
	if groupName == "" {
		groupName = "default"
	}
	c := &DistributedDiskCache{
		rootDir:        rootDir,
		maxSizeBytes:   maxSizeBytes,
		sizeBytes:      &zeroSize,
		lock:           &sync.RWMutex{},
		consistentHash: consistent_hash.NewConsistentHash(),
		listenAddr:     listenAddr,
	}
	c.hostAnnounceChannel = NewHostAnnounceChannel(redisTarget, listenAddr, groupName, c.consistentHash.Set)
	if err := c.initializeCache(); err != nil {
		return nil, err
	}

	// Handles reads / writes to the local filesystem from peer cache nodes.
	c.diskProxy = diskproxy.NewDiskProxy(listenAddr, rootDir, c.remoteWriteFn, c.readNotify)
	return c, nil
}

func (c *DistributedDiskCache) readNotify(k string) {
	c.useEntry(k)
}

func (c *DistributedDiskCache) WithPrefix(prefix string) interfaces.Cache {
	newPrefix := filepath.Join(append(filepath.SplitList(c.prefix), prefix)...)
	if len(newPrefix) > 0 && newPrefix[len(newPrefix)-1] != '/' {
		newPrefix += "/"
	}

	return &DistributedDiskCache{
		rootDir:      c.rootDir,
		maxSizeBytes: c.maxSizeBytes,
		lock:         c.lock,
		evictList:    c.evictList,
		entries:      c.entries,
		sizeBytes:    c.sizeBytes,
		prefix:       newPrefix,

		diskProxy:           c.diskProxy,
		consistentHash:      c.consistentHash,
		listenAddr:          c.listenAddr,
		hostAnnounceChannel: c.hostAnnounceChannel,
	}
}

func (c *DistributedDiskCache) initializeCache() error {
	if err := disk.EnsureDirectoryExists(c.rootDir); err != nil {
		return err
	}
	records := make([]*fileRecord, 0)
	walkFn := func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			records = append(records, makeRecord(path, info))
		}
		return nil
	}
	if err := filepath.Walk(c.rootDir, walkFn); err != nil {
		return err
	}

	// Sort entries by ascending ATime.
	sort.Slice(records, func(i, j int) bool { return records[i].lastUse.Before(records[j].lastUse) })

	// Populate our state tracking datastructures.
	c.evictList = list.New()
	c.entries = make(map[string]*list.Element, len(records))
	for _, record := range records {
		c.addEntry(record)
	}
	log.Printf("Initialized distributed disk cache. Current size: %d (max: %d) bytes", *c.sizeBytes, c.maxSizeBytes)
	return nil
}

func (c *DistributedDiskCache) addEntry(record *fileRecord) {
	c.lock.Lock()
	listElement := c.evictList.PushFront(record)
	c.entries[record.key] = listElement
	*c.sizeBytes += record.sizeBytes
	c.lock.Unlock()
}

func (c *DistributedDiskCache) removeEntry(key string) {
	c.lock.Lock()
	if listElement, ok := c.entries[key]; ok {
		c.evictList.Remove(listElement)
		record := listElement.Value.(*fileRecord)
		*c.sizeBytes -= record.sizeBytes
		delete(c.entries, key)
	}
	c.lock.Unlock()
}

func (c *DistributedDiskCache) useEntry(key string) bool {
	c.lock.Lock()
	listElement, ok := c.entries[key]
	if ok {
		c.evictList.MoveToFront(listElement)
		listElement.Value.(*fileRecord).lastUse = time.Now()
	}
	c.lock.Unlock()
	return ok
}

func (c *DistributedDiskCache) checkSizeAndEvict(ctx context.Context, n int64) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	newSize := *c.sizeBytes + n
	stopDeleteCutoff := int64(cacheEvictionCutoffPercentage * float64(c.maxSizeBytes))

	if newSize > c.maxSizeBytes {
		log.Printf("Cache is currently %d bytes, going to remove items until we're below %d bytes.", *c.sizeBytes, stopDeleteCutoff)
		for *c.sizeBytes+n > stopDeleteCutoff {
			listElement := c.evictList.Back()
			if listElement == nil {
				break
			}
			c.evictList.Remove(listElement)
			record := listElement.Value.(*fileRecord)
			*c.sizeBytes -= record.sizeBytes
			delete(c.entries, record.key)
			if err := disk.DeleteFile(ctx, record.key); err != nil {
				log.Printf("Error deleting file %q (already deleted?)", record.key)
			}
		}
		log.Printf("Cache size is now %d bytes, unlocking.", *c.sizeBytes)
	}

	return nil
}

func (c *DistributedDiskCache) key(ctx context.Context, d *repb.Digest) (string, string, error) {
	hash, err := digest.Validate(d)
	if err != nil {
		return "", "", err
	}
	userPrefix, err := prefix.UserPrefixFromContext(ctx)
	if err != nil {
		return "", "", err
	}

	k := filepath.Join(c.rootDir, userPrefix+c.prefix+hash)
	peer := c.consistentHash.Get(k)
	if peer == "" {
		// if no peer is found, default to local writes.
		peer = c.listenAddr
	}
	return k, peer, nil
}

func (c *DistributedDiskCache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	k, peer, err := c.key(ctx, d)
	if err != nil {
		return false, err
	}
	if peer != c.listenAddr {
		return c.diskProxy.RemoteContains(ctx, peer, k)
	}
	return c.useEntry(k), nil
}

func (c *DistributedDiskCache) ContainsMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest]bool, error) {
	foundMap := make(map[*repb.Digest]bool, len(digests))
	// No parallelism here -- we don't know enough about what kind of io
	// characteristics our disk has anyway. And disk is usually used for
	// on-prem / small instances where this doesn't matter as much.
	for _, d := range digests {
		ok, err := c.Contains(ctx, d)
		if err != nil {
			return nil, err
		}
		foundMap[d] = ok
	}
	return foundMap, nil
}

func (c *DistributedDiskCache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	k, peer, err := c.key(ctx, d)
	if err != nil {
		return nil, err
	}
	if peer != c.listenAddr {
		return c.diskProxy.RemoteGet(ctx, peer, k)
	}
	c.useEntry(k)
	f, err := disk.ReadFile(ctx, k)
	if err != nil {
		c.removeEntry(k)
		return nil, status.NotFoundErrorf("DistributedDiskCache missing file: %s", err)
	}
	return f, nil
}

func (c *DistributedDiskCache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	foundMap := make(map[*repb.Digest][]byte, len(digests))
	// No parallelism here either. Not necessary for an in-memory cache.
	for _, d := range digests {
		data, err := c.Get(ctx, d)
		if err != nil {
			return nil, err
		}
		foundMap[d] = data
	}
	return foundMap, nil
}

func (c *DistributedDiskCache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	k, peer, err := c.key(ctx, d)
	if err != nil {
		return err
	}
	if peer != c.listenAddr {
		return c.diskProxy.RemoteSet(ctx, peer, k, data)
	}

	if err := c.checkSizeAndEvict(ctx, int64(len(data))); err != nil {
		return err
	}
	n, err := disk.WriteFile(ctx, k, data)
	if err != nil {
		// If we had an error writing the file, just return that.
		return err
	}
	c.addEntry(&fileRecord{
		key:       k,
		sizeBytes: int64(n),
		lastUse:   time.Now(),
	})
	return err
}

func (c *DistributedDiskCache) SetMulti(ctx context.Context, kvs map[*repb.Digest][]byte) error {
	for d, data := range kvs {
		if err := c.Set(ctx, d, data); err != nil {
			return err
		}
	}
	return nil
}

func (c *DistributedDiskCache) Delete(ctx context.Context, d *repb.Digest) error {
	k, _, err := c.key(ctx, d)
	if err != nil {
		return err
	}
	c.removeEntry(k)
	return disk.DeleteFile(ctx, k)
}

func (c *DistributedDiskCache) Reader(ctx context.Context, d *repb.Digest, offset int64) (io.Reader, error) {
	k, peer, err := c.key(ctx, d)
	if err != nil {
		return nil, err
	}
	if peer != c.listenAddr {
		return c.diskProxy.RemoteReader(ctx, peer, k)
	}
	c.useEntry(k)
	length := d.GetSizeBytes()
	r, err := disk.FileReader(ctx, k, offset, length)
	if err != nil {
		c.removeEntry(k)
		return nil, status.NotFoundErrorf("DistributedDiskCache missing file: %s", err)
	}
	return r, nil
}

type dbCloseFn func(totalBytesWritten int64) error
type checkOversizeFn func(n int) error
type dbWriteOnClose struct {
	io.WriteCloser
	checkFn      checkOversizeFn
	closeFn      dbCloseFn
	bytesWritten int64
}

func (d *dbWriteOnClose) Write(data []byte) (int, error) {
	if err := d.checkFn(len(data)); err != nil {
		return 0, err
	}
	n, err := d.WriteCloser.Write(data)
	d.bytesWritten += int64(n)
	return n, err
}

func (d *dbWriteOnClose) Close() error {
	if err := d.WriteCloser.Close(); err != nil {
		return err
	}
	return d.closeFn(d.bytesWritten)
}

func (c *DistributedDiskCache) remoteWriteFn(ctx context.Context, k string) (io.WriteCloser, error) {
	writeCloser, err := disk.FileWriter(ctx, k)
	if err != nil {
		return nil, err
	}
	return &dbWriteOnClose{
		WriteCloser: writeCloser,
		checkFn:     func(n int) error { return c.checkSizeAndEvict(ctx, int64(n)) },
		closeFn: func(totalBytesWritten int64) error {
			if err := c.checkSizeAndEvict(ctx, totalBytesWritten); err != nil {
				return err
			}
			c.addEntry(&fileRecord{
				key:       k,
				sizeBytes: totalBytesWritten,
				lastUse:   time.Now(),
			})
			return nil
		},
	}, nil
}

func (c *DistributedDiskCache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	k, peer, err := c.key(ctx, d)
	if err != nil {
		return nil, err
	}
	if peer != c.listenAddr {
		return c.diskProxy.RemoteWriter(ctx, peer, k, d.GetSizeBytes())
	}

	return c.remoteWriteFn(ctx, k)
}
