package filecache

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/cacheproxy"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/hashicorp/serf/serf"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/prototext"
	"github.com/prometheus/client_golang/prometheus"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

const (
	bloomFilterDigestTag     = "filecache.bloomfilter.digest.pbtxt"
	filecacheProxyListenAddr = "filecache.listenaddr"
)

func makeFileNode(r *rspb.ResourceName) *repb.FileNode {
	executable := r.GetInstanceName() == executableSuffix
	fileNode := &repb.FileNode{
		IsExecutable: executable,
		Digest:       r.GetDigest(),
	}
	return fileNode
}

func makeResourceName(node *repb.FileNode) *rspb.ResourceName {
	instanceName := ""
	if node.GetIsExecutable() {
		instanceName = executableSuffix
	}
	return digest.NewResourceName(node.GetDigest(), instanceName, rspb.CacheType_CAS, repb.DigestFunction_BLAKE3).ToProto()
}

type cacheAdapter struct {
	*fileCache
}

func (a *cacheAdapter) Contains(ctx context.Context, r *rspb.ResourceName) (bool, error) {
	return false, status.UnimplementedError("not-supported-by-filecache")
}
func (a *cacheAdapter) Metadata(ctx context.Context, r *rspb.ResourceName) (*interfaces.CacheMetadata, error) {
	return nil, status.UnimplementedError("not-supported-by-filecache")
}
func (a *cacheAdapter) FindMissing(ctx context.Context, resources []*rspb.ResourceName) ([]*repb.Digest, error) {
	return nil, status.UnimplementedError("not-supported-by-filecache")
}
func (a *cacheAdapter) Get(ctx context.Context, r *rspb.ResourceName) ([]byte, error) {
	return nil, status.UnimplementedError("not-supported-by-filecache")
}
func (a *cacheAdapter) GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	results := make(map[*repb.Digest][]byte, len(resources))
	for _, r := range resources {
		buf, err := a.Read(ctx, makeFileNode(r))
		if err != nil {
			return nil, err
		}
		results[r.GetDigest()] = buf
	}
	return results, nil
}
func (a *cacheAdapter) Set(ctx context.Context, r *rspb.ResourceName, data []byte) error {
	return status.UnimplementedError("not-supported-by-filecache")
}
func (a *cacheAdapter) SetMulti(ctx context.Context, kvs map[*rspb.ResourceName][]byte) error {
	return status.UnimplementedError("not-supported-by-filecache")
}
func (a *cacheAdapter) Delete(ctx context.Context, r *rspb.ResourceName) error {
	return status.UnimplementedError("not-supported-by-filecache")
}
func (a *cacheAdapter) Reader(ctx context.Context, r *rspb.ResourceName, uncompressedOffset, limit int64) (io.ReadCloser, error) {
	buf, err := a.Read(ctx, makeFileNode(r))
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(buf)), nil
}
func (a *cacheAdapter) Writer(ctx context.Context, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	return nil, status.UnimplementedError("not-supported-by-filecache")
}
func (a *cacheAdapter) SupportsCompressor(compressor repb.Compressor_Value) bool {
	return false
}
func (a *cacheAdapter) SupportsEncryption(ctx context.Context) bool {
	return false
}

type hashFilter struct {
	encodedDigest string
	filter        *bloom.BloomFilter
}

type fileCacheProxy struct {
	*fileCache
	env             environment.Env
	cacheProxy      *cacheproxy.CacheProxy
	listenAddr      string

	peerHashFilters map[string]*hashFilter
	lruSize     int64
	lruCount    int

	ctx context.Context
	eg  *errgroup.Group
}

func NewProxy(env environment.Env, fc *fileCache, listenAddr string) (*fileCacheProxy, error) {
	g := env.GetGossipService()
	if g == nil {
		return nil, status.FailedPreconditionError("Gossip must be enabled to use filecache proxy")
	}

	cacheProxy := cacheproxy.NewCacheProxy(env, &cacheAdapter{fc}, listenAddr)
	cacheProxy.StartListening()

	ctx, cancel := context.WithCancel(context.TODO())
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		cancel()
		return nil
	})

	eg, gCtx := errgroup.WithContext(ctx)
	fcp := &fileCacheProxy{
		fileCache:       fc,
		env:             env,
		cacheProxy:      cacheProxy,
		listenAddr:      listenAddr,
		peerHashFilters: make(map[string]*hashFilter),
		ctx:             gCtx,
		eg:              eg,
	}
	fcp.eg.Go(func() error {
		return fcp.announce(gCtx)
	})
	g.AddListener(fcp)
	g.SetTags(map[string]string{filecacheProxyListenAddr: listenAddr})
	return fcp, nil
}

const (
	// Refresh bloom filter if more than this many bytes were added.
	significantSizeChangeBytes = 100_000_000  // 100MB

	// Refresh bloom filter if more than this many items were added.
	significantItemCountChange = 1000
)
	
func (p *fileCacheProxy) significantChange() bool {
	lastLRUSize := p.lruSize
	lastLRUCount := p.lruCount

	p.fileCache.lock.Lock()
	p.lruSize = p.l.Size()
	p.lruCount = p.l.Len()
	p.fileCache.lock.Unlock()

	if p.lruSize - lastLRUSize > significantSizeChangeBytes {
		return true
	}
	if p.lruCount - lastLRUCount > significantItemCountChange {
		return true
	}
	return false
}

func (p *fileCacheProxy) serializedBitmap() ([]byte, error) {
	const filterLength = 1_000_000
	filter := bloom.NewWithEstimates(filterLength, 0.01)
	i := 0
	
	p.fileCache.lock.Lock()
	p.fileCache.l.Iter(func(value *entry) bool {
		if i >= filterLength {
			return false
		}
		fp := value.value
		key := p.fileCache.keyPath(fp)
		filter.AddString(key)
		return true
	})
	p.fileCache.lock.Unlock()
	
	bloomBuf := new(bytes.Buffer)
	if _, err := filter.WriteTo(bloomBuf); err != nil {
		return nil, err
	}
	return bloomBuf.Bytes(), nil
}

func (p *fileCacheProxy) announce(ctx context.Context) error {
	// If the LRU size has changed a lot since last scan.
	// If we're the LRU is full, and a set time period has passed.
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !p.significantChange() {
				continue
			}
			bm, err := p.serializedBitmap()
			if err != nil {
				log.Errorf("Error getting serialized bitmap: %s", err)
				continue
			}
			d, err := digest.Compute(bytes.NewReader(bm), repb.DigestFunction_BLAKE3)
			if err != nil {
				log.Errorf("Error computing digest: %s", err)
			}
			fn := &repb.FileNode{Digest: d}
			if _, err := p.writeFromReader(ctx, fn, bytes.NewReader(bm)); err != nil {
				log.Errorf("Error adding bloom filter buf to filecache: %s", err)
				continue
			}

			r := digest.NewResourceName(d, "filecacheProxy", rspb.CacheType_CAS, repb.DigestFunction_BLAKE3).ToProto()
			buf, err := prototext.Marshal(r)
			if err != nil {
				log.Errorf("error marshaling proto: %s", err)
				continue
			}
			p.env.GetGossipService().SetTags(map[string]string{
				bloomFilterDigestTag: string(buf),
			})
			log.Debugf("broadcast new bloom tag: %s", string(buf))
		case <-ctx.Done():
			return nil
		}
	}
}

func (p *fileCacheProxy) getPeerBloomFilter(peer string, resourceName *rspb.ResourceName) (*bloom.BloomFilter, error) {
	rc, err := p.cacheProxy.RemoteReader(p.ctx, peer, resourceName, 0, 0)
	if err != nil {
		return nil, err
	}
	g := new(bloom.BloomFilter)
	if _, err := g.ReadFrom(rc); err != nil {
		return nil, err
	}
	return g, nil
}

func (p *fileCacheProxy) OnEvent(eventType serf.EventType, event serf.Event) {
	if memberEvent, ok := event.(serf.MemberEvent); ok {
		for _, member := range memberEvent.Members {
			encBloomHash := member.Tags[bloomFilterDigestTag]
			memberAddr, ok := member.Tags[filecacheProxyListenAddr]
			if !ok || memberAddr == p.listenAddr {
				continue
			}
			switch member.Status {
			case serf.StatusAlive:
				if encBloomHash == "" {
					log.Debugf("Ignoring empty bloom digest")
					continue
				}
				existing, ok := p.peerHashFilters[memberAddr]
				if ok && existing.encodedDigest == encBloomHash {
					log.Debugf("Ignoring already-seen bloom digest")
					continue
				}
				rn := &rspb.ResourceName{}
				if err := prototext.Unmarshal([]byte(encBloomHash), rn); err != nil {
					log.Errorf("Error unmarshaling tag from peer %q: %s", memberAddr, err)
					continue
				}
				bloomFilter, err := p.getPeerBloomFilter(memberAddr, rn)
				if err != nil {
					log.Errorf("Error fetching bloom filter %q from peer %q.", encBloomHash, memberAddr)
					continue
				}
				p.peerHashFilters[memberAddr] = &hashFilter{
					encodedDigest: encBloomHash,
					filter:        bloomFilter,
				}
				log.Debugf("Updated bloom filter for member %q: hash: %s", memberAddr, encBloomHash)
			case serf.StatusLeaving, serf.StatusLeft, serf.StatusFailed, serf.StatusNone:
				log.Debugf("Removed bloom filter for unavailable member %q", memberAddr)
				delete(p.peerHashFilters, memberAddr)
			}
		}
	}
}

func (p *fileCacheProxy) FastLinkFile(ctx context.Context, node *repb.FileNode, outputPath string) (hit bool) {
	// If we have this file locally; just use it.
	if ok := p.fileCache.FastLinkFile(ctx, node, outputPath); ok {
		return ok
	}

	// If we don't have the file locally; check the bloom filters of our
	// peers. If any of them have it; fetch the file from that peer and try
	// fastlinking again.
	k := key(ctx, node)
	for memberAddr, hashFilter := range p.peerHashFilters {
		if hashFilter.filter.TestString(k) {
			resourceName := makeResourceName(node)
			rc, err := p.cacheProxy.RemoteReader(ctx, memberAddr, resourceName, 0, 0)
			if err == nil {
				p.fileCache.writeFromReader(ctx, node, rc)
				break
			}
		}
	}

	ok := p.fileCache.FastLinkFile(ctx, node, outputPath)
	label := missMetricLabel
	if ok {
		label = hitMetricLabel
		log.Errorf("BLOOM SHARING file %q!!!", k)
	}
	metrics.FileCacheProxyRequests.With(prometheus.Labels{metrics.FileCacheRequestStatusLabel: label}).Inc()
	return ok
}
