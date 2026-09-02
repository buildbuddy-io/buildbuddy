package distributed_client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/config"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/findmissing"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/kuberesolver"
	"github.com/buildbuddy-io/buildbuddy/server/util/lib/set"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/rpcutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"

	dcpb "github.com/buildbuddy-io/buildbuddy/proto/distributed_cache"
	refpb "github.com/buildbuddy-io/buildbuddy/proto/reference"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

const (
	// writeBufSizeBytes controls the maximum size of buffers used for writing
	// to a remote cache. This is also the maximum payload size for each
	// WriteRequest, though with ioutil.DoubleBufferWriter, payloads will be
	// smaller unless the remote cache is falling behind. Experiments and
	// benchmarks show that 128KB, 256KB, and 512KB are all about as fast.
	// Values outside that range cause more allocation in gRPC code. This
	// should be slightly smaller than 2^N, to allow for proto and gRPC
	// overhead.
	writeBufSizeBytes = 512 * 1000 // 512 KB

	// Reference verification outcomes.
	VerificationSuccess = "success"
	VerificationFailure = "failure"
	VerificationError   = "error"

	// How long an async write-reference verification may keep running after
	// the write stream that spawned it completes.
	referenceVerificationTimeout = 1 * time.Minute

	// maxDecompressBufSizeBytes caps the initial buffer allocated for a
	// decompressed peer response, since digest sizes are client-supplied.
	// The buffer grows as needed.
	maxDecompressBufSizeBytes = 4 * 1024 * 1024
)

var (
	enableKubeResolver = flag.Bool("cache.distributed_cache.enable_kube_resolver", false, "Enable Kubernetes resolver for resolving peer pod IPs")
	peerWriteTimeout   = flag.Duration("cache.distributed_cache.peer_write_timeout", time.Minute, "Maximum time to wait for a single distributed cache peer write send or close operation before treating the peer as stalled.")
	connWindowSize     = flag.Int("cache.distributed_cache.conn_window_size", 64*1024*1024, "Static HTTP/2 window size of each connection in bytes")
	streamWindowSize   = flag.Int("cache.distributed_cache.stream_window_size", 8*1024*1024, "Static HTTP/2 Window size of each stream in bytes")
	poolSize           = flag.Int("cache.distributed_cache.client_pool_size", 4, "Number of connections to open per peer.")
)

type Proxy struct {
	env                   environment.Env
	cache                 interfaces.Cache
	log                   log.Logger
	readRefLogger         log.Logger
	writeRefLogger        log.Logger
	bufPool               *bytebufferpool.VariableSizePool
	mu                    *sync.Mutex
	server                *grpc.Server
	clients               map[string]*grpc_client.ClientConnPool
	heartbeatCallback     func(ctx context.Context, peer string)
	hintedHandoffCallback func(ctx context.Context, peer string, r *rspb.ResourceName)
	listenAddr            string
	zone                  string
	enableCompressedReads bool
	verificationWG        sync.WaitGroup
}

func (c *Proxy) WaitForPendingVerificationsForTesting() {
	c.verificationWG.Wait()
}

func New(env environment.Env, c interfaces.Cache, listenAddr string) *Proxy {
	logger := log.NamedSubLogger(fmt.Sprintf("Proxy(%s)", listenAddr))
	proxy := &Proxy{
		env:            env,
		cache:          c,
		log:            logger,
		readRefLogger:  logger.EveryN(100),
		writeRefLogger: logger.EveryN(100),
		bufPool:        bytebufferpool.VariableSize(max(*config.ReadBufSizeBytes, writeBufSizeBytes)),
		listenAddr:     listenAddr,
		mu:             &sync.Mutex{},
		// server goes here
		clients: make(map[string]*grpc_client.ClientConnPool),
	}
	if zone := resources.GetZone(); zone != "" {
		proxy.zone = zone
	}
	return proxy
}

func (c *Proxy) StartListening() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.server != nil {
		return status.FailedPreconditionError("The server is already running.")
	}

	lis, err := net.Listen("tcp", c.listenAddr)
	if err != nil {
		return err
	}
	grpcOptions := grpc_server.CommonGRPCServerOptions(c.env)
	// Disable dynamic windows. Bursty traffic can undersize the window and
	// cause flow control to kick in prematurely.
	grpcOptions = append(grpcOptions, grpc.StaticConnWindowSize(int32(*connWindowSize)))
	grpcOptions = append(grpcOptions, grpc.StaticStreamWindowSize(int32(*streamWindowSize)))
	grpcServer := grpc.NewServer(grpcOptions...)
	reflection.Register(grpcServer)
	dcpb.RegisterDistributedCacheServer(grpcServer, c)
	c.server = grpcServer

	go func() {
		log.Printf("Listening on %s", c.listenAddr)
		if err := c.server.Serve(lis); err != nil {
			log.Warningf("Error serving: %s", err)
		}
	}()
	return nil
}

func (c *Proxy) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.server == nil {
		return status.FailedPreconditionError("The server was already stopped.")
	}
	err := grpc_server.GRPCShutdown(ctx, c.server)
	c.server = nil
	return err
}

func (c *Proxy) SetHeartbeatCallbackFunc(fn func(ctx context.Context, peer string)) {
	c.heartbeatCallback = fn
}

func (c *Proxy) SetHintedHandoffCallbackFunc(fn func(ctx context.Context, peer string, r *rspb.ResourceName)) {
	c.hintedHandoffCallback = fn
}

func (c *Proxy) SetEnableCompressedReads(enabled bool) {
	c.enableCompressedReads = enabled
}

// Size threshold matches the pebble default --cache.pebble.min_bytes_auto_zstd_compression
// used by distributed.copyFile.
func (c *Proxy) shouldReadCompressed(rn *rspb.ResourceName) bool {
	return c.enableCompressedReads &&
		rn.GetCompressor() == repb.Compressor_IDENTITY &&
		rn.GetDigest().GetSizeBytes() > 100 &&
		c.cache.SupportsCompressor(repb.Compressor_ZSTD)
}

func digestToKey(d *repb.Digest) *dcpb.Key {
	return &dcpb.Key{
		Key:       d.GetHash(),
		SizeBytes: d.GetSizeBytes(),
	}
}

func (c *Proxy) CloseInactiveClients(stillActive set.View[string]) {
	c.mu.Lock()
	var poolsToClose []*grpc_client.ClientConnPool
	for peer, pool := range c.clients {
		if !stillActive.Contains(peer) {
			delete(c.clients, peer)
			poolsToClose = append(poolsToClose, pool)
		}
	}
	c.mu.Unlock()
	for _, p := range poolsToClose {
		p.Close()
	}
}

func (c *Proxy) getClient(ctx context.Context, peer string) (dcpb.DistributedCacheClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if client, ok := c.clients[peer]; ok {
		if err := client.Check(ctx); err != nil {
			return nil, status.UnavailableErrorf("no connections to peer %q are ready: %v", peer, err)
		}
		return dcpb.NewDistributedCacheClient(client), nil
	}
	log.Debugf("Creating new client for peer: %q", peer)

	resolverPrefix := "grpc://"
	if kuberesolver.RunningInKubernetes() && *enableKubeResolver {
		resolverPrefix = "kube:///"
	}

	// Disable dynamic windows. Bursty traffic can undersize the window and
	// cause flow control to kick in prematurely.
	conn, err := grpc_client.DialInternalWithPoolSize(c.env, resolverPrefix+peer,
		*poolSize,
		grpc.WithStaticConnWindowSize(int32(*connWindowSize)),
		grpc.WithStaticStreamWindowSize(int32(*streamWindowSize)))
	if err != nil {
		return nil, err
	}
	c.clients[peer] = conn
	return dcpb.NewDistributedCacheClient(conn), nil
}

func (c *Proxy) prepareContext(ctx context.Context) context.Context {
	if c.zone != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, resources.ZoneHeader, c.zone)
	}
	return ctx
}

func (c *Proxy) readWriteContext(ctx context.Context) (context.Context, error) {
	ctx, err := prefix.AttachUserPrefixToContext(c.prepareContext(ctx), c.env.GetAuthenticator())
	if err != nil {
		return ctx, err
	}
	ctx = authutil.ContextWithCachedAuthHeaders(ctx, c.env.GetAuthenticator())
	return ctx, err
}

func (c *Proxy) FindMissing(ctx context.Context, req *dcpb.FindMissingRequest) (*dcpb.FindMissingResponse, error) {
	ctx, err := c.readWriteContext(ctx)
	if err != nil {
		return nil, err
	}
	// Forward the purpose the originating node stamped on the request so the
	// local cache attributes present/absent metrics to the right code path.
	missing, err := c.cache.FindMissing(findmissing.ContextWithPurpose(ctx, req.GetPurpose()), req.GetResources())
	if err != nil {
		return nil, err
	}
	rsp := &dcpb.FindMissingResponse{}
	for _, d := range missing {
		rsp.Missing = append(rsp.Missing, digestToKey(d))
	}
	return rsp, nil
}

func (c *Proxy) Metadata(ctx context.Context, req *dcpb.MetadataRequest) (*dcpb.MetadataResponse, error) {
	ctx, err := c.readWriteContext(ctx)
	if err != nil {
		return nil, err
	}
	md, err := c.cache.Metadata(ctx, req.GetResource())
	if err != nil {
		return nil, err
	}
	return &dcpb.MetadataResponse{
		StoredSizeBytes: md.StoredSizeBytes,
		DigestSizeBytes: md.DigestSizeBytes,
		LastModifyUsec:  md.LastModifyTimeUsec,
		LastAccessUsec:  md.LastAccessTimeUsec,
	}, nil
}

func (c *Proxy) GetWithMetadata(ctx context.Context, req *dcpb.GetWithMetadataRequest) (*dcpb.GetWithMetadataResponse, error) {
	ctx, err := c.readWriteContext(ctx)
	if err != nil {
		return nil, err
	}
	rn := req.GetResource()
	data, md, err := c.cache.GetWithMetadata(ctx, rn)
	if err != nil {
		return nil, err
	}
	return &dcpb.GetWithMetadataResponse{
		Data: data,
		Metadata: &dcpb.MetadataResponse{
			StoredSizeBytes: md.StoredSizeBytes,
			DigestSizeBytes: md.DigestSizeBytes,
			LastModifyUsec:  md.LastModifyTimeUsec,
			LastAccessUsec:  md.LastAccessTimeUsec,
		},
	}, nil
}

type resourceIsolationStringer struct{ *rspb.ResourceName }

func (r resourceIsolationStringer) String() string {
	rep := filepath.Join(r.GetInstanceName(), digest.CacheTypeToPrefix(r.GetCacheType()), r.GetDigest().GetHash())
	if !strings.HasSuffix(rep, "/") {
		rep += "/"
	}
	return rep
}

// ResourceIsolationString lazily returns a compact representation of a
// resource's isolation that is suitable for logging. This returns a
// fmt.Stringer instead of a string because it avoids actual formatting if we
// never log a message (maybe because the log level isn't enabled).
func ResourceIsolationString(r *rspb.ResourceName) fmt.Stringer {
	return resourceIsolationStringer{r}
}

func (c *Proxy) Delete(ctx context.Context, req *dcpb.DeleteRequest) (*dcpb.DeleteResponse, error) {
	ctx, err := c.readWriteContext(ctx)
	if err != nil {
		return nil, err
	}
	err = c.cache.Delete(ctx, req.GetResource())
	if err != nil {
		return nil, err
	}
	return &dcpb.DeleteResponse{}, nil
}

func (c *Proxy) GetMulti(ctx context.Context, req *dcpb.GetMultiRequest) (*dcpb.GetMultiResponse, error) {
	ctx, err := c.readWriteContext(ctx)
	if err != nil {
		return nil, err
	}
	found, err := c.cache.GetMulti(ctx, req.GetResources())
	if err != nil {
		return nil, err
	}
	rsp := &dcpb.GetMultiResponse{}
	for d, buf := range found {
		if len(buf) == 0 {
			c.log.Warningf("returned a zero-length response for digest %q", d.GetHash())
		}
		rsp.KeyValue = append(rsp.KeyValue, &dcpb.KV{
			Key:   digestToKey(d),
			Value: buf,
		})
	}
	return rsp, nil
}

// referenceReadMode returns whether Read should send the client a reference
// to the blob's location in shared storage, and whether it should stream the
// blob's bytes, based on the reference-read experiments. Sending both lets
// the client verify the reference against the authoritative byte stream.
func (c *Proxy) referenceReadMode(ctx context.Context) (sendReference bool, sendBytes bool) {
	fp := c.env.GetExperimentFlagProvider()
	if fp == nil {
		return false, true
	}
	if fp.Boolean(ctx, "distributed_cache.verify_read_gcs_references", false) {
		return true, true
	}
	if fp.Boolean(ctx, "distributed_cache.read_gcs_references", false) {
		return true, false
	}
	return false, true
}

func (c *Proxy) Read(req *dcpb.ReadRequest, stream dcpb.DistributedCache_ReadServer) error {
	ctx, err := c.readWriteContext(stream.Context())
	if err != nil {
		return err
	}
	up, _ := prefix.UserPrefixFromContext(ctx)
	rn := req.GetResource()

	sendReference, sendBytes := c.referenceReadMode(ctx)
	var ref *refpb.Reference
	if refCache, ok := c.cache.(interfaces.ReferenceCache); ok {
		if sendReference {
			if r, err := refCache.ReadReference(ctx, rn); err == nil {
				ref = r
			}
		}
		// If no reference was minted, just stream the bytes.
		if ref == nil {
			sendBytes = true
		}
	}

	if ref != nil && !sendBytes {
		if err := stream.Send(&dcpb.ReadResponse{Reference: ref}); err != nil {
			return err
		}
		c.readRefLogger.Debugf("Read(%q) succeeded by reference (user prefix: %s)", ResourceIsolationString(rn), up)
		return nil
	}

	reader, err := c.cache.Reader(ctx, rn, req.GetOffset(), req.GetLimit())
	if err != nil {
		c.log.Debugf("Read(%q) failed (user prefix: %s), err: %s", ResourceIsolationString(rn), up, err)
		return err
	}
	defer reader.Close()

	// In verification mode, only send the reference once the byte reader has
	// been opened successfully, so a missing blob surfaces to the client the
	// same way it does today.
	if ref != nil {
		if err := stream.Send(&dcpb.ReadResponse{Reference: ref}); err != nil {
			return err
		}
	}

	bufSize := int64(digest.SafeBufferSize(rn, *config.ReadBufSizeBytes))
	copyBuf := c.bufPool.Get(bufSize)
	defer c.bufPool.Put(copyBuf)

	for {
		n, err := ioutil.ReadTryFillBuffer(reader, copyBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&dcpb.ReadResponse{Data: copyBuf[:n]}); err != nil {
			return err
		}
	}

	c.log.Debugf("Read(%q) succeeded (user prefix: %s)", ResourceIsolationString(rn), up)
	return nil
}

func (c *Proxy) Write(stream dcpb.DistributedCache_WriteServer) error {
	ctx, err := c.readWriteContext(stream.Context())
	if err != nil {
		return err
	}
	up, _ := prefix.UserPrefixFromContext(ctx)

	var bytesWritten int64
	var writeCloser interfaces.CommittedWriteCloser
	// A reference to-be-verified received alongside data bytes, and the
	// resource to verify it against.
	var verifyRef *refpb.Reference
	var verifyRN *rspb.ResourceName
	var req *dcpb.WriteRequest
	for {
		if req == nil {
			req = dcpb.WriteRequestFromVTPool()
			defer req.ReturnToVTPool()
		} else {
			// VT unmarshal doesn't reset, so we need to reset manually.
			req.ResetVT()
		}
		err := stream.RecvMsg(req)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		rn := req.GetResource()
		if writeCloser == nil {
			if rn.GetCacheType() == rspb.CacheType_CAS && req.GetCheckAlreadyExists() {
				missing, err := c.cache.FindMissing(findmissing.ContextWithPurpose(ctx, repb.FindMissingBlobsRequest_WRITE_DEDUPE), []*rspb.ResourceName{rn})
				if err == nil && len(missing) == 0 {
					return status.AlreadyExistsError("CAS digest already exists")
				}
			}
			if req.GetReference() != nil {
				if len(req.GetData()) == 0 {
					return c.writeReference(ctx, stream, req, up)
				}
				// The request carries both bytes and a reference: the bytes
				// are written and the reference is verified. Clone these
				// because the request proto is pooled and reused for later
				// messages.
				verifyRef = req.GetReference().CloneVT()
				verifyRN = rn.CloneVT()
			}
			wc, err := c.cache.Writer(ctx, rn)
			if err != nil {
				c.log.Debugf("Write(%q) failed (user prefix: %s), err: %s", ResourceIsolationString(rn), up, err)
				return err
			}
			defer wc.Close()
			writeCloser = wc
		}
		n, err := writeCloser.Write(req.GetData())
		if err != nil {
			return err
		}
		bytesWritten += int64(n)
		if req.GetFinishWrite() {
			if err := writeCloser.Commit(); err != nil {
				return err
			}
			if verifyRef != nil {
				if refCache, ok := c.cache.(interfaces.ReferenceCache); ok {
					// Verification is observe-only, so don't block the
					// write response on it. The stream context is canceled
					// when this handler returns, so extend it.
					vctx, cancel := background.ExtendContextForFinalization(ctx, referenceVerificationTimeout)
					c.verificationWG.Add(1)
					go func() {
						defer c.verificationWG.Done()
						defer cancel()
						c.verifyReferenceWrite(vctx, refCache, verifyRef, verifyRN)
					}()
				}
			}
			c.log.Debugf("Write(%q) succeeded (user prefix: %s)", ResourceIsolationString(rn), up)
			return c.finishWrite(ctx, stream, req, bytesWritten)
		}
	}
	return nil
}

// writeReference handles a write request that carries a reference to a blob
// in shared storage instead of data bytes.
func (c *Proxy) writeReference(ctx context.Context, stream dcpb.DistributedCache_WriteServer, req *dcpb.WriteRequest, userPrefix string) error {
	rn := req.GetResource()
	if !req.GetFinishWrite() {
		return status.InvalidArgumentError("a write carrying a reference must be a single message with finish_write set")
	}
	if len(req.GetData()) > 0 {
		return status.InvalidArgumentError("a write carrying a reference must not carry data bytes")
	}
	refCache, ok := c.cache.(interfaces.ReferenceCache)
	if !ok {
		return status.UnimplementedErrorf("the local cache (%T) cannot accept references", c.cache)
	}
	if err := refCache.WriteReference(ctx, req.GetReference(), rn, req.GetReferenceMustBeCloned()); err != nil {
		c.log.Warningf("Write(%q) by reference failed (user prefix: %s), err: %s", ResourceIsolationString(rn), userPrefix, err)
		return err
	}
	c.writeRefLogger.Debugf("Write(%q) succeeded by reference (user prefix: %s)", ResourceIsolationString(rn), userPrefix)
	return c.finishWrite(ctx, stream, req, req.GetReference().GetMetadata().GetStoredSizeBytes())
}

// verifyReferenceWrite checks that dereferencing ref yields content that
// hashes to rn's digest, and logs and counts the outcome.
func (c *Proxy) verifyReferenceWrite(ctx context.Context, refCache interfaces.ReferenceCache, ref *refpb.Reference, rn *rspb.ResourceName) {
	if rn.GetCacheType() != rspb.CacheType_CAS {
		c.log.Errorf("Reference write verification is only supported for CAS cache type, got %q", rn.GetCacheType())
		metrics.DistributedCacheReferenceWriteVerificationCount.With(
			prometheus.Labels{metrics.VerificationOutcomeLabel: VerificationError}).Inc()
		return
	}

	// Dereference the reference with the IDENTITY compressor to verify its hash
	identityRN := rn.CloneVT()
	identityRN.Compressor = repb.Compressor_IDENTITY
	readCloser, err := refCache.Dereference(ctx, ref, identityRN, 0, 0)
	if err != nil {
		c.log.Errorf("Error dereferencing %q for write verification: %s", ResourceIsolationString(rn), err)
		metrics.DistributedCacheReferenceWriteVerificationCount.With(
			prometheus.Labels{metrics.VerificationOutcomeLabel: VerificationError}).Inc()
		return
	}
	defer readCloser.Close()

	// Compute the digest of the dereferenced bytes and compare what's expected
	d, err := digest.Compute(readCloser, rn.GetDigestFunction())
	if err != nil {
		c.log.Errorf("Reference write verification error for %q: %s", ResourceIsolationString(rn), err)
		metrics.DistributedCacheReferenceWriteVerificationCount.With(
			prometheus.Labels{metrics.VerificationOutcomeLabel: VerificationError}).Inc()
		return
	}
	if d.GetHash() != rn.GetDigest().GetHash() || d.GetSizeBytes() != rn.GetDigest().GetSizeBytes() {
		c.log.Errorf("Reference write verification failed for %q: expected %s/%d, got %s/%d", ResourceIsolationString(rn), rn.GetDigest().GetHash(), rn.GetDigest().GetSizeBytes(), d.GetHash(), d.GetSizeBytes())
		metrics.DistributedCacheReferenceWriteVerificationCount.With(
			prometheus.Labels{metrics.VerificationOutcomeLabel: VerificationFailure}).Inc()
		return
	}
	metrics.DistributedCacheReferenceWriteVerificationCount.With(
		prometheus.Labels{metrics.VerificationOutcomeLabel: VerificationSuccess}).Inc()
}

func (c *Proxy) finishWrite(ctx context.Context, stream dcpb.DistributedCache_WriteServer, req *dcpb.WriteRequest, committedSize int64) error {
	if req.GetHandoffPeer() != "" && c.hintedHandoffCallback != nil {
		// Because the hinted handoff callback might hold on to the resource
		// in a queue, and we're pooling WriteRequest protos, clone it.
		c.hintedHandoffCallback(ctx, req.GetHandoffPeer(), req.GetResource().CloneVT())
	}
	return stream.SendAndClose(&dcpb.WriteResponse{
		CommittedSize: committedSize,
	})
}

func (c *Proxy) Heartbeat(ctx context.Context, req *dcpb.HeartbeatRequest) (*dcpb.HeartbeatResponse, error) {
	if req.GetSource() == "" {
		return nil, status.InvalidArgumentError("A source is required.")
	}
	if c.heartbeatCallback != nil {
		c.heartbeatCallback(ctx, req.GetSource())
	}
	return &dcpb.HeartbeatResponse{}, nil
}

func (c *Proxy) RemoteContains(ctx context.Context, peer string, r *rspb.ResourceName) (bool, error) {
	missing, err := c.RemoteFindMissing(findmissing.ContextWithPurpose(ctx, repb.FindMissingBlobsRequest_CONTAINS), peer, []*rspb.ResourceName{r})
	if err != nil {
		return false, err
	}
	return len(missing) == 0, nil
}

func (c *Proxy) RemoteMetadata(ctx context.Context, peer string, r *rspb.ResourceName) (*interfaces.CacheMetadata, error) {
	req := &dcpb.MetadataRequest{
		Resource: r,
	}
	client, err := c.getClient(ctx, peer)
	if err != nil {
		return nil, err
	}

	md, err := client.Metadata(ctx, req)
	if err != nil {
		return nil, err
	}
	return &interfaces.CacheMetadata{
		StoredSizeBytes:    md.GetStoredSizeBytes(),
		DigestSizeBytes:    md.GetDigestSizeBytes(),
		LastAccessTimeUsec: md.GetLastAccessUsec(),
		LastModifyTimeUsec: md.GetLastModifyUsec(),
	}, nil
}

func (c *Proxy) RemoteGetWithMetadata(ctx context.Context, peer string, r *rspb.ResourceName) ([]byte, *interfaces.CacheMetadata, error) {
	client, err := c.getClient(ctx, peer)
	if err != nil {
		return nil, nil, err
	}
	// Fetch compressed data over the wire and decompress it locally, like
	// RemoteReader and RemoteGetMulti do.
	decompress := c.shouldReadCompressed(r)
	if decompress {
		r = r.CloneVT()
		r.Compressor = repb.Compressor_ZSTD
	}
	rsp, err := client.GetWithMetadata(ctx, &dcpb.GetWithMetadataRequest{Resource: r})
	if err != nil {
		return nil, nil, err
	}
	data := rsp.GetData()
	if decompress {
		data, err = compression.DecompressZstd(make([]byte, 0, digest.SafeBufferSize(r, maxDecompressBufSizeBytes)), data)
		if err != nil {
			return nil, nil, err
		}
	}
	md := rsp.GetMetadata()
	return data, &interfaces.CacheMetadata{
		StoredSizeBytes:    md.GetStoredSizeBytes(),
		DigestSizeBytes:    md.GetDigestSizeBytes(),
		LastAccessTimeUsec: md.GetLastAccessUsec(),
		LastModifyTimeUsec: md.GetLastModifyUsec(),
	}, nil
}

func (c *Proxy) RemoteFindMissing(ctx context.Context, peer string, resources []*rspb.ResourceName) ([]*repb.Digest, error) {
	req := &dcpb.FindMissingRequest{
		Resources: resources,
		// Propagate the originating purpose to the authoritative peer so it can
		// attribute present/absent metrics to the right code path.
		Purpose: findmissing.PurposeFromContext(ctx),
	}
	client, err := c.getClient(ctx, peer)
	if err != nil {
		return nil, err
	}
	rsp, err := client.FindMissing(ctx, req)
	if err != nil {
		return nil, err
	}
	var missing []*repb.Digest
	for _, k := range rsp.GetMissing() {
		missing = append(missing, &repb.Digest{
			Hash:      k.GetKey(),
			SizeBytes: k.GetSizeBytes(),
		})
	}
	return missing, nil
}

func (c *Proxy) RemoteDelete(ctx context.Context, peer string, r *rspb.ResourceName) error {
	req := &dcpb.DeleteRequest{
		Resource: r,
	}
	client, err := c.getClient(ctx, peer)
	if err != nil {
		return err
	}
	_, err = client.Delete(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (c *Proxy) RemoteGetMulti(ctx context.Context, peer string, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	req := &dcpb.GetMultiRequest{}
	hashResources := make(map[string]*rspb.ResourceName, len(resources))
	compressedHashes := make(set.Set[string], len(resources))
	for _, r := range resources {
		hashResources[r.GetDigest().GetHash()] = r
		if c.shouldReadCompressed(r) {
			r = r.CloneVT()
			r.Compressor = repb.Compressor_ZSTD
			compressedHashes.Add(r.GetDigest().GetHash())
		}
		req.Resources = append(req.Resources, r)
	}
	client, err := c.getClient(ctx, peer)
	if err != nil {
		return nil, err
	}
	rsp, err := client.GetMulti(ctx, req)
	if err != nil {
		return nil, err
	}
	resultMap := make(map[*repb.Digest][]byte, len(rsp.GetKeyValue()))
	for _, keyValue := range rsp.GetKeyValue() {
		rn, ok := hashResources[keyValue.GetKey().GetKey()]
		if !ok {
			continue
		}
		d := rn.GetDigest()
		buf := keyValue.GetValue()
		if compressedHashes.Contains(d.GetHash()) {
			buf, err = compression.DecompressZstd(make([]byte, 0, digest.SafeBufferSize(rn, maxDecompressBufSizeBytes)), buf)
			if err != nil {
				return nil, err
			}
		}
		resultMap[d] = buf
	}
	return resultMap, nil
}

func (c *Proxy) RemoteReader(ctx context.Context, peer string, r *rspb.ResourceName, offset, limit int64) (io.ReadCloser, error) {
	client, err := c.getClient(ctx, peer)
	if err != nil {
		return nil, err
	}
	// Pebble rejects offset/limit when the request matches the stored compressor,
	// so skip the rewrite on the partial-read path.
	decompress := offset == 0 && limit == 0 && c.shouldReadCompressed(r)
	// The resource the caller actually asked for, captured before any
	// transport-only compressor rewrite below.
	requested := r
	if decompress {
		r = r.CloneVT()
		r.Compressor = repb.Compressor_ZSTD
	}
	req := &dcpb.ReadRequest{
		Offset:   offset,
		Limit:    limit,
		Resource: r,
	}
	stream, err := client.Read(ctx, req)
	if err != nil {
		return nil, err
	}
	rc, err := newDistributedCacheReader(stream, r.GetDigest().GetSizeBytes() == offset)
	if err != nil {
		return nil, err
	}

	if rc.rsp.GetReference() != nil {
		// Fetching more messages from the stream or closing it returns the
		// proto to the pool, but the reference may live longer, so clone it.
		ref := rc.rsp.GetReference().CloneVT()

		// Confirm the provided reference matches what was requested.
		fr := ref.GetMetadata().GetFileRecord()
		frd := fr.GetDigest()
		if frd.GetHash() != r.GetDigest().GetHash() ||
			frd.GetSizeBytes() != r.GetDigest().GetSizeBytes() ||
			fr.GetDigestFunction() != r.GetDigestFunction() ||
			fr.GetIsolation().GetCacheType() != r.GetCacheType() ||
			fr.GetIsolation().GetRemoteInstanceName() != r.GetInstanceName() {
			rc.Close()
			return nil, status.InternalErrorf("peer %q returned a reference for %s/%d (cache type %s, instance %q), but %s/%d (cache type %s, instance %q) was requested",
				peer,
				frd.GetHash(), frd.GetSizeBytes(), fr.GetIsolation().GetCacheType(), fr.GetIsolation().GetRemoteInstanceName(),
				r.GetDigest().GetHash(), r.GetDigest().GetSizeBytes(), r.GetCacheType(), r.GetInstanceName())
		}

		// If the server is also streaming the data, serve those bytes to the
		// caller and verify that dereferencing the reference produces the
		// same stream. This lets us verify references end-to-end while the
		// byte stream remains the source of truth.
		if rc.moreData() {
			byteReader := io.ReadCloser(rc)
			if decompress {
				dr, err := compression.NewZstdDecompressingReader(rc)
				if err != nil {
					rc.Close()
					return nil, err
				}
				byteReader = dr
			}
			recordReadResponseMetrics("bytes", r)
			refReader, err := c.dereference(ctx, peer, ref, requested, offset, limit)
			if err != nil {
				// Verification is best-effort: the byte stream is
				// authoritative, so log and serve it.
				c.log.Warningf("Cannot verify reference for %q from peer %q: %s", ResourceIsolationString(r), peer, err)
				metrics.DistributedCacheReferenceVerificationCount.With(
					prometheus.Labels{metrics.VerificationOutcomeLabel: VerificationError}).Inc()
				return byteReader, nil
			}
			return NewVerifyingReadCloser(byteReader, refReader, c.log, r, peer), nil
		}

		// The reference is the whole response: dereference it.
		if err := rc.Close(); err != nil {
			c.log.Warningf("Error closing read stream after receiving a reference: %s", err)
		}
		recordReadResponseMetrics("reference", r)
		return c.dereference(ctx, peer, ref, requested, offset, limit)
	}

	if !decompress {
		recordReadResponseMetrics("bytes", r)
		return rc, nil
	}
	dr, err := compression.NewZstdDecompressingReader(rc)
	if err != nil {
		rc.Close()
		return nil, err
	}
	recordReadResponseMetrics("bytes", r)
	return dr, nil
}

// recordReadResponseMetrics records that a peer read's payload was received
// as responseType ("reference" or "bytes"), attributing the requested
// digest's size to it.
func recordReadResponseMetrics(responseType string, r *rspb.ResourceName) {
	metrics.DistributedCacheReadResponseCount.With(
		prometheus.Labels{metrics.DistributedCacheReadResponseType: responseType}).Inc()
	metrics.DistributedCacheReadResponseSizeBytes.With(
		prometheus.Labels{metrics.DistributedCacheReadResponseType: responseType}).Add(float64(r.GetDigest().GetSizeBytes()))
}

func (c *Proxy) dereference(ctx context.Context, peer string, ref *refpb.Reference, requested *rspb.ResourceName, offset, limit int64) (io.ReadCloser, error) {
	refCache, ok := c.cache.(interfaces.ReferenceCache)
	if !ok {
		return nil, status.FailedPreconditionErrorf("peer %q returned a reference, but the local cache (%T) cannot dereference", peer, c.cache)
	}
	return refCache.Dereference(ctx, ref, requested, offset, limit)
}

// verifyingReadCloser serves bytes from primary while reading the same number
// of bytes from secondary and comparing the two streams. It logs the outcome
// and records a metric for analysis.
type verifyingReadCloser struct {
	primary   io.ReadCloser
	secondary io.ReadCloser
	log       log.Logger
	resource  *rspb.ResourceName
	peer      string

	scratch  []byte
	compared int64
	done     bool
}

func NewVerifyingReadCloser(primary, secondary io.ReadCloser, log log.Logger, r *rspb.ResourceName, peer string) io.ReadCloser {
	return &verifyingReadCloser{
		primary:   primary,
		secondary: secondary,
		log:       log,
		resource:  r,
		peer:      peer,
	}
}

func (v *verifyingReadCloser) verify(p []byte) {
	if v.done {
		return
	}
	if len(p) > cap(v.scratch) {
		v.scratch = make([]byte, len(p))
	}
	scratch := v.scratch[:len(p)]
	if _, err := io.ReadFull(v.secondary, scratch); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			v.report(VerificationFailure, status.InternalErrorf("dereferenced bytes ended early at offset %d", v.compared))
		} else {
			v.report(VerificationError, status.InternalErrorf("error reading dereferenced bytes at offset %d: %s", v.compared, err))
		}
		return
	}
	if !bytes.Equal(p, scratch) {
		v.report(VerificationFailure, status.InternalErrorf("dereferenced bytes differ from streamed bytes at offset %d", v.compared))
		return
	}
	v.compared += int64(len(p))
}

func (v *verifyingReadCloser) verifyEOF() {
	if v.done {
		return
	}
	var b [1]byte
	n, err := v.secondary.Read(b[:])
	switch {
	case n == 0 && err == io.EOF:
		v.report(VerificationSuccess, nil)
	case n != 0 || err == nil:
		v.report(VerificationFailure, status.InternalErrorf("dereferenced bytes continue past streamed bytes at offset %d", v.compared))
	default:
		v.report(VerificationError, status.InternalErrorf("error reading dereferenced bytes at offset %d: %s", v.compared, err))
	}
}

func (v *verifyingReadCloser) report(verificationStatus string, err error) {
	v.done = true
	switch verificationStatus {
	case VerificationFailure:
		v.log.Errorf("Reference verification failed for %q from peer %q: %s", ResourceIsolationString(v.resource), v.peer, err)
	case VerificationError:
		v.log.Warningf("Reference verification error for %q from peer %q: %s", ResourceIsolationString(v.resource), v.peer, err)
	}
	metrics.DistributedCacheReferenceVerificationCount.With(
		prometheus.Labels{metrics.VerificationOutcomeLabel: verificationStatus}).Inc()
}

func (v *verifyingReadCloser) Read(p []byte) (int, error) {
	n, err := v.primary.Read(p)
	if n > 0 {
		v.verify(p[:n])
	}
	if err == io.EOF {
		v.verifyEOF()
	}
	return n, err
}

func (v *verifyingReadCloser) Close() error {
	err := v.primary.Close()
	if serr := v.secondary.Close(); err == nil {
		err = serr
	}
	return err
}

type distributedCacheReader struct {
	stream dcpb.DistributedCache_ReadClient
	rsp    *dcpb.ReadResponse
	// offset into rsp.Data so we don't muck with rsp.Data
	// advancing rsp.Data prevents vtproto from reusing the backing buffer.
	off int
	err error
}

func newDistributedCacheReader(stream dcpb.DistributedCache_ReadClient, expectEOF bool) (*distributedCacheReader, error) {
	r := &distributedCacheReader{
		stream: stream,
		rsp:    dcpb.ReadResponseFromVTPool(),
	}
	// Bit annoying here -- the gRPC stream won't give us an error until
	// we've called Recv on it. But we don't want to return a reader that
	// we know will error on first read with NotFound -- we want to return
	// that error now. So read the first message here and return any unexpected
	// error.
	r.moreData()
	if r.err == nil || (r.err == io.EOF && expectEOF) {
		return r, nil
	}
	return nil, r.err
}

// moreData fetches the next batch of data if necessary, and returns true if
// there is more data.
func (r *distributedCacheReader) moreData() bool {
	if r.err == nil && r.off == len(r.rsp.GetData()) {
		r.err = r.stream.RecvMsg(r.rsp)
		if r.err == nil {
			r.off = 0
		}
	}
	return r.err == nil || r.off < len(r.rsp.GetData())
}

func (r *distributedCacheReader) Read(out []byte) (int, error) {
	if !r.moreData() {
		return 0, r.err
	}
	n := copy(out, r.rsp.GetData()[r.off:])
	r.off += n
	if !r.moreData() {
		// If there is no more data, allow returning a possible EOF. This lets
		// the client skip making another Read call just to get EOF.
		return n, r.err
	}
	return n, nil
}

func (r *distributedCacheReader) WriteTo(w io.Writer) (int64, error) {
	var total int64
	for r.moreData() {
		n, err := w.Write(r.rsp.GetData()[r.off:])
		total += int64(n)
		if err != nil {
			return total, err
		}
		r.off += n
	}
	if r.err == io.EOF {
		return total, nil
	}
	return total, r.err
}

func (r *distributedCacheReader) Close() error {
	r.rsp.ReturnToVTPool()
	return r.stream.CloseSend()
}

type streamWriteCloser struct {
	cancelFunc    context.CancelFunc
	sender        rpcutil.Sender[*dcpb.WriteRequest, *dcpb.WriteResponse]
	r             *rspb.ResourceName
	peer          string
	handoffPeer   string
	alreadyExists bool
}

func (wc *streamWriteCloser) send(req *dcpb.WriteRequest) error {
	err := wc.sender.SendWithTimeoutCause(req, *peerWriteTimeout, context.DeadlineExceeded)
	if errors.Is(err, context.DeadlineExceeded) {
		err = status.DeadlineExceededErrorf("timed out sending distributed cache write to peer %q for %s", wc.peer, ResourceIsolationString(wc.r))
		wc.cancelFunc()
	}
	return err
}

func (wc *streamWriteCloser) closeAndRecv() (*dcpb.WriteResponse, error) {
	rsp, err := wc.sender.CloseAndRecvWithTimeoutCause(*peerWriteTimeout, context.DeadlineExceeded)
	if errors.Is(err, context.DeadlineExceeded) {
		err = status.DeadlineExceededErrorf("timed out finalizing distributed cache write to peer %q for %s", wc.peer, ResourceIsolationString(wc.r))
		wc.cancelFunc()
	}
	return rsp, err
}

func (wc *streamWriteCloser) Write(data []byte) (int, error) {
	if wc.alreadyExists {
		return len(data), nil
	}
	req := &dcpb.WriteRequest{
		Data:               data,
		FinishWrite:        false,
		CheckAlreadyExists: true,
		HandoffPeer:        wc.handoffPeer,
		Resource:           wc.r,
	}
	err := wc.send(req)
	if err == io.EOF {
		_, streamErr := wc.closeAndRecv()
		if status.IsAlreadyExistsError(streamErr) {
			wc.alreadyExists = true
			err = nil
		} else if streamErr != nil {
			return 0, streamErr
		} else {
			return 0, io.ErrShortWrite
		}
	}
	return len(data), err
}

func (wc *streamWriteCloser) Commit() error {
	if wc.alreadyExists {
		return nil
	}

	req := &dcpb.WriteRequest{
		FinishWrite:        true,
		CheckAlreadyExists: true,
		HandoffPeer:        wc.handoffPeer,
		Resource:           wc.r,
	}
	sendErr := wc.send(req)
	if sendErr != nil && sendErr != io.EOF {
		return sendErr
	}
	_, err := wc.closeAndRecv()
	if status.IsAlreadyExistsError(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return sendErr
}

func (wc *streamWriteCloser) Close() error {
	// Cancel the stream ctx to unblock any in-flight stream.Send() in the
	// Sender's background goroutine and let gRPC clean up the stream.
	// Deliberately do NOT call stream.CloseAndRecv() here: if Commit() was
	// called successfully it already did, and if the write was abandoned the
	// stream is already broken, so CloseAndRecv would just race against an
	// unwinding Send and leak a goroutine stuck in waitOnHeader.
	wc.cancelFunc()
	return nil
}

func (c *Proxy) RemoteWriter(ctx context.Context, peer, handoffPeer string, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	client, err := c.getClient(ctx, peer)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	stream, err := client.Write(ctx)
	if err != nil {
		cancel()
		return nil, err
	}

	wc := &streamWriteCloser{
		cancelFunc:  cancel,
		sender:      rpcutil.NewSender[*dcpb.WriteRequest, *dcpb.WriteResponse](ctx, stream),
		peer:        peer,
		handoffPeer: handoffPeer,
		r:           r,
	}
	return ioutil.NewDoubleBufferWriter(ctx, wc, c.bufPool, digest.SafeBufferSize(r, writeBufSizeBytes), writeBufSizeBytes), nil
}

func (c *Proxy) SendHeartbeat(ctx context.Context, peer string) error {
	client, err := c.getClient(ctx, peer)
	if err != nil {
		return err
	}
	req := &dcpb.HeartbeatRequest{
		Source: c.listenAddr,
	}
	_, err = client.Heartbeat(c.prepareContext(ctx), req)
	return err
}
