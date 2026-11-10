package vmvfs

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/bits"
	"net"
	"os"
	"path/filepath"
	"runtime/metrics"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vfs"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfs_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfscommon"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/cache/dirtools"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	vfspb "github.com/buildbuddy-io/buildbuddy/proto/vfs"
	vmfspb "github.com/buildbuddy-io/buildbuddy/proto/vmvfs"
	libVsock "github.com/mdlayher/vsock"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

const (
	// this should match guestVFSMountDir in firecracker.go
	mountDir       = "/vfs"
	scratchDir     = "/tmp/vmvfs-scratch"
	inputCacheDir  = "/tmp/vmvfs-inputs"
	inputChunkSize = 128 * 1024
	// outputChunkSize is the most file contents a ReadOutputs message has.
	outputChunkSize = 1 << 20
	// contentTimeBuckets is how many buckets the content request times are
	// counted in.
	contentTimeBuckets = 16
)

type vfsServer struct {
	vfs        *vfs.VFS
	local      *vfs_server.Server
	inputStore *remoteInputStore
	content    *contentClient
	hostConn   *grpc.ClientConn

	mu sync.Mutex
	// Context & cancel func for VFS RPCs.
	cancelRemoteFunc context.CancelFunc
	// layout is the current task's, for finding its outputs.
	layout *container.FileSystemLayout
	// mirror, if set, sends the current task's writes to the host.
	mirror *vfs_server.MirrorClient
	// taskProfile, if set, receives the CPU profile of the current task.
	taskProfile *bytes.Buffer
	// taskCounters is what serverCounters returned when the current task
	// was prepared.
	taskCounters map[string]int64
	// finishCounters is what serverCounters returned at the end of the last
	// Finish call.
	finishCounters map[string]int64
}

// contentClient makes raw content protocol requests to the host over a pool
// of vsock connections, each carrying a request at a time.
type contentClient struct {
	mu   sync.Mutex
	idle []*contentConn

	// What the requests since the last Finish call added up to.
	requests, bytes, nanos, dials atomic.Int64
	// The same requests by how long they took. See
	// FinishResponse.content_time_counts.
	timeCounts, timeUsec, timeBytes [contentTimeBuckets]atomic.Int64
	// blocking makes new connections blockingConns.
	blocking atomic.Bool
}

type contentConn struct {
	io.ReadWriteCloser
	// interrupt makes the connection's pending and later reads and writes
	// fail.
	interrupt func()
	r         *bufio.Reader
}

// blockingConn is a vsock connection to the host whose reads and writes
// block their thread in the system call, instead of parking the goroutine
// until the Go network poller reports the socket ready.
//
// The FUSE server's threads block in reads of /dev/fuse, which the poller
// does not cover. When a thread that was polling the network picks up a
// response and then goes back to reading /dev/fuse, no thread is left
// polling, and another request's response waits until the runtime's monitor
// thread polls, which it does only every 10 ms. A blocking read is woken by
// the kernel as soon as the response arrives.
type blockingConn struct {
	mu     sync.Mutex
	fd     int
	closed bool
}

func dialBlocking(port uint32) (*blockingConn, error) {
	for {
		fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
		if err != nil {
			return nil, err
		}
		err = unix.Connect(fd, &unix.SockaddrVM{CID: unix.VMADDR_CID_HOST, Port: port})
		if err == nil {
			return &blockingConn{fd: fd}, nil
		}
		unix.Close(fd)
		// A signal, such as the Go runtime's preemption signal, cancels a
		// vsock connect in progress, so we start over with a new socket.
		if err != unix.EINTR {
			return nil, err
		}
	}
}

func (c *blockingConn) Read(p []byte) (int, error) {
	for {
		n, err := unix.Read(c.fd, p)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return 0, err
		}
		if n == 0 && len(p) > 0 {
			return 0, io.EOF
		}
		return n, nil
	}
}

func (c *blockingConn) Write(p []byte) (int, error) {
	written := 0
	for written < len(p) {
		n, err := unix.Write(c.fd, p[written:])
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return written, err
		}
		written += n
	}
	return written, nil
}

func (c *blockingConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return unix.Close(c.fd)
}

// interrupt makes pending and later reads and writes fail. It holds the
// lock that Close takes, so that it never shuts down a descriptor that was
// closed and given to another file.
func (c *blockingConn) interrupt() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		unix.Shutdown(c.fd, unix.SHUT_RDWR)
	}
}

func (c *contentClient) get() (conn *contentConn, reused bool, err error) {
	c.mu.Lock()
	if n := len(c.idle); n > 0 {
		conn := c.idle[n-1]
		c.idle = c.idle[:n-1]
		c.mu.Unlock()
		return conn, true, nil
	}
	c.mu.Unlock()
	c.dials.Add(1)
	if c.blocking.Load() {
		bc, err := dialBlocking(vsock.HostVFSContentPort)
		if err != nil {
			return nil, false, status.UnavailableErrorf("dial host content server: %s", err)
		}
		return &contentConn{ReadWriteCloser: bc, interrupt: bc.interrupt, r: bufio.NewReaderSize(bc, 64*1024)}, false, nil
	}
	nc, err := libVsock.Dial(libVsock.Host, vsock.HostVFSContentPort, &libVsock.Config{})
	if err != nil {
		return nil, false, status.UnavailableErrorf("dial host content server: %s", err)
	}
	return &contentConn{
		ReadWriteCloser: nc,
		interrupt:       func() { nc.SetDeadline(time.Unix(1, 0)) },
		r:               bufio.NewReaderSize(nc, 64*1024),
	}, false, nil
}

func (c *contentClient) put(conn *contentConn) {
	c.mu.Lock()
	c.idle = append(c.idle, conn)
	c.mu.Unlock()
}

// reset closes the idle connections, since a VM snapshot restore breaks them.
func (c *contentClient) reset() {
	c.mu.Lock()
	idle := c.idle
	c.idle = nil
	c.mu.Unlock()
	for _, conn := range idle {
		conn.Close()
	}
}

// read requests length bytes of node at offset, and passes the content in
// the response to consume, which must read all n bytes of it. A request on a
// reused connection that fails before consume starts is retried once on a
// new connection, in case the old connection was broken. Once consume has
// started, it may have written part of the content, so it is not retried.
// Cancelling ctx fails the request.
func (c *contentClient) read(ctx context.Context, node *repb.FileNode, offset, length int64, consume func(r io.Reader, n int64) error) error {
	start := time.Now()
	c.requests.Add(1)
	var received int64
	defer func() {
		dt := time.Since(start)
		c.nanos.Add(int64(dt))
		b := min(bits.Len64(uint64(dt.Microseconds())/32), contentTimeBuckets-1)
		c.timeCounts[b].Add(1)
		c.timeUsec[b].Add(dt.Microseconds())
		c.timeBytes[b].Add(received)
	}()
	for attempt := 0; ; attempt++ {
		conn, reused, err := c.get()
		if err != nil {
			return err
		}
		// Unblock the reads and writes below when ctx is cancelled.
		stop := context.AfterFunc(ctx, conn.interrupt)
		consumed := false
		err = vfs_server.WriteContentRequest(conn, node, offset, length)
		if err == nil {
			var n int64
			n, err = vfs_server.ReadContentResponseHeader(conn.r)
			if err == nil {
				consumed = true
				c.bytes.Add(n)
				received = n
				err = consume(conn.r, n)
			}
		}
		if !stop() {
			err = errors.Join(err, ctx.Err())
		}
		if err == nil {
			c.put(conn)
			return nil
		}
		conn.Close()
		if !reused || attempt > 0 || consumed || ctx.Err() != nil {
			return err
		}
	}
}

// ReadAt reads part of node into buf and returns how many bytes it read.
func (c *contentClient) ReadAt(ctx context.Context, node *repb.FileNode, buf []byte, offset int64) (int, error) {
	var n int
	err := c.read(ctx, node, offset, int64(len(buf)), func(r io.Reader, size int64) error {
		if size > int64(len(buf)) {
			return status.DataLossErrorf("host sent %d bytes, more than the %d requested", size, len(buf))
		}
		n = int(size)
		_, err := io.ReadFull(r, buf[:n])
		return err
	})
	return n, err
}

type remoteInputStore struct {
	cacheDir string
	fetches  singleflight.Group[string, struct{}]

	mu                 sync.RWMutex
	client             bspb.ByteStreamClient
	remoteInstanceName string
	digestFunction     repb.DigestFunction_Value
	// compress requests zstd-compressed whole inputs, which pays off only
	// when the host forwards them from the remote CAS.
	compress bool
	// content, if set, replaces client for fetching contents.
	content      *contentClient
	cachedRanges map[string]struct{}
}

func (s *remoteInputStore) configure(client bspb.ByteStreamClient, remoteInstanceName string, digestFunction repb.DigestFunction_Value, compress bool, content *contentClient) {
	s.mu.Lock()
	s.client = client
	s.remoteInstanceName = remoteInstanceName
	s.digestFunction = digestFunction
	s.compress = compress
	s.content = content
	s.mu.Unlock()
}

func (s *remoteInputStore) path(node *repb.FileNode) string {
	return filepath.Join(s.cacheDir, node.GetDigest().GetHash())
}

func (s *remoteInputStore) Fetch(ctx context.Context, node *repb.FileNode) error {
	path := s.path(node)
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	_, _, err := s.fetches.Do(ctx, node.GetDigest().GetHash(), func(ctx context.Context) (struct{}, error) {
		if _, err := os.Stat(path); err == nil {
			return struct{}{}, nil
		}
		s.mu.RLock()
		client := s.client
		remoteInstanceName := s.remoteInstanceName
		digestFunction := s.digestFunction
		compress := s.compress
		content := s.content
		s.mu.RUnlock()
		if client == nil {
			return struct{}{}, status.FailedPreconditionError("ByteStream client is not configured")
		}
		tmp, err := os.CreateTemp(s.cacheDir, "input-")
		if err != nil {
			return struct{}{}, err
		}
		tmpPath := tmp.Name()
		defer os.Remove(tmpPath)
		defer tmp.Close()
		if node.GetDigest().GetSizeBytes() > inputChunkSize {
			// Reserve the logical size without allocating or downloading contents.
			err = tmp.Truncate(node.GetDigest().GetSizeBytes())
		} else if content != nil {
			err = content.read(ctx, node, 0, node.GetDigest().GetSizeBytes(), func(r io.Reader, n int64) error {
				_, err := io.CopyN(tmp, r, n)
				return err
			})
		} else if compress {
			rn := digest.NewCASResourceName(node.GetDigest(), remoteInstanceName, digestFunction)
			rn.SetCompressor(repb.Compressor_ZSTD)
			err = cachetools.GetBlob(ctx, client, rn, tmp)
		} else {
			// cachetools.GetBlob would ask for zstd, which only costs CPU
			// when the host serves the input from its file cache.
			rn := digest.NewCASResourceName(node.GetDigest(), remoteInstanceName, digestFunction)
			err = readBlob(ctx, client, rn, tmp)
		}
		if err != nil {
			return struct{}{}, err
		}
		if err := tmp.Close(); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, os.Rename(tmpPath, path)
	})
	return err
}

// readBlob writes the uncompressed contents of rn to w.
func readBlob(ctx context.Context, client bspb.ByteStreamClient, rn *digest.CASResourceName, w io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.Read(ctx, &bspb.ReadRequest{ResourceName: rn.DownloadString()})
	if err != nil {
		return err
	}
	for {
		rsp, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := w.Write(rsp.GetData()); err != nil {
			return err
		}
	}
}

func (s *remoteInputStore) FetchRange(ctx context.Context, node *repb.FileNode, offset, length int64) error {
	if offset < 0 || length < 0 {
		return status.InvalidArgumentError("negative input read range")
	}
	size := node.GetDigest().GetSizeBytes()
	if size <= inputChunkSize || offset >= size || length == 0 {
		return nil
	}
	end := offset + min(length, size-offset)
	for start := offset / inputChunkSize * inputChunkSize; start < end; start += inputChunkSize {
		key := fmt.Sprintf("%s/%d", node.GetDigest().GetHash(), start)
		s.mu.RLock()
		_, cached := s.cachedRanges[key]
		s.mu.RUnlock()
		if cached {
			continue
		}
		_, _, err := s.fetches.Do(ctx, key, func(ctx context.Context) (struct{}, error) {
			s.mu.RLock()
			_, cached := s.cachedRanges[key]
			client := s.client
			content := s.content
			rn := digest.NewCASResourceName(node.GetDigest(), s.remoteInstanceName, s.digestFunction)
			s.mu.RUnlock()
			if cached {
				return struct{}{}, nil
			}
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			limit := min(int64(inputChunkSize), size-start)
			data := make([]byte, 0, limit)
			if content != nil {
				n, err := content.ReadAt(ctx, node, data[:limit], start)
				if err != nil {
					return struct{}{}, err
				}
				data = data[:n]
			} else {
				stream, err := client.Read(ctx, &bspb.ReadRequest{
					ResourceName: rn.DownloadString(), ReadOffset: start, ReadLimit: limit,
				})
				if err != nil {
					return struct{}{}, err
				}
				for {
					rsp, err := stream.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						return struct{}{}, err
					}
					if int64(len(rsp.GetData())) > limit-int64(len(data)) {
						return struct{}{}, status.DataLossError("input range exceeded requested length")
					}
					data = append(data, rsp.GetData()...)
				}
			}
			if int64(len(data)) != limit {
				return struct{}{}, status.DataLossErrorf("input range returned %d bytes, expected %d", len(data), limit)
			}
			f, err := os.OpenFile(s.path(node), os.O_WRONLY, 0)
			if err != nil {
				return struct{}{}, err
			}
			defer f.Close()
			if _, err := f.WriteAt(data, start); err != nil {
				return struct{}{}, err
			}
			s.mu.Lock()
			if s.cachedRanges == nil {
				s.cachedRanges = make(map[string]struct{})
			}
			s.cachedRanges[key] = struct{}{}
			s.mu.Unlock()
			return struct{}{}, nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *remoteInputStore) Open(ctx context.Context, node *repb.FileNode) (*os.File, error) {
	return os.Open(s.path(node))
}

// setBDI sets the readahead window, in KiB, and the share of the dirty page
// limit, in percent, of the VFS mount's backing device info, where set.
func setBDI(readAheadKB, maxDirtyRatio int32) error {
	if readAheadKB <= 0 && maxDirtyRatio <= 0 {
		return nil
	}
	// The backing device info is named after the mount's device number.
	// Read that from mountinfo, since a stat of the mount would be a FUSE
	// request to this process.
	dev := ""
	if b, err := os.ReadFile("/proc/self/mountinfo"); err == nil {
		for line := range strings.SplitSeq(string(b), "\n") {
			if f := strings.Fields(line); len(f) > 4 && f[4] == mountDir {
				dev = f[2]
			}
		}
	}
	if dev == "" {
		return status.NotFoundError("VFS mount not found in mountinfo")
	}
	for name, v := range map[string]int32{"read_ahead_kb": readAheadKB, "max_ratio": maxDirtyRatio} {
		if v <= 0 {
			continue
		}
		if err := os.WriteFile("/sys/class/bdi/"+dev+"/"+name, []byte(fmt.Sprint(v)), 0); err != nil {
			return status.WrapErrorf(err, "set VFS %s", name)
		}
	}
	return nil
}

func dialHost(ctx context.Context, block bool) (*grpc.ClientConn, error) {
	vsockDialer := func(ctx context.Context, s string) (net.Conn, error) {
		conn, err := libVsock.Dial(libVsock.Host, vsock.HostVFSServerPort, &libVsock.Config{})
		return conn, err
	}
	opts := []grpc.DialOption{grpc.WithContextDialer(vsockDialer), grpc.WithInsecure()}
	if block {
		opts = append(opts, grpc.WithBlock())
	}
	return grpc.DialContext(ctx, "vsock", opts...)
}

func NewServer(writebackCache bool) (*vfsServer, error) {
	if err := os.MkdirAll(mountDir, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(scratchDir, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(inputCacheDir, 0755); err != nil {
		return nil, err
	}

	conn, err := dialHost(context.Background(), false)
	if err != nil {
		return nil, status.InternalErrorf("Could not dial host: %s", err)
	}

	vfsClient := vfspb.NewFileSystemClient(conn)
	inputStore := &remoteInputStore{cacheDir: inputCacheDir}
	local, err := vfs_server.NewLocal(scratchDir, inputStore.Open)
	if err != nil {
		return nil, status.WrapError(err, "create guest VFS server")
	}
	// A reused VM must see only the next action's inputs.
	local.SetPruneStale(true)
	local.SetWritebackCache(writebackCache)

	fs := vfs.New(vfsClient, mountDir, &vfs.Options{WritebackCache: writebackCache})
	if err := fs.Mount(); err != nil {
		return nil, status.InternalErrorf("Could not mount VFS: %s", err)
	}

	return &vfsServer{
		vfs:        fs,
		local:      local,
		inputStore: inputStore,
		content:    &contentClient{},
		hostConn:   conn,
	}, nil
}

// serverCounters returns this process's cumulative CPU time, page faults,
// context switches and garbage collection counters.
func serverCounters() map[string]int64 {
	out := map[string]int64{}
	var ru unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &ru); err == nil {
		out["user_usec"] = ru.Utime.Nano() / 1000
		out["system_usec"] = ru.Stime.Nano() / 1000
		out["minor_faults"] = ru.Minflt
		out["major_faults"] = ru.Majflt
		out["voluntary_switches"] = ru.Nvcsw
		out["involuntary_switches"] = ru.Nivcsw
	}
	// These come from runtime/metrics and not from runtime.ReadMemStats,
	// which stops the world. With Go 1.26 and 1.27, a stop that begins while
	// a GC mark worker is scanning a goroutine that is blocked in a system
	// call with its P (such as go-fuse's readers of /dev/fuse) does not
	// finish until the runtime's monitor thread wakes, which takes until the
	// process's next timer, or up to two minutes if it has none.
	samples := []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}, {Name: "/cpu/classes/scavenge/total:cpu-seconds"}, {Name: "/gc/cycles/total:gc-cycles"}}
	metrics.Read(samples)
	out["gc_cpu_usec"] = int64(samples[0].Value.Float64() * 1e6)
	out["scavenge_cpu_usec"] = int64(samples[1].Value.Float64() * 1e6)
	out["gc_cycles"] = int64(samples[2].Value.Uint64())
	return out
}

// countersSince formats how much each of serverCounters' values grew from
// before to now, with prefix before each name.
func countersSince(prefix string, now, before map[string]int64) string {
	var b strings.Builder
	for _, name := range slices.Sorted(maps.Keys(now)) {
		fmt.Fprintf(&b, "%s%s=%d ", prefix, name, now[name]-before[name])
	}
	return strings.TrimSpace(b.String())
}

func (s *vfsServer) Prepare(ctx context.Context, req *vmfspb.PrepareRequest) (*vmfspb.PrepareResponse, error) {
	prepareStart := time.Now()
	startCounters := serverCounters()
	s.mu.Lock()
	finishCounters := s.finishCounters
	s.mu.Unlock()
	conn, err := dialHost(ctx, true)
	if err != nil {
		return nil, status.WrapError(err, "redial host VFS server")
	}
	dialDuration := time.Since(prepareStart)

	// This is the context that is used to make RPCs to the host.
	// It needs to stay alive as long as there's an active command on the VM.
	rpcCtx, cancel := context.WithCancel(context.Background())
	client := vfspb.FileSystemClient(vfspb.NewFileSystemClient(conn))
	var invalidations *vfscommon.InodeInvalidations
	var layout *container.FileSystemLayout
	var mirror *vfs_server.MirrorClient
	rsp := &vmfspb.PrepareResponse{DialDurationUsec: dialDuration.Microseconds()}
	if finishCounters != nil {
		rsp.IdleStats = countersSince("idle_", startCounters, finishCounters)
	}
	if req.GetUseGuestServer() {
		var cpuProfile bytes.Buffer
		if req.GetProfilePreparation() {
			if err := pprof.StartCPUProfile(&cpuProfile); err != nil {
				cancel()
				_ = conn.Close()
				return nil, err
			}
			defer pprof.StopCPUProfile()
		}
		casClient := repb.NewContentAddressableStorageClient(conn)
		rn := digest.NewCASResourceName(req.GetInputRootDigest(), req.GetRemoteInstanceName(), req.GetDigestFunction())
		treeStart := time.Now()
		tree, err := cachetools.GetTreeFromRootDirectoryDigest(rpcCtx, casClient, rn)
		if err != nil {
			cancel()
			_ = conn.Close()
			return nil, status.WrapError(err, "fetch input tree")
		}
		rsp.GetTreeDurationUsec = time.Since(treeStart).Microseconds()
		validateStart := time.Now()
		if err := dirtools.ValidateTreeNames(tree, false); err != nil {
			cancel()
			_ = conn.Close()
			return nil, err
		}
		if wd := req.GetWorkingDirectory(); wd != "" {
			if err := dirtools.ValidateWorkingDirectoryInTree(tree, req.GetDigestFunction(), wd); err != nil {
				cancel()
				_ = conn.Close()
				return nil, err
			}
		}
		rsp.ValidateDurationUsec = time.Since(validateStart).Microseconds()
		mode := req.GetContentMode()
		if mode == "" {
			mode = vfs_server.GuestContentRemote
		}
		var content *contentClient
		switch mode {
		case vfs_server.GuestContentRemote, vfs_server.GuestContentFileCache:
		case vfs_server.GuestContentRaw, vfs_server.GuestContentDirect:
			s.content.blocking.Store(req.GetBlockingContentReads())
			s.content.reset()
			content = s.content
		default:
			cancel()
			_ = conn.Close()
			return nil, status.InvalidArgumentErrorf("unknown guest content mode %q", mode)
		}
		s.inputStore.configure(bspb.NewByteStreamClient(conn), req.GetRemoteInstanceName(), req.GetDigestFunction(), mode == vfs_server.GuestContentRemote, content)
		if mode == vfs_server.GuestContentDirect {
			s.local.SetInputReader(s.content.ReadAt)
		} else {
			s.local.SetInputReader(nil)
		}
		// The original mode is left as it was, so that the others can be
		// compared with it.
		s.vfs.SetKeepImmutableCache(mode != vfs_server.GuestContentRemote)
		s.vfs.SetNoFlushImmutable(req.GetNoFlushInputs())
		if err := setBDI(req.GetFuseReadAheadKb(), req.GetFuseMaxDirtyRatio()); err != nil {
			cancel()
			_ = conn.Close()
			return nil, err
		}
		layout = &container.FileSystemLayout{
			RemoteInstanceName: req.GetRemoteInstanceName(),
			DigestFunction:     req.GetDigestFunction(),
			Inputs:             tree,
			WorkingDirectory:   req.GetWorkingDirectory(),
			OutputDirectories:  req.GetOutputDirectories(),
			OutputFiles:        req.GetOutputFiles(),
			OutputPaths:        req.GetOutputPaths(),
		}
		namespaceStart := time.Now()
		invalidations, err = s.local.Prepare(rpcCtx, layout, s.inputStore)
		if err != nil {
			cancel()
			_ = conn.Close()
			return nil, status.WrapError(err, "prepare guest VFS server")
		}
		rsp.NamespacePrepareDurationUsec = time.Since(namespaceStart).Microseconds()
		if req.GetMirrorWrites() {
			mc, err := libVsock.Dial(libVsock.Host, vsock.HostVFSMirrorPort, &libVsock.Config{})
			if err != nil {
				cancel()
				_ = conn.Close()
				return nil, status.WrapError(err, "dial host write mirror")
			}
			mirror = vfs_server.NewMirrorClient(mc)
			s.local.SetWriteMirror(mirror)
		} else {
			s.local.SetWriteMirror(nil)
		}
		if req.GetProfilePreparation() {
			pprof.StopCPUProfile()
			rsp.PreparationCpuProfile = cpuProfile.Bytes()
		}
		client = vfs_server.NewDirectClient(s.local)
	}
	s.mu.Lock()
	oldConn := s.hostConn
	oldCancel := s.cancelRemoteFunc
	oldMirror := s.mirror
	s.hostConn = conn
	s.vfs.SetClient(client)
	s.cancelRemoteFunc = cancel
	s.layout = layout
	s.mirror = mirror
	s.mu.Unlock()
	if oldCancel != nil {
		oldCancel()
	}
	if oldMirror != nil {
		_ = oldMirror.Close()
	}
	if oldConn != nil {
		_ = oldConn.Close()
	}

	invalidateStart := time.Now()
	if err := s.vfs.PrepareForTask(rpcCtx, "fc", invalidations); err != nil {
		return nil, err
	}
	rsp.KernelInvalidateDurationUsec = time.Since(invalidateStart).Microseconds()

	// Count only what the task does from here on.
	s.vfs.TakeOpStats()
	s.content.requests.Store(0)
	s.content.bytes.Store(0)
	s.content.nanos.Store(0)
	s.content.dials.Store(0)
	for i := range contentTimeBuckets {
		s.content.timeCounts[i].Store(0)
		s.content.timeUsec[i].Store(0)
		s.content.timeBytes[i].Store(0)
	}
	if req.GetProfileTask() {
		profile := &bytes.Buffer{}
		if err := pprof.StartCPUProfile(profile); err != nil {
			log.Warningf("Could not profile the task: %s", err)
		} else {
			s.mu.Lock()
			s.taskProfile = profile
			s.mu.Unlock()
		}
	}

	counters := serverCounters()
	s.mu.Lock()
	s.taskCounters = counters
	s.mu.Unlock()
	rsp.PrepareStats = countersSince("prep_", counters, startCounters)
	rsp.TotalDurationUsec = time.Since(prepareStart).Microseconds()
	return rsp, nil
}

func (s *vfsServer) Finish(ctx context.Context, request *vmfspb.FinishRequest) (*vmfspb.FinishResponse, error) {
	s.mu.Lock()
	if s.cancelRemoteFunc != nil {
		s.cancelRemoteFunc()
	}
	mirror := s.mirror
	s.mirror = nil
	s.mu.Unlock()
	if mirror != nil {
		s.local.SetWriteMirror(nil)
		_ = mirror.Close()
	}

	err := s.vfs.FinishTask()
	if err != nil {
		return nil, err
	}

	// Input fetch errors in the guest server must fail the action, as they
	// do with the host server, since the command may have ignored them.
	rsp := &vmfspb.FinishResponse{
		ContentRequests: s.content.requests.Swap(0),
		ContentBytes:    s.content.bytes.Swap(0),
		ContentUsec:     s.content.nanos.Swap(0) / 1000,
		ContentDials:    s.content.dials.Swap(0),
	}
	for i := range contentTimeBuckets {
		rsp.ContentTimeCounts = append(rsp.ContentTimeCounts, s.content.timeCounts[i].Swap(0))
		rsp.ContentTimeUsec = append(rsp.ContentTimeUsec, s.content.timeUsec[i].Swap(0))
		rsp.ContentTimeBytes = append(rsp.ContentTimeBytes, s.content.timeBytes[i].Swap(0))
	}
	after := serverCounters()
	s.mu.Lock()
	profile := s.taskProfile
	s.taskProfile = nil
	before := s.taskCounters
	s.mu.Unlock()
	memory := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}, {Name: "/memory/classes/total:bytes"}}
	metrics.Read(memory)
	rsp.ServerStats = fmt.Sprintf("%s heap_kb=%d runtime_kb=%d threads=%d", countersSince("", after, before), memory[0].Value.Uint64()/1000, memory[1].Value.Uint64()/1000, pprof.Lookup("threadcreate").Count())
	if profile != nil {
		pprof.StopCPUProfile()
		rsp.TaskCpuProfile = profile.Bytes()
	}
	opStats := s.vfs.TakeOpStats()
	for _, op := range slices.Sorted(maps.Keys(opStats)) {
		rsp.FuseOpStats = append(rsp.FuseOpStats, &vmfspb.FuseOpStats{
			Op:           op,
			Count:        opStats[op].Count,
			DurationUsec: opStats[op].Duration.Microseconds(),
		})
	}
	if request.GetReportUnusedInputs() {
		rsp.UnusedInputs = s.local.UnusedInputs()
	}
	if err := s.local.TaskError(); err != nil {
		st := gstatus.Convert(err)
		rsp.TaskErrorCode = int32(st.Code())
		rsp.TaskErrorMessage = st.Message()
	}
	finishCounters := serverCounters()
	s.mu.Lock()
	s.finishCounters = finishCounters
	s.mu.Unlock()
	return rsp, nil
}

// ReadOutputs streams the entries under the task's output paths, after
// waiting for the host to apply the writes mirrored to it, if any.
func (s *vfsServer) ReadOutputs(req *vmfspb.ReadOutputsRequest, stream vmfspb.FileSystem_ReadOutputsServer) error {
	s.mu.Lock()
	layout := s.layout
	mirror := s.mirror
	s.mu.Unlock()
	if layout == nil {
		return status.FailedPreconditionError("guest VFS server is not prepared")
	}
	if mirror != nil {
		if err := mirror.Sync(); err != nil {
			return status.WrapError(err, "sync mirrored writes")
		}
	}
	buf := make([]byte, outputChunkSize)
	return s.local.WalkOutputs(layout, func(entry *vmfspb.OutputEntry, backingPath string) error {
		rsp := &vmfspb.ReadOutputsResponse{Entry: entry}
		if !req.GetSendContents() || backingPath == "" || entry.GetSize() == 0 {
			return stream.Send(rsp)
		}
		f, err := os.Open(backingPath)
		if err != nil {
			return err
		}
		defer f.Close()
		for remaining := entry.GetSize(); remaining > 0; {
			n, err := io.ReadFull(f, buf[:min(remaining, int64(len(buf)))])
			if err != nil {
				return status.WrapErrorf(err, "read output %q", entry.GetPath())
			}
			// gRPC encodes the message before Send returns, so buf can be
			// reused.
			rsp.Data = buf[:n]
			if err := stream.Send(rsp); err != nil {
				return err
			}
			rsp = &vmfspb.ReadOutputsResponse{}
			remaining -= int64(n)
		}
		return nil
	})
}

// UploadOutputs uploads the task's outputs to the cache through the host's
// proxy, reading them through the FUSE mount.
func (s *vfsServer) UploadOutputs(ctx context.Context, req *vmfspb.UploadOutputsRequest) (*vmfspb.UploadOutputsResponse, error) {
	s.mu.Lock()
	layout := s.layout
	conn := s.hostConn
	s.mu.Unlock()
	if layout == nil {
		return nil, status.FailedPreconditionError("guest VFS server is not prepared")
	}
	env := real_environment.NewBatchEnv()
	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	cmd := &repb.Command{
		OutputFiles:       layout.OutputFiles,
		OutputDirectories: layout.OutputDirectories,
		OutputPaths:       layout.OutputPaths,
	}
	rootDir := filepath.Join(mountDir, layout.WorkingDirectory)
	result := &repb.ActionResult{}
	txInfo, err := dirtools.UploadTree(ctx, env, dirtools.NewDirHelper(rootDir, cmd, 0755), layout.RemoteInstanceName, layout.DigestFunction, rootDir, cmd, result, false /*=addToFileCache*/, nil /*=chunkingParams*/)
	if err != nil {
		return nil, status.WrapError(err, "upload outputs")
	}
	return &vmfspb.UploadOutputsResponse{
		ActionResult:     result,
		FileCount:        txInfo.FileCount,
		BytesTransferred: txInfo.BytesTransferred,
	}, nil
}

// Run serves the guest VFS. writebackCache mounts it with the kernel's
// writeback cache.
func Run(writebackCache bool) error {
	ctx := context.Background()
	listener, err := vsock.NewGuestListener(ctx, vsock.VMVFSPort)
	if err != nil {
		return status.WrapError(err, "error listening on vsock port")
	}
	log.Infof("Starting VM VFS listener on vsock port: %d", vsock.VMVFSPort)
	server := grpc.NewServer()
	vmService, err := NewServer(writebackCache)
	if err != nil {
		return status.WrapError(err, "failed to start server")
	}
	vmfspb.RegisterFileSystemServer(server, vmService)
	if err := server.Serve(listener); err != nil {
		return status.WrapError(err, "failed to serve")
	}
	return nil
}
