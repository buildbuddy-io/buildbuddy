package vmvfs

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"sync"
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
}

// contentClient makes raw content protocol requests to the host over a pool
// of vsock connections, each carrying a request at a time.
type contentClient struct {
	mu   sync.Mutex
	idle []*contentConn
}

type contentConn struct {
	net.Conn
	r *bufio.Reader
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
	nc, err := libVsock.Dial(libVsock.Host, vsock.HostVFSContentPort, &libVsock.Config{})
	if err != nil {
		return nil, false, status.UnavailableErrorf("dial host content server: %s", err)
	}
	return &contentConn{Conn: nc, r: bufio.NewReaderSize(nc, 64*1024)}, false, nil
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
	for attempt := 0; ; attempt++ {
		conn, reused, err := c.get()
		if err != nil {
			return err
		}
		// Unblock the reads and writes below when ctx is cancelled.
		stop := context.AfterFunc(ctx, func() { conn.SetDeadline(time.Unix(1, 0)) })
		consumed := false
		err = vfs_server.WriteContentRequest(conn, node, offset, length)
		if err == nil {
			var n int64
			n, err = vfs_server.ReadContentResponseHeader(conn.r)
			if err == nil {
				consumed = true
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

func (s *vfsServer) Prepare(ctx context.Context, req *vmfspb.PrepareRequest) (*vmfspb.PrepareResponse, error) {
	conn, err := dialHost(ctx, true)
	if err != nil {
		return nil, status.WrapError(err, "redial host VFS server")
	}

	// This is the context that is used to make RPCs to the host.
	// It needs to stay alive as long as there's an active command on the VM.
	rpcCtx, cancel := context.WithCancel(context.Background())
	client := vfspb.FileSystemClient(vfspb.NewFileSystemClient(conn))
	var invalidations *vfscommon.InodeInvalidations
	var layout *container.FileSystemLayout
	var mirror *vfs_server.MirrorClient
	rsp := &vmfspb.PrepareResponse{}
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
		mode := req.GetContentMode()
		if mode == "" {
			mode = vfs_server.GuestContentRemote
		}
		var content *contentClient
		switch mode {
		case vfs_server.GuestContentRemote, vfs_server.GuestContentFileCache:
		case vfs_server.GuestContentRaw, vfs_server.GuestContentDirect:
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

	if err := s.vfs.PrepareForTask(rpcCtx, "fc", invalidations); err != nil {
		return nil, err
	}

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
	rsp := &vmfspb.FinishResponse{}
	if request.GetReportUnusedInputs() {
		rsp.UnusedInputs = s.local.UnusedInputs()
	}
	if err := s.local.TaskError(); err != nil {
		st := gstatus.Convert(err)
		rsp.TaskErrorCode = int32(st.Code())
		rsp.TaskErrorMessage = st.Message()
	}
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
