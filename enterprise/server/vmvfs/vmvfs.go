package vmvfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vfs"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfs_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfscommon"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/cache/dirtools"
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
)

const (
	// this should match guestVFSMountDir in firecracker.go
	mountDir       = "/vfs"
	scratchDir     = "/tmp/vmvfs-scratch"
	inputCacheDir  = "/tmp/vmvfs-inputs"
	inputChunkSize = 128 * 1024
)

type vfsServer struct {
	vfs        *vfs.VFS
	local      *vfs_server.Server
	inputStore *remoteInputStore
	hostConn   *grpc.ClientConn

	mu sync.Mutex
	// Context & cancel func for VFS RPCs.
	cancelRemoteFunc context.CancelFunc
}

type remoteInputStore struct {
	cacheDir string
	fetches  singleflight.Group[string, struct{}]

	mu                 sync.RWMutex
	client             bspb.ByteStreamClient
	remoteInstanceName string
	digestFunction     repb.DigestFunction_Value
	cachedRanges       map[string]struct{}
}

func (s *remoteInputStore) configure(client bspb.ByteStreamClient, remoteInstanceName string, digestFunction repb.DigestFunction_Value) {
	s.mu.Lock()
	s.client = client
	s.remoteInstanceName = remoteInstanceName
	s.digestFunction = digestFunction
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
		} else {
			rn := digest.NewCASResourceName(node.GetDigest(), remoteInstanceName, digestFunction)
			rn.SetCompressor(repb.Compressor_ZSTD)
			err = cachetools.GetBlob(ctx, client, rn, tmp)
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
			rn := digest.NewCASResourceName(node.GetDigest(), s.remoteInstanceName, s.digestFunction)
			s.mu.RUnlock()
			if cached {
				return struct{}{}, nil
			}
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			limit := min(int64(inputChunkSize), size-start)
			stream, err := client.Read(ctx, &bspb.ReadRequest{
				ResourceName: rn.DownloadString(), ReadOffset: start, ReadLimit: limit,
			})
			if err != nil {
				return struct{}{}, err
			}
			data := make([]byte, 0, limit)
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

func NewServer() (*vfsServer, error) {
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

	fs := vfs.New(vfsClient, mountDir, &vfs.Options{})
	if err := fs.Mount(); err != nil {
		return nil, status.InternalErrorf("Could not mount VFS: %s", err)
	}

	return &vfsServer{
		vfs:        fs,
		local:      local,
		inputStore: inputStore,
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
		s.inputStore.configure(bspb.NewByteStreamClient(conn), req.GetRemoteInstanceName(), req.GetDigestFunction())
		layout := &container.FileSystemLayout{
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
		if req.GetProfilePreparation() {
			pprof.StopCPUProfile()
			rsp.PreparationCpuProfile = cpuProfile.Bytes()
		}
		client = vfs_server.NewDirectClient(s.local)
	}
	s.mu.Lock()
	oldConn := s.hostConn
	oldCancel := s.cancelRemoteFunc
	s.hostConn = conn
	s.vfs.SetClient(client)
	s.cancelRemoteFunc = cancel
	s.mu.Unlock()
	if oldCancel != nil {
		oldCancel()
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
	s.mu.Unlock()

	err := s.vfs.FinishTask()
	if err != nil {
		return nil, err
	}

	return &vmfspb.FinishResponse{}, nil
}

func Run() error {
	ctx := context.Background()
	listener, err := vsock.NewGuestListener(ctx, vsock.VMVFSPort)
	if err != nil {
		return status.WrapError(err, "error listening on vsock port")
	}
	log.Infof("Starting VM VFS listener on vsock port: %d", vsock.VMVFSPort)
	server := grpc.NewServer()
	vmService, err := NewServer()
	if err != nil {
		return status.WrapError(err, "failed to start server")
	}
	vmfspb.RegisterFileSystemServer(server, vmService)
	if err := server.Serve(listener); err != nil {
		return status.WrapError(err, "failed to serve")
	}
	return nil
}
