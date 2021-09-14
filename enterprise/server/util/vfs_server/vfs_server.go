package vfs_server

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/dirtools"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/hanwen/go-fuse/v2/fs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	vfspb "github.com/buildbuddy-io/buildbuddy/proto/vfs"
	gstatus "google.golang.org/grpc/status"
)

type fileHandle struct {
	mu        sync.Mutex
	f         *os.File
	openFlags uint32
}

type LazyFileProvider interface {
	Place(relPath, fullPath string) error
}

type CASLazyFileProvider struct {
	env                environment.Env
	ctx                context.Context
	remoteInstanceName string
	inputFiles         map[string]*repb.FileNode
}

func NewCASLazyFileProvider(env environment.Env, ctx context.Context, remoteInstanceName string, inputFiles map[string]*repb.FileNode) *CASLazyFileProvider {
	return &CASLazyFileProvider{
		env:                env,
		ctx:                ctx,
		remoteInstanceName: remoteInstanceName,
		inputFiles:         inputFiles,
	}
}

func (p *CASLazyFileProvider) Place(relPath, fullPath string) error {
	fileNode, ok := p.inputFiles[relPath]
	if !ok {
		return status.NotFoundErrorf("unknown file %q", relPath)
	}
	ff := dirtools.NewBatchFileFetcher(p.ctx, p.remoteInstanceName, p.env.GetFileCache(), p.env.GetByteStreamClient(), p.env.GetContentAddressableStorageClient())
	fileMap := dirtools.FileMap{
		digest.NewKey(fileNode.GetDigest()): {&dirtools.FilePointer{
			FullPath:     fullPath,
			RelativePath: relPath,
			FileNode:     fileNode,
		}},
	}
	if err := ff.FetchFiles(fileMap, &dirtools.DownloadTreeOpts{}); err != nil {
		return err
	}
	return nil
}

type Server struct {
	env           environment.Env
	workspacePath string

	server *grpc.Server

	mu                 sync.Mutex
	context            context.Context
	remoteInstanceName string
	lazyFiles          map[string]struct{}
	lazyFileProvider   LazyFileProvider
	nextHandleID       uint64
	fileHandles        map[uint64]*fileHandle
}

func New(env environment.Env, workspacePath string) *Server {
	return &Server{
		env:           env,
		workspacePath: workspacePath,
		lazyFiles:     make(map[string]struct{}),
		fileHandles:   make(map[uint64]*fileHandle),
		nextHandleID:  1,
	}
}

func (p *Server) Prepare(lazyFiles []string, lazyFileProvider LazyFileProvider) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lazyFileProvider = lazyFileProvider

	dirsToMake := make(map[string]struct{})
	for _, lf := range lazyFiles {
		p.lazyFiles[lf] = struct{}{}
		dirsToMake[filepath.Dir(lf)] = struct{}{}
	}

	for dir := range dirsToMake {
		dir := filepath.Join(p.workspacePath, dir)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return nil
}

func (p *Server) computeFullPath(relativePath string) (string, error) {
	fullPath := filepath.Clean(filepath.Join(p.workspacePath, relativePath))
	if !strings.HasPrefix(fullPath, p.workspacePath) {
		return "", status.PermissionDeniedError("open request outside of workspace")
	}
	return fullPath, nil
}

func syscallErrProto(sysErr error) *vfspb.SyscallError {
	errno := fs.ToErrno(sysErr)
	return &vfspb.SyscallError{Errno: uint32(errno)}
}

// syscallErrStatus creates a gRPC status error that includes the given syscall error code.
func syscallErrStatus(sysErr error) error {
	s := gstatus.New(codes.Unknown, fmt.Sprintf("syscall error: %s", sysErr))
	s, err := s.WithDetails(syscallErrProto(sysErr))
	// should never happen
	if err != nil {
		alert.UnexpectedEvent("could_not_make_syscall_err", "err: %s", err)
	}
	return s.Err()
}

func (h *fileHandle) open(fullPath string, req *vfspb.OpenRequest) (*vfspb.OpenResponse, error) {
	f, err := os.OpenFile(fullPath, int(req.GetFlags()), os.FileMode(req.GetMode()))
	if err != nil {
		return nil, syscallErrStatus(err)
	}
	h.f = f
	h.openFlags = req.GetFlags()
	return &vfspb.OpenResponse{}, nil
}

// TODO(vadim): investigate if we can allow parallel reads
func (h *fileHandle) read(req *vfspb.ReadRequest) (*vfspb.ReadResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	buf := make([]byte, req.GetNumBytes())
	n, err := h.f.ReadAt(buf, req.GetOffset())
	if err != nil && err != io.EOF {
		return nil, syscallErrStatus(err)
	}
	return &vfspb.ReadResponse{Data: buf[:n]}, nil
}

func (h *fileHandle) write(req *vfspb.WriteRequest) (*vfspb.WriteResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	var n int
	var err error
	if int(h.openFlags)&os.O_APPEND != 0 {
		n, err = h.f.Write(req.GetData())
	} else {
		n, err = h.f.WriteAt(req.GetData(), req.GetOffset())
	}

	if err != nil {
		return nil, syscallErrStatus(err)
	}
	return &vfspb.WriteResponse{NumBytes: uint32(n)}, nil
}

func (h *fileHandle) fsync(req *vfspb.FsyncRequest) (*vfspb.FsyncResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if err := h.f.Sync(); err != nil {
		return nil, syscallErrStatus(err)
	}
	return &vfspb.FsyncResponse{}, nil
}

func (h *fileHandle) flush(req *vfspb.FlushRequest) (*vfspb.FlushResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Flush may be called more than once for the same handle if the user duplicated the FD.
	// We can't close our local FD until all the user FDs are closed.

	fd, err := syscall.Dup(int(h.f.Fd()))
	if err != nil {
		return nil, syscallErrStatus(err)
	}
	if err := syscall.Close(fd); err != nil {
		return nil, syscallErrStatus(err)
	}
	return &vfspb.FlushResponse{}, nil
}

func (h *fileHandle) release(req *vfspb.ReleaseRequest) (*vfspb.ReleaseResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if err := h.f.Close(); err != nil {
		return nil, syscallErrStatus(err)
	}
	h.f = nil
	return &vfspb.ReleaseResponse{}, nil
}

func (p *Server) Open(ctx context.Context, request *vfspb.OpenRequest) (*vfspb.OpenResponse, error) {
	p.mu.Lock()
	ctx = p.context
	p.mu.Unlock()

	fullPath, err := p.computeFullPath(request.GetPath())
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	_, isLazyFile := p.lazyFiles[request.GetPath()]
	p.mu.Unlock()
	if isLazyFile {
		err := p.lazyFileProvider.Place(request.GetPath(), fullPath)
		if err != nil {
			return nil, err
		}
		p.mu.Lock()
		delete(p.lazyFiles, request.GetPath())
		p.mu.Unlock()
	}

	fh := &fileHandle{}
	rsp, err := fh.open(fullPath, request)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	handleID := p.nextHandleID
	rsp.HandleId = handleID
	p.nextHandleID++
	p.fileHandles[handleID] = fh
	p.mu.Unlock()
	return rsp, nil
}

func (p *Server) getFileHandle(id uint64) (*fileHandle, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fh, ok := p.fileHandles[id]
	if !ok {
		return nil, status.NotFoundErrorf("file handle %d not found", id)
	}
	return fh, nil
}

func (p *Server) Allocate(ctx context.Context, request *vfspb.AllocateRequest) (*vfspb.AllocateResponse, error) {
	fh, err := p.getFileHandle(request.GetHandleId())
	if err != nil {
		return nil, err
	}
	return fh.allocate(request)
}

func (p *Server) Read(ctx context.Context, request *vfspb.ReadRequest) (*vfspb.ReadResponse, error) {
	fh, err := p.getFileHandle(request.GetHandleId())
	if err != nil {
		return nil, err
	}
	return fh.read(request)
}

func (p *Server) Write(ctx context.Context, request *vfspb.WriteRequest) (*vfspb.WriteResponse, error) {
	fh, err := p.getFileHandle(request.GetHandleId())
	if err != nil {
		return nil, err
	}
	return fh.write(request)
}

func (p *Server) Fsync(ctx context.Context, request *vfspb.FsyncRequest) (*vfspb.FsyncResponse, error) {
	fh, err := p.getFileHandle(request.GetHandleId())
	if err != nil {
		return nil, err
	}
	return fh.fsync(request)
}

func (p *Server) Flush(ctx context.Context, request *vfspb.FlushRequest) (*vfspb.FlushResponse, error) {
	fh, err := p.getFileHandle(request.GetHandleId())
	if err != nil {
		return nil, err
	}
	return fh.flush(request)
}

func (p *Server) Release(ctx context.Context, request *vfspb.ReleaseRequest) (*vfspb.ReleaseResponse, error) {
	fh, err := p.getFileHandle(request.GetHandleId())
	if err != nil {
		return nil, err
	}
	rsp, err := fh.release(request)
	p.mu.Lock()
	delete(p.fileHandles, request.GetHandleId())
	p.mu.Unlock()
	return rsp, err
}

func (p *Server) getAttr(fullPath string) (*vfspb.Attrs, error) {
	fi, err := os.Stat(fullPath)
	if err != nil {
		return nil, syscallErrStatus(err)
	}
	return &vfspb.Attrs{
		Size: fi.Size(),
		Perm: uint32(fi.Mode().Perm()),
	}, nil
}

func (p *Server) GetAttr(ctx context.Context, request *vfspb.GetAttrRequest) (*vfspb.GetAttrResponse, error) {
	fullPath, err := p.computeFullPath(request.GetPath())
	if err != nil {
		return nil, err
	}

	attrs, err := p.getAttr(fullPath)
	if err != nil {
		return nil, err
	}
	return &vfspb.GetAttrResponse{Attrs: attrs}, nil
}

func (p *Server) SetAttr(ctx context.Context, request *vfspb.SetAttrRequest) (*vfspb.SetAttrResponse, error) {
	fullPath, err := p.computeFullPath(request.GetPath())
	if err != nil {
		return nil, err
	}

	if request.SetPerms != nil {
		if err := os.Chmod(fullPath, os.FileMode(request.SetPerms.Perms)); err != nil {
			return nil, syscallErrStatus(err)
		}
	}

	if request.SetSize != nil {
		if err := os.Truncate(fullPath, request.SetSize.GetSize()); err != nil {
			return nil, syscallErrStatus(err)
		}
	}

	attrs, err := p.getAttr(fullPath)
	if err != nil {
		return nil, err
	}
	return &vfspb.SetAttrResponse{Attrs: attrs}, nil
}

func (p *Server) Rename(ctx context.Context, request *vfspb.RenameRequest) (*vfspb.RenameResponse, error) {
	oldFullPath, err := p.computeFullPath(request.GetOldPath())
	if err != nil {
		return nil, err
	}
	oldNewPath, err := p.computeFullPath(request.GetNewPath())
	if err != nil {
		return nil, err
	}

	if err := os.Rename(oldFullPath, oldNewPath); err != nil {
		return nil, syscallErrStatus(err)
	}

	return &vfspb.RenameResponse{}, nil
}

func (p *Server) Mkdir(ctx context.Context, request *vfspb.MkdirRequest) (*vfspb.MkdirResponse, error) {
	fullPath, err := p.computeFullPath(request.GetPath())
	if err != nil {
		return nil, err
	}
	if err := os.Mkdir(fullPath, os.FileMode(request.GetPerms())); err != nil {
		return nil, syscallErrStatus(err)
	}
	return &vfspb.MkdirResponse{}, nil
}

func (p *Server) Rmdir(ctx context.Context, request *vfspb.RmdirRequest) (*vfspb.RmdirResponse, error) {
	fullPath, err := p.computeFullPath(request.GetPath())
	if err != nil {
		return nil, err
	}
	if err := os.Remove(fullPath); err != nil {
		return nil, syscallErrStatus(err)
	}
	return &vfspb.RmdirResponse{}, nil
}

func (p *Server) Symlink(ctx context.Context, request *vfspb.SymlinkRequest) (*vfspb.SymlinkResponse, error) {
	fullPath, err := p.computeFullPath(request.GetPath())
	if err != nil {
		return nil, err
	}

	target := request.GetTarget()

	var targetFullPath string
	// If the symlink target is an absolute path, rewrite it with the real location in the workspace.
	if strings.HasPrefix(target, "/") {
		target = filepath.Join(p.workspacePath, target)
		targetFullPath = target
	} else {
		targetFullPath = filepath.Join(filepath.Dir(fullPath), target)
	}

	// Check that nothing sneaky is going on.
	// TODO(vadim): check if you could still do something shady by moving a relative symlink to a higher directory
	if !strings.HasPrefix(filepath.Clean(targetFullPath), p.workspacePath) {
		return nil, status.PermissionDeniedError("symlink target outside of workspace")
	}

	if err := os.Symlink(target, fullPath); err != nil {
		return nil, syscallErrStatus(err)
	}
	return &vfspb.SymlinkResponse{}, nil
}

func (p *Server) Unlink(ctx context.Context, request *vfspb.UnlinkRequest) (*vfspb.UnlinkResponse, error) {
	fullPath, err := p.computeFullPath(request.GetPath())
	if err != nil {
		return nil, err
	}
	if err := os.Remove(fullPath); err != nil {
		return nil, syscallErrStatus(err)
	}
	return &vfspb.UnlinkResponse{}, nil
}

func (p *Server) Start(lis net.Listener) error {
	p.server = grpc.NewServer()
	vfspb.RegisterFileSystemServer(p.server, p)
	go func() {
		_ = p.server.Serve(lis)
	}()
	return nil
}

func (p *Server) Stop() {
	p.mu.Lock()
	for _, fh := range p.fileHandles {
		_, _ = fh.release(&vfspb.ReleaseRequest{})
	}
	p.mu.Unlock()
	p.server.Stop()
}

func (p *Server) SetExecutionContext(ctx context.Context) {
	p.mu.Lock()
	p.context = ctx
	p.mu.Unlock()
}
