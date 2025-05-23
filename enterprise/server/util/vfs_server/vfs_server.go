//go:build linux && !android

package vfs_server

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/server/cache/dirtools"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	vfspb "github.com/buildbuddy-io/buildbuddy/proto/vfs"
	fusefs "github.com/hanwen/go-fuse/v2/fs"
	gstatus "google.golang.org/grpc/status"
)

// Fuse operations
const (
	_OFD_GETLK  = 36
	_OFD_SETLK  = 37
	_OFD_SETLKW = 38

	// This is the total (arbitrary) size we report via statfs.
	// We don't actually enforce this is a limit.
	reportedSizeBytes = 20 * 1024 * 1024 * 1024
)

var (
	syscallStatusENOENT    = makeSyscallErrStatus(syscall.ENOENT)
	syscallStatusEPERM     = makeSyscallErrStatus(syscall.EPERM)
	syscallStatusEEXIST    = makeSyscallErrStatus(syscall.EEXIST)
	syscallStatusENOTEMPTY = makeSyscallErrStatus(syscall.ENOTEMPTY)
	syscallStatusEINVAL    = makeSyscallErrStatus(syscall.EINVAL)
)

// makeSyscallErrStatus creates a gRPC status error that includes the given syscall error code.
func makeSyscallErrStatus(sysErr error) error {
	s := gstatus.New(codes.Unknown, fmt.Sprintf("syscall error: %s", sysErr))
	s, err := s.WithDetails(syscallErrProto(sysErr))
	// should never happen
	if err != nil {
		alert.UnexpectedEvent("could_not_make_syscall_err", "err: %s", err)
	}
	return s.Err()
}

// syscallErrStatus returns a gRPC status error that includes the given syscall error code.
func syscallErrStatus(sysErr error) error {
	// Return predefined types for common errors.
	if errno, ok := sysErr.(syscall.Errno); ok {
		switch errno {
		case syscall.ENOENT:
			return syscallStatusENOENT
		case syscall.EPERM:
			return syscallStatusEPERM
		case syscall.EEXIST:
			return syscallStatusEEXIST
		case syscall.ENOTEMPTY:
			return syscallStatusENOTEMPTY
		case syscall.EINVAL:
			return syscallStatusEINVAL
		}
	}
	return makeSyscallErrStatus(sysErr)
}

type fileHandle struct {
	node *fsNode

	mu        sync.Mutex
	f         *os.File
	lockFD    int
	openFlags uint32
}

type DirectClient struct {
	s *Server
}

// NewDirectClient returns a client that makes direct function calls to the
// VFS server, bypassing gRPC.
func NewDirectClient(s *Server) *DirectClient {
	return &DirectClient{s}
}

func (dc *DirectClient) GetDirectoryContents(ctx context.Context, in *vfspb.GetDirectoryContentsRequest, opts ...grpc.CallOption) (*vfspb.GetDirectoryContentsResponse, error) {
	return dc.s.GetDirectoryContents(ctx, in)
}

func (dc *DirectClient) Lookup(ctx context.Context, in *vfspb.LookupRequest, opts ...grpc.CallOption) (*vfspb.LookupResponse, error) {
	return dc.s.Lookup(ctx, in)
}

func (dc *DirectClient) Mknod(ctx context.Context, in *vfspb.MknodRequest, opts ...grpc.CallOption) (*vfspb.MknodResponse, error) {
	return dc.s.Mknod(ctx, in)
}

func (dc *DirectClient) Create(ctx context.Context, in *vfspb.CreateRequest, opts ...grpc.CallOption) (*vfspb.CreateResponse, error) {
	return dc.s.Create(ctx, in)
}

func (dc *DirectClient) Open(ctx context.Context, in *vfspb.OpenRequest, opts ...grpc.CallOption) (*vfspb.OpenResponse, error) {
	return dc.s.Open(ctx, in)
}

func (dc *DirectClient) Allocate(ctx context.Context, in *vfspb.AllocateRequest, opts ...grpc.CallOption) (*vfspb.AllocateResponse, error) {
	return dc.s.Allocate(ctx, in)
}

func (dc *DirectClient) Read(ctx context.Context, in *vfspb.ReadRequest, opts ...grpc.CallOption) (*vfspb.ReadResponse, error) {
	return dc.s.Read(ctx, in)
}

func (dc *DirectClient) Write(ctx context.Context, in *vfspb.WriteRequest, opts ...grpc.CallOption) (*vfspb.WriteResponse, error) {
	return dc.s.Write(ctx, in)
}

func (dc *DirectClient) Lseek(ctx context.Context, in *vfspb.LseekRequest, opts ...grpc.CallOption) (*vfspb.LseekResponse, error) {
	return dc.s.Lseek(ctx, in)
}

func (dc *DirectClient) Fsync(ctx context.Context, in *vfspb.FsyncRequest, opts ...grpc.CallOption) (*vfspb.FsyncResponse, error) {
	return dc.s.Fsync(ctx, in)
}

func (dc *DirectClient) Flush(ctx context.Context, in *vfspb.FlushRequest, opts ...grpc.CallOption) (*vfspb.FlushResponse, error) {
	return dc.s.Flush(ctx, in)
}

func (dc *DirectClient) Release(ctx context.Context, in *vfspb.ReleaseRequest, opts ...grpc.CallOption) (*vfspb.ReleaseResponse, error) {
	return dc.s.Release(ctx, in)
}

func (dc *DirectClient) CopyFileRange(ctx context.Context, in *vfspb.CopyFileRangeRequest, opts ...grpc.CallOption) (*vfspb.CopyFileRangeResponse, error) {
	return dc.s.CopyFileRange(ctx, in)
}

func (dc *DirectClient) GetAttr(ctx context.Context, in *vfspb.GetAttrRequest, opts ...grpc.CallOption) (*vfspb.GetAttrResponse, error) {
	return dc.s.GetAttr(ctx, in)
}

func (dc *DirectClient) SetAttr(ctx context.Context, in *vfspb.SetAttrRequest, opts ...grpc.CallOption) (*vfspb.SetAttrResponse, error) {
	return dc.s.SetAttr(ctx, in)
}

func (dc *DirectClient) Rename(ctx context.Context, in *vfspb.RenameRequest, opts ...grpc.CallOption) (*vfspb.RenameResponse, error) {
	return dc.s.Rename(ctx, in)
}

func (dc *DirectClient) Mkdir(ctx context.Context, in *vfspb.MkdirRequest, opts ...grpc.CallOption) (*vfspb.MkdirResponse, error) {
	return dc.s.Mkdir(ctx, in)
}

func (dc *DirectClient) Rmdir(ctx context.Context, in *vfspb.RmdirRequest, opts ...grpc.CallOption) (*vfspb.RmdirResponse, error) {
	return dc.s.Rmdir(ctx, in)
}

func (dc *DirectClient) Link(ctx context.Context, in *vfspb.LinkRequest, opts ...grpc.CallOption) (*vfspb.LinkResponse, error) {
	return dc.s.Link(ctx, in)
}

func (dc *DirectClient) Symlink(ctx context.Context, in *vfspb.SymlinkRequest, opts ...grpc.CallOption) (*vfspb.SymlinkResponse, error) {
	return dc.s.Symlink(ctx, in)
}

func (dc *DirectClient) Unlink(ctx context.Context, in *vfspb.UnlinkRequest, opts ...grpc.CallOption) (*vfspb.UnlinkResponse, error) {
	return dc.s.Unlink(ctx, in)
}

func (dc *DirectClient) GetLk(ctx context.Context, in *vfspb.GetLkRequest, opts ...grpc.CallOption) (*vfspb.GetLkResponse, error) {
	return dc.s.GetLk(ctx, in)
}

func (dc *DirectClient) SetLk(ctx context.Context, in *vfspb.SetLkRequest, opts ...grpc.CallOption) (*vfspb.SetLkResponse, error) {
	return dc.s.SetLk(ctx, in)
}

func (dc *DirectClient) SetLkw(ctx context.Context, in *vfspb.SetLkRequest, opts ...grpc.CallOption) (*vfspb.SetLkResponse, error) {
	return dc.s.SetLkw(ctx, in)
}

func (dc *DirectClient) Statfs(ctx context.Context, in *vfspb.StatfsRequest, opts ...grpc.CallOption) (*vfspb.StatfsResponse, error) {
	return dc.s.Statfs(ctx, in)
}

const (
	fsDirectoryNode = iota
	fsFileNode
	fsSymlinkNode
	fsCharDevNode
)

type fsNode struct {
	server   *Server
	id       uint64
	nodeType byte
	fileNode *repb.FileNode
	target   string

	mu          sync.Mutex
	accessed    bool
	attrs       *vfspb.Attrs
	name        string
	parent      *fsNode
	children    map[string]*fsNode
	backingPath string
}

func (fsn *fsNode) IsDirectory() bool {
	return fsn.nodeType == fsDirectoryNode
}

func (fsn *fsNode) IsFile() bool {
	return fsn.nodeType == fsFileNode
}

func (fsn *fsNode) IsSymlink() bool {
	return fsn.nodeType == fsSymlinkNode
}

func (fsn *fsNode) mode() uint32 {
	switch fsn.nodeType {
	case fsFileNode:
		return syscall.S_IFREG
	case fsDirectoryNode:
		return syscall.S_IFDIR
	case fsSymlinkNode:
		return syscall.S_IFLNK
	case fsCharDevNode:
		return syscall.S_IFCHR
	}
	return 0
}

func updateAttr(attr *vfspb.Attrs, mod func(attr *vfspb.Attrs)) *vfspb.Attrs {
	newAttrs := proto.Clone(attr).(*vfspb.Attrs)
	mod(newAttrs)
	return newAttrs
}

// statBlocksToFSBlocks converts block count from 512-byte blocks (as reported
// by Stat) to block count based on the filesystem block size.
func (p *Server) statBlocksToFSBlocks(statBlocks int64) int64 {
	statBlocksPerFSBlock := p.backingBlockSize / 512
	return statBlocks / statBlocksPerFSBlock
}

func (fsn *fsNode) refreshAttrs() error {
	fsn.mu.Lock()
	defer fsn.mu.Unlock()
	if fsn.backingPath == "" {
		return nil
	}
	fi, err := os.Stat(fsn.backingPath)
	if err != nil {
		return syscallErrStatus(err)
	}
	oldBlocks := fsn.attrs.Blocks
	fsn.attrs = updateAttr(fsn.attrs, func(attr *vfspb.Attrs) {
		attr.Size = fi.Size()
		attr.MtimeNanos = uint64(fi.ModTime().UnixNano())
		rawStat := fi.Sys().(*syscall.Stat_t)
		atime := rawStat.Atim
		attr.AtimeNanos = uint64(atime.Nsec + atime.Sec*1e9)
		attr.Blocks = rawStat.Blocks
		// This is an int32 on ARM for some reason, hence the seemingly weird
		// cast.
		attr.BlockSize = int64(rawStat.Blksize)
	})
	fsn.server.mu.Lock()
	fsn.server.blocks += fsn.server.statBlocksToFSBlocks(fsn.attrs.Blocks - oldBlocks)
	fsn.server.mu.Unlock()
	return nil
}

func (fsn *fsNode) Path() string {
	n := fsn
	var segments []string
	for n != nil {
		segments = append(segments, n.name)
		n = n.parent
	}
	slices.Reverse(segments)
	return filepath.Join(segments...)
}

func newDirNode(parent *fsNode, name string) *fsNode {
	return &fsNode{
		nodeType: fsDirectoryNode,
		name:     name,
		attrs: &vfspb.Attrs{
			Size: 1000,
			Perm: uint32(0755),
		},
		parent: parent,
	}
}

func newCASFileNode(parent *fsNode, refn *repb.FileNode) *fsNode {
	perms := 0644
	if refn.IsExecutable {
		perms |= 0111
	}
	now := time.Now()
	return &fsNode{
		nodeType: fsFileNode,
		name:     refn.GetName(),
		attrs: &vfspb.Attrs{
			Size:       refn.GetDigest().GetSizeBytes(),
			Perm:       uint32(perms),
			Immutable:  true,
			Nlink:      1,
			MtimeNanos: uint64(now.UnixNano()),
			AtimeNanos: uint64(now.UnixNano()),
		},
		fileNode: refn,
		parent:   parent,
	}
}

func newSymlinkNode(parent *fsNode, name string, target string) *fsNode {
	return &fsNode{
		nodeType: fsSymlinkNode,
		name:     name,
		attrs: &vfspb.Attrs{
			Size: 1000,
			Perm: uint32(0644),
		},
		parent: parent,
		target: target,
	}
}

type Server struct {
	env              environment.Env
	workspacePath    string
	backingBlockSize int64

	server *grpc.Server

	nextNodeID atomic.Uint64

	mu                 sync.Mutex
	blocks             int64
	nextId             uint64
	nodes              map[uint64]*fsNode
	internalTaskCtx    context.Context
	root               *fsNode
	casFetcher         *casFetcher
	remoteInstanceName string
	fileHandles        map[uint64]*fileHandle

	// total number of CAS files in the tree.
	casFileCount int64
	// total size of CAS files in the tree.
	casFileSizeBytes int64
}

func New(env environment.Env, workspacePath string) (*Server, error) {
	rootNode := newDirNode(nil, "")
	nodes := make(map[uint64]*fsNode)
	nodes[0] = rootNode

	fs := unix.Statfs_t{}
	err := unix.Statfs(workspacePath, &fs)
	if err != nil {
		return nil, err
	}

	return &Server{
		env:              env,
		workspacePath:    workspacePath,
		backingBlockSize: fs.Bsize,
		fileHandles:      make(map[uint64]*fileHandle),
		root:             rootNode,
		nodes:            nodes,
	}, nil
}

func (p *Server) Path() string {
	return p.workspacePath
}

func (p *Server) generateScratchPath(name string) (string, error) {
	randStr, err := random.RandomString(10)
	if err != nil {
		return "", err
	}
	return filepath.Join(p.workspacePath, name+"."+randStr), nil
}

func (p *Server) taskCtx() context.Context {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.internalTaskCtx
}

func (p *Server) addNode(node *fsNode) uint64 {
	id := atomic.AddUint64(&p.nextId, 1)
	node.server = p
	node.id = id
	p.mu.Lock()
	p.nodes[id] = node
	if node.fileNode != nil {
		p.casFileCount++
		p.casFileSizeBytes += node.fileNode.GetDigest().GetSizeBytes()
	}
	p.mu.Unlock()
	return id
}

func (p *Server) updateLayout(ctx context.Context, inputTree *repb.Tree, digestFunction repb.DigestFunction_Value) error {
	_, dirMap, err := dirtools.DirMapFromTree(inputTree, digestFunction)
	if err != nil {
		return err
	}
	numDirs := 0
	numFiles := 0
	numSymlinks := 0

	p.root.mu.Lock()
	defer p.root.mu.Unlock()

	var walkDir func(dir *repb.Directory, parentNode *fsNode) error
	walkDir = func(dir *repb.Directory, parentNode *fsNode) error {
		numDirs++
		if parentNode.children == nil && (len(dir.GetDirectories()) > 0 || len(dir.GetFiles()) > 0 || len(dir.GetSymlinks()) > 0) {
			parentNode.children = make(map[string]*fsNode)
		}
		for _, childDirNode := range dir.GetDirectories() {
			childDir, ok := dirMap[digest.NewKey(childDirNode.Digest)]
			if !ok {
				if !digest.IsEmptyHash(childDirNode.Digest, digestFunction) {
					return status.NotFoundErrorf("could not find dir %q", childDirNode.Digest)
				}
				childDir = &repb.Directory{}
			}

			subNode := parentNode.children[childDirNode.GetName()]
			if subNode == nil || !subNode.IsDirectory() {
				subNode = newDirNode(parentNode, childDirNode.GetName())
				p.addNode(subNode)
				parentNode.children[childDirNode.Name] = subNode
			}
			if err := walkDir(childDir, subNode); err != nil {
				return err
			}
		}
		for _, childFileNode := range dir.GetFiles() {
			fileNode := newCASFileNode(parentNode, childFileNode)
			p.addNode(fileNode)
			parentNode.children[childFileNode.Name] = fileNode
			numFiles++
		}
		for _, childSymlink := range dir.GetSymlinks() {
			symlinkNode := newSymlinkNode(parentNode, childSymlink.GetName(), childSymlink.GetTarget())
			p.addNode(symlinkNode)
			parentNode.children[childSymlink.Name] = symlinkNode
			numSymlinks++
		}
		return nil
	}

	err = walkDir(inputTree.Root, p.root)
	if err != nil {
		return err
	}

	log.CtxDebugf(ctx, "VFS contains %d directories, %d files and %d symlinks", numDirs, numFiles, numSymlinks)

	return nil
}

func (p *Server) ComputeStats() *repb.VfsStats {
	stats := &repb.VfsStats{
		CasFilesCount:     p.casFileCount,
		CasFilesSizeBytes: p.casFileSizeBytes,
	}

	p.mu.Lock()
	var walkNode func(node *fsNode)
	walkNode = func(node *fsNode) {
		node.mu.Lock()
		if node.fileNode != nil && node.accessed {
			stats.CasFilesAccessedCount++
			stats.CasFilesAccessedBytes += node.fileNode.GetDigest().GetSizeBytes()
		}
		children := node.children
		node.mu.Unlock()
		for _, child := range children {
			walkNode(child)
		}
	}
	walkNode(p.root)
	p.mu.Unlock()

	p.casFetcher.UpdateIOStats(stats)

	return stats
}

// Prepare is used to inform the VFS server about files that can be lazily loaded on the first open attempt.
func (p *Server) Prepare(ctx context.Context, layout *container.FileSystemLayout) error {
	p.mu.Lock()
	p.casFileCount = 0
	p.casFileSizeBytes = 0
	p.mu.Unlock()
	// There may already be nodes in the tree prior to `Prepare` to be called,
	// for example by the workspace code pre-creating the action output
	// directories. We merge the known CAS inputs with the tree we already have.
	err := p.updateLayout(ctx, layout.Inputs, layout.DigestFunction)
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.casFetcher = newCASFetcher(p.env, layout.RemoteInstanceName, layout.DigestFunction)
	p.internalTaskCtx = ctx
	return nil
}

func (p *Server) computeFullPath(relativePath string) (string, error) {
	fullPath := filepath.Clean(filepath.Join(p.workspacePath, relativePath))
	if !strings.HasPrefix(fullPath, p.workspacePath) {
		return "", status.PermissionDeniedError("path is outside of workspace")
	}
	return fullPath, nil
}

func syscallErrProto(sysErr error) *vfspb.SyscallError {
	errno := fusefs.ToErrno(sysErr)
	return &vfspb.SyscallError{Errno: uint32(errno)}
}

func (p *Server) lookupNode(id uint64) (*fsNode, error) {
	p.mu.Lock()
	node, ok := p.nodes[id]
	p.mu.Unlock()
	if !ok {
		return nil, syscallErrStatus(syscall.ENOENT)
	}
	return node, nil
}

func (p *Server) lookupParentAndChild(parentID uint64, name string) (*fsNode, *fsNode, error) {
	parentNode, err := p.lookupNode(parentID)
	if err != nil {
		return nil, nil, err
	}

	parentNode.mu.Lock()
	defer parentNode.mu.Unlock()
	childNode, ok := parentNode.children[name]
	if !ok {
		return nil, nil, syscallErrStatus(syscall.ENOENT)
	}
	return parentNode, childNode, nil
}

func (p *Server) lookupChild(parentID uint64, name string) (*fsNode, error) {
	_, childNode, err := p.lookupParentAndChild(parentID, name)
	return childNode, err
}

func (p *Server) Lookup(ctx context.Context, request *vfspb.LookupRequest) (*vfspb.LookupResponse, error) {
	childNode, err := p.lookupChild(request.GetParentId(), request.GetName())
	if err != nil {
		return nil, err
	}
	return &vfspb.LookupResponse{
		Mode:          childNode.mode(),
		Attrs:         childNode.attrs,
		Id:            childNode.id,
		SymlinkTarget: childNode.target,
	}, nil
}

func (p *Server) GetDirectoryContents(ctx context.Context, request *vfspb.GetDirectoryContentsRequest) (*vfspb.GetDirectoryContentsResponse, error) {
	node, err := p.lookupNode(request.GetId())
	if err != nil {
		return nil, err
	}

	node.mu.Lock()
	var names []string
	for name := range node.children {
		names = append(names, name)
	}
	node.mu.Unlock()

	slices.Sort(names)

	var nodes []*vfspb.Node
	for _, name := range names {
		node.mu.Lock()
		child, ok := node.children[name]
		node.mu.Unlock()

		// Shouldn't happen in practice.
		if !ok {
			return nil, status.UnknownError("child node disappeared")
		}

		child.mu.Lock()
		nodes = append(nodes, &vfspb.Node{
			Id:    child.id,
			Mode:  child.mode(),
			Name:  name,
			Attrs: child.attrs,
		})
		child.mu.Unlock()
	}
	return &vfspb.GetDirectoryContentsResponse{Nodes: nodes}, nil
}

func (h *fileHandle) open(fullPath string, req *vfspb.OpenRequest) (*vfspb.OpenResponse, error) {
	flags := int(req.GetFlags())
	f, err := os.OpenFile(fullPath, flags, os.FileMode(req.GetFlags()))
	if err != nil {
		return nil, syscallErrStatus(err)
	}
	h.mu.Lock()
	h.f = f
	h.openFlags = req.GetFlags()
	h.mu.Unlock()

	return &vfspb.OpenResponse{}, nil
}

func (h *fileHandle) read(req *vfspb.ReadRequest) (*vfspb.ReadResponse, error) {
	h.mu.Lock()
	f := h.f
	h.mu.Unlock()

	buf := make([]byte, req.GetNumBytes())
	n, err := f.ReadAt(buf, req.GetOffset())
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

func (h *fileHandle) lseek(request *vfspb.LseekRequest) (*vfspb.LseekResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	off, err := h.f.Seek(int64(request.GetOffset()), int(request.GetWhence()))
	if err != nil {
		return nil, syscallErrStatus(err)
	}
	return &vfspb.LseekResponse{Offset: uint64(off)}, nil
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

	if err := h.node.refreshAttrs(); err != nil {
		return nil, err
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

	if h.lockFD != 0 {
		if err := syscall.Close(h.lockFD); err != nil {
			return nil, syscallErrStatus(err)
		}
	}

	return &vfspb.ReleaseResponse{}, nil
}

func (h *fileHandle) allocate(req *vfspb.AllocateRequest) (*vfspb.AllocateResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if err := syscall.Fallocate(int(h.f.Fd()), req.GetMode(), req.GetOffset(), req.GetNumBytes()); err != nil {
		return nil, syscallErrStatus(err)
	}
	return &vfspb.AllocateResponse{}, nil
}

func (p *Server) createNode(nodeType byte, backingPath string, mode uint32, parentNode *fsNode, name string) (*fsNode, error) {
	now := time.Now()

	node := &fsNode{
		nodeType: nodeType,
		name:     name,
		attrs: &vfspb.Attrs{
			Perm:       mode,
			Nlink:      1,
			MtimeNanos: uint64(now.UnixNano()),
			AtimeNanos: uint64(now.UnixNano()),
		},
		backingPath: backingPath,
		parent:      parentNode,
	}

	parentNode.mu.Lock()
	if parentNode.children == nil {
		parentNode.children = make(map[string]*fsNode)
	}
	parentNode.children[name] = node
	parentNode.mu.Unlock()
	p.addNode(node)
	return node, nil
}

func (p *Server) createFile(ctx context.Context, mode uint32, parentNode *fsNode, name string) (*fsNode, error) {
	localFilePath, err := p.generateScratchPath(name)
	if err != nil {
		return nil, syscallErrStatus(err)
	}

	return p.createNode(fsFileNode, localFilePath, mode, parentNode, name)
}

func groupIDStringFromContext(ctx context.Context) string {
	if c, err := claims.ClaimsFromContext(ctx); err == nil {
		return c.GroupID
	}
	return interfaces.AuthAnonymousUser
}

var downloadDeduper = singleflight.Group[string, struct{}]{}

type casFetcher struct {
	env                environment.Env
	remoteInstanceName string
	digestFunction     repb.DigestFunction_Value

	mu                sync.Mutex
	fetchStart        time.Time
	fetchesInProgress int

	fileCacheHits     int
	downloadCount     int
	downloadDuration  time.Duration
	downloadSizeBytes int64
}

func newCASFetcher(env environment.Env, remoteInstanceName string, digestFunction repb.DigestFunction_Value) *casFetcher {
	return &casFetcher{
		env:                env,
		remoteInstanceName: remoteInstanceName,
		digestFunction:     digestFunction,
	}
}

func (cf *casFetcher) downloadToFileCache(ctx context.Context, node *fsNode) error {
	bsClient := cf.env.GetByteStreamClient()
	rn := digest.NewCASResourceName(node.fileNode.GetDigest(), cf.remoteInstanceName, cf.digestFunction)
	rn.SetCompressor(repb.Compressor_ZSTD)
	w, err := cf.env.GetFileCache().Writer(ctx, node.fileNode, cf.digestFunction)
	if err != nil {
		return err
	}
	defer w.Close()

	if err := cachetools.GetBlob(ctx, bsClient, rn, w); err != nil {
		return err
	}

	if err := w.Commit(); err != nil {
		return err
	}

	return nil
}

func (cf *casFetcher) dedupeDownloadToFileCache(ctx context.Context, node *fsNode) error {
	cf.mu.Lock()
	cf.fetchesInProgress++
	// Don't include concurrent downloads in total fetch time.
	// The final download duration will reflect the amount of wall time the
	// action was blocked on CAS fetches.
	if cf.fetchesInProgress == 1 {
		cf.fetchStart = time.Now()
	}
	cf.mu.Unlock()

	defer func() {
		cf.mu.Lock()
		cf.fetchesInProgress--
		if cf.fetchesInProgress == 0 {
			cf.downloadDuration += time.Since(cf.fetchStart)
		}
		cf.mu.Unlock()
	}()

	dedupeKey := groupIDStringFromContext(ctx) + "-" + node.fileNode.GetDigest().GetHash()
	_, _, err := downloadDeduper.Do(ctx, dedupeKey, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, cf.downloadToFileCache(ctx, node)
	})
	return err
}

func (cf *casFetcher) Open(ctx context.Context, node *fsNode) (*os.File, error) {
	// If we can open the file directly from the file cache then use that.
	if f, err := cf.env.GetFileCache().Open(ctx, node.fileNode); err == nil {
		cf.mu.Lock()
		cf.fileCacheHits++
		cf.mu.Unlock()
		return f, nil
	}

	if err := cf.dedupeDownloadToFileCache(ctx, node); err != nil {
		return nil, err
	}

	cf.mu.Lock()
	// N.B. in case of deduping, the download will be counted in the stats of
	// all deduped actions.
	cf.downloadCount++
	cf.downloadSizeBytes += node.fileNode.GetDigest().GetSizeBytes()
	cf.mu.Unlock()

	// CAS downloads put the CAS artifacts directly into the file cache.
	return cf.env.GetFileCache().Open(ctx, node.fileNode)
}

func (cf *casFetcher) UpdateIOStats(stats *repb.VfsStats) {
	cf.mu.Lock()
	defer cf.mu.Unlock()
	stats.FileDownloadSizeBytes = cf.downloadSizeBytes
	stats.FileDownloadCount = int64(cf.downloadCount)
	stats.FileDownloadDurationUsec = cf.downloadDuration.Microseconds()
}

func (p *Server) Mknod(ctx context.Context, request *vfspb.MknodRequest) (*vfspb.MknodResponse, error) {
	parentNode, err := p.lookupNode(request.GetParentId())
	if err != nil {
		return nil, err
	}

	if request.GetMode()&unix.S_IFREG != 0 {
		node, err := p.createFile(ctx, request.GetMode(), parentNode, request.GetName())
		if err != nil {
			return nil, err
		}
		if err := unix.Mknod(node.backingPath, request.GetMode(), 0); err != nil {
			return nil, syscallErrStatus(err)
		}
		return &vfspb.MknodResponse{Id: node.id, Attrs: node.attrs}, nil
	}

	if request.GetMode()&unix.S_IFCHR != 0 {
		if request.GetDev() != 0 {
			return nil, syscallErrStatus(syscall.ENOSYS)
		}
		node, err := p.createNode(fsCharDevNode, "", request.GetMode(), parentNode, request.GetName())
		if err != nil {
			return nil, err
		}
		return &vfspb.MknodResponse{Id: node.id, Attrs: node.attrs}, nil
	}

	return nil, syscallErrStatus(syscall.ENOSYS)
}

func (p *Server) Create(ctx context.Context, request *vfspb.CreateRequest) (*vfspb.CreateResponse, error) {
	parentNode, err := p.lookupNode(request.GetParentId())
	if err != nil {
		return nil, err
	}

	node, err := p.createFile(ctx, request.GetMode(), parentNode, request.GetName())
	if err != nil {
		log.CtxWarningf(p.taskCtx(), "Open %q could not create new file: %s", request.GetName(), err)
		return nil, err
	}

	file, err := os.OpenFile(node.backingPath, int(request.GetFlags()), os.FileMode(request.GetMode()))
	if err != nil {
		return nil, syscallErrStatus(err)
	}

	node.mu.Lock()
	id := node.id
	node.mu.Unlock()

	fh := &fileHandle{
		node:      node,
		f:         file,
		openFlags: request.GetFlags(),
	}
	// We use the file descriptor as the file handle ID so that we can directly
	// use the flock/fcntl syscalls to implement file locking (SetLk, SetLkw,
	// GetLk). This is needed because file locking works on descriptors, not
	// paths.
	handleID := uint64(fh.f.Fd())
	p.mu.Lock()
	p.fileHandles[handleID] = fh
	p.mu.Unlock()
	return &vfspb.CreateResponse{Id: id, HandleId: handleID}, nil
}

func (p *Server) Open(ctx context.Context, request *vfspb.OpenRequest) (*vfspb.OpenResponse, error) {
	node, err := p.lookupNode(request.GetId())
	if err != nil {
		return nil, err
	}

	var openedFile *os.File
	node.mu.Lock()
	backingFile := node.backingPath
	// Update atime for CAS nodes. For other nodes, we use the atime from the
	// backing file.
	if backingFile == "" {
		node.attrs = updateAttr(node.attrs, func(attr *vfspb.Attrs) {
			attr.AtimeNanos = uint64(time.Now().UnixNano())
		})
	}
	node.accessed = true
	node.mu.Unlock()
	if backingFile != "" {
		f, err := os.OpenFile(backingFile, int(request.GetFlags()), os.FileMode(request.GetFlags()))
		if err != nil {
			log.CtxWarningf(p.taskCtx(), "Open %d could not open file %q: %s", request.GetId(), backingFile, err)
			return nil, syscallErrStatus(err)
		}
		openedFile = f
	} else if node.fileNode != nil {
		f, err := p.casFetcher.Open(p.taskCtx(), node)
		if err != nil {
			log.CtxWarningf(p.taskCtx(), "Open %q could not fetch file from cache: %s", node.Path(), err)
			return nil, err
		}
		openedFile = f
	} else {
		log.CtxWarningf(p.taskCtx(), "Open called on an unlinked node %q", node.Path())
		return nil, syscallErrStatus(syscall.EINVAL)
	}

	fh := &fileHandle{
		node:      node,
		f:         openedFile,
		openFlags: request.GetFlags(),
	}
	// We use the file descriptor as the file handle ID so that we can directly
	// use the flock/fcntl syscalls to implement file locking (SetLk, SetLkw,
	// GetLk). This is needed because file locking works on descriptors, not
	// paths.
	handleID := uint64(fh.f.Fd())
	p.mu.Lock()
	p.fileHandles[handleID] = fh
	p.mu.Unlock()
	return &vfspb.OpenResponse{HandleId: handleID}, nil
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
		log.CtxWarningf(p.taskCtx(), "fsync: could not find file handle %d", request.GetHandleId())
		return nil, err
	}
	rsp, err := fh.fsync(request)
	if err != nil {
		log.CtxWarningf(p.taskCtx(), "fsync: could not fsync file handle %d", request.GetHandleId())
	}
	return rsp, err
}

func (p *Server) Lseek(ctx context.Context, request *vfspb.LseekRequest) (*vfspb.LseekResponse, error) {
	fh, err := p.getFileHandle(request.GetHandleId())
	if err != nil {
		return nil, err
	}

	return fh.lseek(request)
}

func (p *Server) Flush(ctx context.Context, request *vfspb.FlushRequest) (*vfspb.FlushResponse, error) {
	fh, err := p.getFileHandle(request.GetHandleId())
	if err != nil {
		log.CtxWarningf(p.taskCtx(), "flush: could not find file handle %d", request.GetHandleId())
		return nil, err
	}
	rsp, err := fh.flush(request)
	if err != nil {
		log.CtxWarningf(p.taskCtx(), "flush: could not flush file handle %d: %s", request.GetHandleId(), err)
	}
	return rsp, err
}

func (p *Server) Release(ctx context.Context, request *vfspb.ReleaseRequest) (*vfspb.ReleaseResponse, error) {
	fh, err := p.getFileHandle(request.GetHandleId())
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	delete(p.fileHandles, request.GetHandleId())
	p.mu.Unlock()

	return fh.release(request)
}

func (p *Server) GetAttr(ctx context.Context, request *vfspb.GetAttrRequest) (*vfspb.GetAttrResponse, error) {
	node, err := p.lookupNode(request.GetId())
	if err != nil {
		log.Infof("get attr %d not found", request.GetId())
		return nil, err
	}
	if err := node.refreshAttrs(); err != nil {
		return nil, err
	}
	node.mu.Lock()
	defer node.mu.Unlock()
	return &vfspb.GetAttrResponse{Attrs: node.attrs}, nil
}

func (p *Server) processSetAttr(node *fsNode, request *vfspb.SetAttrRequest, newAttrs *vfspb.Attrs) error {
	if request.SetPerms != nil {
		newAttrs.Perm = request.SetPerms.Perms
	}

	if request.SetSize != nil {
		if node.backingPath == "" {
			return syscall.EPERM
		}
		if err := os.Truncate(node.backingPath, request.SetSize.GetSize()); err != nil {
			return err
		}
		newAttrs.Size = request.SetSize.GetSize()
	}

	if request.SetMtime != nil {
		newAttrs.MtimeNanos = request.SetMtime.GetMtimeNanos()
	}
	if request.SetAtime != nil {
		newAttrs.AtimeNanos = request.SetAtime.GetAtimeNanos()
	}

	// If either mtime or atime are explicitly set then propagate those
	// updates to the backing file. Because of passthrough, we can't know
	// if the timestamps on a backing file were updated. Passing through the
	// timestamp updates to the backing file means we can trust whatever values
	// we get back from Stat when refreshing attributes.
	if node.backingPath != "" && (request.SetMtime != nil || request.SetAtime != nil) {
		var atime, mtime time.Time
		if request.SetMtime != nil {
			mtime = time.Unix(0, int64(request.SetMtime.GetMtimeNanos()))
		}
		if request.SetAtime != nil {
			atime = time.Unix(0, int64(request.SetAtime.GetAtimeNanos()))
		}
		if err := os.Chtimes(node.backingPath, atime, mtime); err != nil {
			return err
		}
	}

	return nil
}

func (p *Server) processSetExtendedAttr(node *fsNode, request *vfspb.SetAttrRequest, newAttrs *vfspb.Attrs) error {
	set := request.SetExtended != nil
	remove := request.RemoveExtended != nil
	if !set && !remove {
		return status.InvalidArgumentErrorf("either set or remove should be present")
	}
	if set && remove {
		return status.InvalidArgumentErrorf("both set and remove should not be present")
	}

	if se := request.SetExtended; se != nil {
		exists := false
		for _, xa := range newAttrs.Extended {
			if xa.Name == se.Name {
				if se.Flags&unix.XATTR_CREATE != 0 {
					return syscall.EEXIST
				}
				xa.Value = se.Value
				exists = true
				break
			}
		}
		if !exists {
			if se.Flags&unix.XATTR_REPLACE != 0 {
				return syscall.ENODATA
			}
			newAttrs.Extended = append(newAttrs.Extended, &vfspb.ExtendedAttr{
				Name:  se.Name,
				Value: se.Value,
			})
		}
		return nil
	}

	if re := request.RemoveExtended; re != nil {
		for i, xa := range newAttrs.Extended {
			if xa.Name == re.Name {
				newAttrs.Extended = append(newAttrs.Extended[:i], newAttrs.Extended[i+1:]...)
				return nil
			}
		}
		return syscall.ENODATA
	}

	return nil
}

func (p *Server) SetAttr(ctx context.Context, request *vfspb.SetAttrRequest) (*vfspb.SetAttrResponse, error) {
	node, err := p.lookupNode(request.GetId())
	if err != nil {
		return nil, err
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	// Not using updateAttr here to avoid putting all the setattr logic
	// inside a nested function.
	newAttrs := proto.Clone(node.attrs).(*vfspb.Attrs)

	if request.SetExtended != nil || request.RemoveExtended != nil {
		if err := p.processSetExtendedAttr(node, request, newAttrs); err != nil {
			return nil, syscallErrStatus(err)
		}
	} else {
		if err := p.processSetAttr(node, request, newAttrs); err != nil {
			return nil, syscallErrStatus(err)
		}
	}

	node.attrs = newAttrs

	return &vfspb.SetAttrResponse{Attrs: node.attrs}, nil
}

func (p *Server) Rename(ctx context.Context, request *vfspb.RenameRequest) (*vfspb.RenameResponse, error) {
	oldParentNode, oldChildNode, err := p.lookupParentAndChild(request.GetOldParentId(), request.GetOldName())
	if err != nil {
		return nil, err
	}

	newParentNode, err := p.lookupNode(request.GetNewParentId())
	if err != nil {
		return nil, err
	}

	newParentNode.mu.Lock()
	newChildNode, newExists := newParentNode.children[request.GetNewName()]
	newParentNode.mu.Unlock()
	if newExists {
		if err := unlink(newParentNode, newChildNode, request.GetNewName()); err != nil {
			return nil, err
		}
	}

	// If RENAME_WHITEOUT flag is present, we need to create a character
	// device where the old node used to be.
	if request.GetFlags()&unix.RENAME_WHITEOUT != 0 {
		_, err := p.createNode(fsCharDevNode, "", 0644, oldParentNode, request.GetOldName())
		if err != nil {
			return nil, err
		}
	} else {
		oldParentNode.mu.Lock()
		delete(oldParentNode.children, request.GetOldName())
		oldParentNode.mu.Unlock()
	}

	newParentNode.mu.Lock()
	if newParentNode.children == nil {
		newParentNode.children = make(map[string]*fsNode)
	}
	newParentNode.children[request.GetNewName()] = oldChildNode
	newParentNode.mu.Unlock()

	return &vfspb.RenameResponse{}, nil
}

func (p *Server) Mkdir(ctx context.Context, request *vfspb.MkdirRequest) (*vfspb.MkdirResponse, error) {
	parentNode, err := p.lookupNode(request.GetParentId())
	if err != nil {
		return nil, err
	}

	parentNode.mu.Lock()
	defer parentNode.mu.Unlock()
	if _, ok := parentNode.children[request.GetName()]; ok {
		return nil, syscallErrStatus(syscall.EEXIST)
	}

	if parentNode.children == nil {
		parentNode.children = make(map[string]*fsNode)
	}
	newNode := &fsNode{
		name: request.GetName(),
		attrs: &vfspb.Attrs{
			Size: 1000,
			Perm: request.GetPerms(),
		},
		parent: parentNode,
	}
	parentNode.children[request.GetName()] = newNode
	p.addNode(newNode)

	return &vfspb.MkdirResponse{Id: newNode.id, Attrs: newNode.attrs}, nil
}

func (p *Server) Rmdir(ctx context.Context, request *vfspb.RmdirRequest) (*vfspb.RmdirResponse, error) {
	parentNode, childNode, err := p.lookupParentAndChild(request.GetParentId(), request.GetName())
	if err != nil {
		return nil, err
	}

	childNode.mu.Lock()
	empty := len(childNode.children) == 0
	if !empty {
		childNode.mu.Unlock()
		return nil, syscallErrStatus(syscall.ENOTEMPTY)
	}
	childNode.mu.Unlock()

	parentNode.mu.Lock()
	delete(parentNode.children, request.GetName())
	parentNode.mu.Unlock()

	return &vfspb.RmdirResponse{}, nil
}

func (p *Server) Link(ctx context.Context, request *vfspb.LinkRequest) (*vfspb.LinkResponse, error) {
	existingNode, err := p.lookupNode(request.GetTargetId())
	if err != nil {
		return nil, err
	}

	newParentNode, err := p.lookupNode(request.GetParentId())
	if err != nil {
		return nil, err
	}

	existingNode.mu.Lock()
	newAttrs := updateAttr(existingNode.attrs, func(attr *vfspb.Attrs) {
		attr.Nlink++
	})
	existingNode.attrs = newAttrs
	existingNode.mu.Unlock()

	newParentNode.mu.Lock()
	if newParentNode.children == nil {
		newParentNode.children = make(map[string]*fsNode)
	}
	newParentNode.children[request.GetName()] = existingNode
	newParentNode.mu.Unlock()

	return &vfspb.LinkResponse{Attrs: newAttrs}, nil
}

func (p *Server) Symlink(ctx context.Context, request *vfspb.SymlinkRequest) (*vfspb.SymlinkResponse, error) {
	parentNode, err := p.lookupNode(request.GetParentId())
	if err != nil {
		return nil, err
	}

	parentNode.mu.Lock()
	defer parentNode.mu.Unlock()
	_, ok := parentNode.children[request.GetName()]
	if ok {
		log.Infof("parent already has child %q", request.GetName())
		return nil, syscallErrStatus(syscall.EEXIST)
	}
	if parentNode.children == nil {
		parentNode.children = make(map[string]*fsNode)
	}
	node := newSymlinkNode(parentNode, request.GetName(), request.GetTarget())
	parentNode.children[request.GetName()] = node
	id := p.addNode(node)
	return &vfspb.SymlinkResponse{Id: id}, nil
}

func unlink(parentNode *fsNode, childNode *fsNode, childName string) error {
	childNode.mu.Lock()
	childNode.attrs = updateAttr(childNode.attrs, func(attr *vfspb.Attrs) {
		attr.Nlink--
	})
	if childNode.backingPath != "" && childNode.attrs.Nlink == 0 {
		err := os.Remove(childNode.backingPath)
		if err != nil {
			childNode.mu.Unlock()
			return syscallErrStatus(err)
		}
		childNode.backingPath = ""
		childNode.server.mu.Lock()
		childNode.server.blocks -= childNode.server.statBlocksToFSBlocks(childNode.attrs.Blocks)
		childNode.server.mu.Unlock()
	}
	childNode.mu.Unlock()

	parentNode.mu.Lock()
	if _, ok := parentNode.children[childName]; !ok {
		return syscallErrStatus(syscall.ENOENT)
	}
	delete(parentNode.children, childName)
	parentNode.mu.Unlock()
	return nil
}

func (p *Server) Unlink(ctx context.Context, request *vfspb.UnlinkRequest) (*vfspb.UnlinkResponse, error) {
	parentNode, childNode, err := p.lookupParentAndChild(request.GetParentId(), request.GetName())
	if err != nil {
		return nil, err
	}
	if err := unlink(parentNode, childNode, request.GetName()); err != nil {
		return nil, err
	}
	return &vfspb.UnlinkResponse{}, nil
}

func (p *Server) getLockFD(handleID uint64) (int, error) {
	fh, err := p.getFileHandle(handleID)
	if err != nil {
		return 0, err
	}

	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.lockFD == 0 {
		// Open a separate FD to use for locking. Because of FUSE passthrough,
		// the kernel will maintain a reference to the original FD which may
		// prevent locks from automatically being released when the file
		// handle is closed.
		// Theoretically an application could open a file, delete it and then
		// call the lock functions, but we assume that's unlikely to happen
		// in practice.
		lockFD, err := syscall.Open(fh.f.Name(), syscall.O_RDWR, 0)
		if err != nil {
			log.CtxWarningf(p.taskCtx(), "could not open file %q for locking: %s", fh.f.Name(), err)
			return 0, syscallErrStatus(err)
		}
		fh.lockFD = lockFD
	}
	return fh.lockFD, nil
}

func (p *Server) GetLk(ctx context.Context, req *vfspb.GetLkRequest) (*vfspb.GetLkResponse, error) {
	lockFD, err := p.getLockFD(req.GetHandleId())
	if err != nil {
		return nil, err
	}

	flk := syscall.Flock_t{}
	fileLockFromProto(req.GetFileLock()).ToFlockT(&flk)
	if err := syscall.FcntlFlock(uintptr(lockFD), _OFD_GETLK, &flk); err != nil {
		return nil, syscallErrStatus(err)
	}
	out := &fuse.FileLock{}
	out.FromFlockT(&flk)
	return &vfspb.GetLkResponse{FileLock: fileLockToProto(out)}, nil
}

func (p *Server) SetLk(ctx context.Context, req *vfspb.SetLkRequest) (*vfspb.SetLkResponse, error) {
	return p.setlk(ctx, req, false /*=wait*/)
}

func (p *Server) SetLkw(ctx context.Context, req *vfspb.SetLkRequest) (*vfspb.SetLkResponse, error) {
	return p.setlk(ctx, req, true /*=wait*/)
}

func (p *Server) setlk(ctx context.Context, req *vfspb.SetLkRequest, wait bool) (*vfspb.SetLkResponse, error) {
	lockFD, err := p.getLockFD(req.GetHandleId())
	if err != nil {
		return nil, err
	}

	lk := fileLockFromProto(req.GetFileLock())
	flags := req.Flags

	if (flags & fuse.FUSE_LK_FLOCK) != 0 {
		// Lock with flock(2)
		var op int
		switch lk.Typ {
		case syscall.F_RDLCK:
			op = syscall.LOCK_SH
		case syscall.F_WRLCK:
			op = syscall.LOCK_EX
		case syscall.F_UNLCK:
			op = syscall.LOCK_UN
		default:
			return nil, syscallErrStatus(syscall.EINVAL)
		}
		if !wait {
			op |= syscall.LOCK_NB
		}
		if err := syscall.Flock(lockFD, op); err != nil {
			return nil, syscallErrStatus(err)
		}
		return &vfspb.SetLkResponse{}, nil
	}

	// Lock with fcntl(2)
	flk := syscall.Flock_t{}
	lk.ToFlockT(&flk)
	var op int
	if wait {
		op = _OFD_SETLKW
	} else {
		op = _OFD_SETLK
	}
	if err := syscall.FcntlFlock(uintptr(lockFD), op, &flk); err != nil {
		return nil, syscallErrStatus(err)
	}
	return &vfspb.SetLkResponse{}, nil
}

func (p *Server) CopyFileRange(ctx context.Context, request *vfspb.CopyFileRangeRequest) (*vfspb.CopyFileRangeResponse, error) {
	rh, err := p.getFileHandle(request.GetReadHandleId())
	if err != nil {
		return nil, err
	}
	wh, err := p.getFileHandle(request.GetWriteHandleId())
	if err != nil {
		return nil, err
	}

	var lockFirst, lockSecond *fileHandle
	if request.GetReadHandleId() <= request.GetWriteHandleId() {
		lockFirst = rh
		lockSecond = wh
	} else {
		lockFirst = wh
		lockSecond = rh
	}
	lockFirst.mu.Lock()
	defer lockFirst.mu.Unlock()
	if lockSecond != lockFirst {
		lockSecond.mu.Lock()
		defer lockSecond.mu.Unlock()
	}

	n, err := unix.CopyFileRange(int(rh.f.Fd()), &request.ReadHandleOffset, int(wh.f.Fd()), &request.WriteHandleOffset, int(request.GetNumBytes()), int(request.GetFlags()))
	if err != nil {
		return nil, syscallErrStatus(err)
	}
	return &vfspb.CopyFileRangeResponse{NumBytesCopied: uint32(n)}, nil
}

func (p *Server) Statfs(ctx context.Context, request *vfspb.StatfsRequest) (*vfspb.StatfsResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	totalBlocks := reportedSizeBytes / p.backingBlockSize

	free := totalBlocks - p.blocks
	// We don't enforce a usage limit yet so used blocks may go over the
	// total blocks.
	if free < 0 {
		free = 0
	}

	return &vfspb.StatfsResponse{
		BlockSize:       p.backingBlockSize,
		TotalBlocks:     uint64(totalBlocks),
		BlocksFree:      uint64(free),
		BlocksAvailable: uint64(free),
	}, nil
}

func (p *Server) Start(lis net.Listener) error {
	p.mu.Lock()
	p.server = grpc.NewServer(grpc_server.CommonGRPCServerOptions(p.env)...)
	p.mu.Unlock()
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
	server := p.server
	p.mu.Unlock()
	if server != nil {
		server.Stop()
	}
}

func fileLockFromProto(pb *vfspb.FileLock) *fuse.FileLock {
	return &fuse.FileLock{
		Start: pb.Start,
		End:   pb.End,
		Typ:   pb.Typ,
		Pid:   pb.Pid,
	}
}

func fileLockToProto(lk *fuse.FileLock) *vfspb.FileLock {
	return &vfspb.FileLock{
		Start: lk.Start,
		End:   lk.End,
		Typ:   lk.Typ,
		Pid:   lk.Pid,
	}
}
