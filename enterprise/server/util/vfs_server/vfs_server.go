//go:build ((linux && !android) || (darwin && !ios)) && (amd64 || arm64)

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

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/server/cache/dirtools"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/hanwen/go-fuse/v2/fuse"
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
)

type fileHandle struct {
	node *fsNode

	mu        sync.Mutex
	f         *os.File
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

const (
	fsDirectoryNode = iota
	fsFileNode
	fsSymlinkNode
)

type fsNode struct {
	id       uint64
	nodeType byte
	fileNode *repb.FileNode
	target   string

	mu          sync.Mutex
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
	}
	return 0
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

	fsn.attrs = &vfspb.Attrs{
		Size:      fi.Size(),
		Perm:      fsn.attrs.Perm,
		Immutable: fsn.attrs.Immutable,
		Nlink:     fsn.attrs.Nlink,
	}
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
	return &fsNode{
		nodeType: fsFileNode,
		name:     refn.GetName(),
		attrs: &vfspb.Attrs{
			Size:      refn.GetDigest().GetSizeBytes(),
			Perm:      uint32(perms),
			Immutable: true,
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
	env           environment.Env
	workspacePath string

	server *grpc.Server

	nextNodeID atomic.Uint64

	mu                 sync.Mutex
	nextId             uint64
	nodes              map[uint64]*fsNode
	taskCtx            context.Context
	fileFetcher        *dirtools.BatchFileFetcher
	root               *fsNode
	remoteInstanceName string
	fileHandles        map[uint64]*fileHandle
}

func New(env environment.Env, workspacePath string) *Server {
	return &Server{
		env:           env,
		workspacePath: workspacePath,
		fileHandles:   make(map[uint64]*fileHandle),
		root:          &fsNode{attrs: &vfspb.Attrs{Size: 0, Perm: 0755}},
	}
}

func (p *Server) addNode(node *fsNode) uint64 {
	id := atomic.AddUint64(&p.nextId, 1)
	node.id = id
	p.mu.Lock()
	p.nodes[id] = node
	p.mu.Unlock()
	return id
}

func (p *Server) createLayout(ctx context.Context, inputTree *repb.Tree, digestFunction repb.DigestFunction_Value) (*fsNode, error) {
	_, dirMap, err := dirtools.DirMapFromTree(inputTree, digestFunction)
	if err != nil {
		return nil, err
	}
	numDirs := 0
	numFiles := 0
	numSymlinks := 0

	rootNode := newDirNode(nil, "")
	p.mu.Lock()
	p.nodes = make(map[uint64]*fsNode)
	p.nodes[0] = rootNode
	p.mu.Unlock()

	rootNode.mu.Lock()
	defer rootNode.mu.Unlock()

	var walkDir func(dir *repb.Directory, parentNode *fsNode) error
	walkDir = func(dir *repb.Directory, parentNode *fsNode) error {
		numDirs++
		if len(dir.GetDirectories()) > 0 || len(dir.GetFiles()) > 0 || len(dir.GetSymlinks()) > 0 {
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
			subNode := newDirNode(parentNode, childDirNode.GetName())
			p.addNode(subNode)
			parentNode.children[childDirNode.Name] = subNode
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

	err = walkDir(inputTree.Root, rootNode)
	if err != nil {
		return nil, err
	}

	log.CtxDebugf(ctx, "VFS contains %d directories, %d files and %d symlinks", numDirs, numFiles, numSymlinks)

	return rootNode, nil
}

func (p *Server) mkdirAll(root *fsNode, path string) {
	node := root
	for _, dir := range strings.Split(path, string(os.PathSeparator)) {
		if subNode, ok := node.children[dir]; ok {
			node = subNode
			continue
		}

		node.mu.Lock()
		if node.children == nil {
			node.children = make(map[string]*fsNode)
		}
		subNode := newDirNode(node, dir)
		p.addNode(subNode)
		node.children[dir] = subNode
		node.mu.Unlock()
		node = subNode
	}
}

// Prepare is used to inform the VFS server about files that can be lazily loaded on the first open attempt.
func (p *Server) Prepare(ctx context.Context, layout *container.FileSystemLayout) error {
	rootNode, err := p.createLayout(ctx, layout.Inputs, layout.DigestFunction)
	if err != nil {
		return err
	}

	dirsToCreate := make(map[string]struct{})
	for _, dir := range layout.OutputDirs {
		dirsToCreate[dir] = struct{}{}
	}
	for _, file := range layout.OutputFiles {
		dirsToCreate[filepath.Dir(file)] = struct{}{}
	}
	for _, path := range layout.OutputPaths {
		dirsToCreate[filepath.Dir(path)] = struct{}{}
	}

	for path := range dirsToCreate {
		p.mkdirAll(rootNode, path)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.taskCtx = ctx
	p.root = rootNode
	p.fileFetcher = dirtools.NewBatchFileFetcher(ctx, p.env, layout.RemoteInstanceName, layout.DigestFunction)
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

	return &vfspb.ReleaseResponse{}, nil
}

func (p *Server) createFile(ctx context.Context, request *vfspb.CreateRequest, parentNode *fsNode, name string) (*os.File, *fsNode, error) {
	parentNode.mu.Lock()
	parentBackingPath := parentNode.backingPath
	parentNode.mu.Unlock()
	if parentBackingPath == "" {
		wsPath, err := p.computeFullPath(parentNode.Path())
		if err != nil {
			return nil, nil, err
		}
		if err := os.MkdirAll(wsPath, 0755); err != nil {
			return nil, nil, syscallErrStatus(err)
		}
		parentNode.mu.Lock()
		parentNode.backingPath = wsPath
		parentBackingPath = wsPath
		parentNode.mu.Unlock()
	}

	localFilePath := filepath.Join(parentBackingPath, name)
	f, err := os.OpenFile(localFilePath, int(request.GetFlags()), os.FileMode(request.GetMode()))
	if err != nil {
		return nil, nil, syscallErrStatus(err)
	}

	node := &fsNode{
		nodeType: fsFileNode,
		name:     name,
		attrs: &vfspb.Attrs{
			Perm: request.GetMode(),
		},
		backingPath: localFilePath,
		parent:      parentNode,
	}
	parentNode.mu.Lock()
	if parentNode.children == nil {
		parentNode.children = make(map[string]*fsNode)
	}
	parentNode.children[name] = node
	parentNode.mu.Unlock()
	p.addNode(node)
	return f, node, nil
}

func (p *Server) openCASFile(ctx context.Context, node *fsNode, flags uint32) (*os.File, error) {
	// If we can open the file directly from the file cache then use that.
	if f, err := p.env.GetFileCache().Open(ctx, node.fileNode); err == nil {
		return f, nil
	}

	// If the file is not in the file cache, download it to the backing
	// directory and return a file handle to the backing file.
	localFileName, err := random.RandomString(16)
	if err != nil {
		return nil, err
	}
	localFilePath := filepath.Join(p.workspacePath, localFileName)
	fileMap := dirtools.FileMap{
		digest.NewKey(node.fileNode.Digest): {&dirtools.FilePointer{
			FullPath: localFilePath,
			FileNode: node.fileNode,
		}},
	}
	if err := p.fileFetcher.FetchFiles(fileMap, &dirtools.DownloadTreeOpts{}); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(localFilePath, int(flags), 0)
	if err != nil {
		return nil, syscallErrStatus(err)
	}
	node.mu.Lock()
	node.backingPath = localFilePath
	node.mu.Unlock()
	return f, nil
}

func (p *Server) Create(ctx context.Context, request *vfspb.CreateRequest) (*vfspb.CreateResponse, error) {
	parentNode, err := p.lookupNode(request.GetParentId())
	if err != nil {
		return nil, err
	}

	file, node, err := p.createFile(ctx, request, parentNode, request.GetName())
	if err != nil {
		log.CtxWarningf(p.taskCtx, "Open %q could not create new file: %s", request.GetName(), err)
		return nil, err
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
	node.mu.Unlock()
	if backingFile != "" {
		f, err := os.OpenFile(backingFile, int(request.GetFlags()), os.FileMode(request.GetFlags()))
		if err != nil {
			log.CtxWarningf(p.taskCtx, "Open %d could not open file %q: %s", request.GetId(), backingFile, err)
			return nil, syscallErrStatus(err)
		}
		openedFile = f
	} else {
		f, err := p.openCASFile(ctx, node, request.GetFlags())
		if err != nil {
			return nil, err
		}
		openedFile = f
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
		log.CtxWarningf(p.taskCtx, "fsync: could not find file handle %d", request.GetHandleId())
		return nil, err
	}
	rsp, err := fh.fsync(request)
	if err != nil {
		log.CtxWarningf(p.taskCtx, "fsync: could not fsync file handle %d", request.GetHandleId())
	}
	return rsp, err
}

func (p *Server) Flush(ctx context.Context, request *vfspb.FlushRequest) (*vfspb.FlushResponse, error) {
	fh, err := p.getFileHandle(request.GetHandleId())
	if err != nil {
		log.CtxWarningf(p.taskCtx, "flush: could not find file handle %d", request.GetHandleId())
		return nil, err
	}
	rsp, err := fh.flush(request)
	if err != nil {
		log.CtxWarningf(p.taskCtx, "flush: could not flush file handle %d: %s", request.GetHandleId(), err)
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

func (p *Server) getAttr(fullPath string) (*vfspb.Attrs, error) {
	fi, err := os.Stat(fullPath)
	if err != nil {
		return nil, syscallErrStatus(err)
	}
	attrs := &vfspb.Attrs{
		Size: fi.Size(),
		Perm: uint32(fi.Mode().Perm()),
	}
	if stat, ok := fi.Sys().(*syscall.Stat_t); ok {
		attrs.Nlink = uint32(stat.Nlink)
	}
	return attrs, nil
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

func (p *Server) SetAttr(ctx context.Context, request *vfspb.SetAttrRequest) (*vfspb.SetAttrResponse, error) {
	node, err := p.lookupNode(request.GetId())
	if err != nil {
		return nil, err
	}

	node.mu.Lock()
	defer node.mu.Unlock()
	if request.SetPerms != nil {
		node.attrs.Perm = request.SetPerms.Perms
	}

	if request.SetSize != nil {
		if node.backingPath == "" {
			return nil, syscallErrStatus(syscall.EPERM)
		}
		if err := os.Truncate(node.backingPath, request.SetSize.GetSize()); err != nil {
			return nil, syscallErrStatus(err)
		}
		node.attrs.Size = request.SetSize.GetSize()
	}
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
		if oldChildNode.IsFile() && newChildNode.IsFile() {
			if err := unlink(newParentNode, newChildNode, request.GetNewName()); err != nil {
				return nil, err
			}
		} else {
			return nil, syscallErrStatus(syscall.EEXIST)
		}
	}

	oldChildNode.mu.Lock()
	newBackingPath := ""
	if oldChildNode.backingPath != "" {
		newBackingPath = filepath.Join(p.workspacePath, newParentNode.Path(), request.GetNewName())
		_ = os.MkdirAll(filepath.Dir(newBackingPath), 0755)
		log.CtxWarningf(p.taskCtx, "renaming %q to %q", oldChildNode.backingPath, newBackingPath)
		if err := os.Rename(oldChildNode.backingPath, newBackingPath); err != nil {
			oldChildNode.mu.Unlock()
			return nil, err
		}
		oldChildNode.backingPath = newBackingPath
	}
	oldChildNode.mu.Unlock()

	if newBackingPath != "" {
		var ubs func(node *fsNode, parentBackingPath string)
		ubs = func(node *fsNode, parentBackingPath string) {
			node.mu.Lock()
			children := make(map[string]*fsNode)
			for name, child := range node.children {
				children[name] = child
			}
			node.mu.Unlock()

			for name, child := range children {
				child.mu.Lock()
				log.CtxWarningf(p.taskCtx, "node %q backing file %q", name, child.backingPath)
				if strings.HasPrefix(child.backingPath, oldChildNode.backingPath) {
					newChildBackingPath := filepath.Join(parentBackingPath, name)
					log.CtxWarningf(p.taskCtx, "updating backing path %q to %q", child.backingPath, newChildBackingPath)
					child.backingPath = newChildBackingPath
				}
				child.mu.Unlock()
				ubs(child, filepath.Join(parentBackingPath, name))
			}
		}
		ubs(oldChildNode, newBackingPath)
	}

	oldParentNode.mu.Lock()
	delete(oldParentNode.children, request.GetOldName())
	oldParentNode.mu.Unlock()

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

	return &vfspb.MkdirResponse{Id: newNode.id}, nil
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
	if childNode.backingPath != "" {
		if err := os.Remove(childNode.backingPath); err != nil {
			childNode.mu.Unlock()
			return nil, err
		}
	}
	childNode.mu.Unlock()

	parentNode.mu.Lock()
	delete(parentNode.children, request.GetName())
	parentNode.mu.Unlock()

	return &vfspb.RmdirResponse{}, nil
}

func (p *Server) Link(ctx context.Context, request *vfspb.LinkRequest) (*vfspb.LinkResponse, error) {
	return nil, syscallErrStatus(syscall.EPERM)
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
	if childNode.backingPath != "" {
		err := os.Remove(childNode.backingPath)
		if err != nil {
			childNode.mu.Unlock()
			return syscallErrStatus(err)
		}
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

func (p *Server) GetLk(ctx context.Context, req *vfspb.GetLkRequest) (*vfspb.GetLkResponse, error) {
	flk := syscall.Flock_t{}
	fileLockFromProto(req.GetFileLock()).ToFlockT(&flk)
	if err := syscall.FcntlFlock(uintptr(req.GetHandleId()), _OFD_GETLK, &flk); err != nil {
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
		if err := syscall.Flock(int(req.GetHandleId()), op); err != nil {
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
	if err := syscall.FcntlFlock(uintptr(req.GetHandleId()), op, &flk); err != nil {
		return nil, syscallErrStatus(err)
	}
	return &vfspb.SetLkResponse{}, nil
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
