//go:build ((linux && !android) || (darwin && !ios)) && (amd64 || arm64)

package vfs_server

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/cache/dirtools"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	vfspb "github.com/buildbuddy-io/buildbuddy/proto/vfs"
	fusefs "github.com/hanwen/go-fuse/v2/fs"
	gstatus "google.golang.org/grpc/status"
)

// Files opened for reading that are smaller than this size are returned inline to avoid the overhead of additional
// Read RPCs.
const smallFileThresholdBytes = 512 * 1024

// Fuse operations
const (
	_OFD_GETLK  = 36
	_OFD_SETLK  = 37
	_OFD_SETLKW = 38
)

type fileHandle struct {
	mu        sync.Mutex
	f         *os.File
	openFlags uint32
}

type LazyFileProvider interface {
	// GetAllFilePaths should return all file paths that this provider can handle.
	// The list is only retrieved once during initialization.
	GetAllFilePaths() []*LazyFile

	GetRoot() *vfspb.DirectoryEntry

	// Place requests that the file in the VFS at path `relPath` should be written to the local filesystem at path
	// `fullPath`.
	Place(relPath, fullPath string) error
}

type LazyFile struct {
	Path   string
	Target string
	Size   int64
	Perms  fs.FileMode
}

type treeNode struct {
	file    *repb.FileNode
	symlink *repb.SymlinkNode
}

type CASLazyFileProvider struct {
	env                environment.Env
	ctx                context.Context
	remoteInstanceName string
	digestFunction     repb.DigestFunction_Value
	inputFiles         map[string]*treeNode
	root               *vfspb.DirectoryEntry
}

func NewCASLazyFileProvider(env environment.Env, ctx context.Context, remoteInstanceName string, digestFunction repb.DigestFunction_Value, inputTree *repb.Tree) (*CASLazyFileProvider, error) {
	_, dirMap, err := dirtools.DirMapFromTree(inputTree, digestFunction)
	if err != nil {
		return nil, err
	}

	numDirs := 0
	numFiles := 0
	numSymlinks := 0

	vroot := &vfspb.DirectoryEntry{}

	inputFiles := make(map[string]*treeNode)
	var walkDir func(dir *repb.Directory, path string, vdir *vfspb.DirectoryEntry) error
	walkDir = func(dir *repb.Directory, path string, vdir *vfspb.DirectoryEntry) error {
		numDirs++
		for _, childDirNode := range dir.GetDirectories() {
			childDir, ok := dirMap[digest.NewKey(childDirNode.Digest)]
			if !ok {
				return status.NotFoundErrorf("could not find dir %q", childDirNode.Digest)
			}
			childDirNode.GetName()
			vsub := &vfspb.DirectoryEntry{
				Name: childDirNode.GetName(),
				Attrs: &vfspb.Attrs{
					Size: 1000,
					Perm: uint32(0755),
				},
			}
			vdir.Directories = append(vdir.Directories, vsub)
			if err := walkDir(childDir, filepath.Join(path, childDirNode.GetName()), vsub); err != nil {
				return err
			}
		}
		for _, childFileNode := range dir.GetFiles() {
			perms := 0644
			if childFileNode.IsExecutable {
				perms |= 0111
			}
			vfile := &vfspb.FileEntry{
				Name: childFileNode.GetName(),
				Attrs: &vfspb.Attrs{
					Size:      childFileNode.GetDigest().GetSizeBytes(),
					Perm:      uint32(perms),
					Immutable: true,
				},
			}
			vdir.Files = append(vdir.Files, vfile)
			inputFiles[filepath.Join(path, childFileNode.GetName())] = &treeNode{file: childFileNode}
			numFiles++
		}
		for _, sl := range dir.GetSymlinks() {
			vlink := &vfspb.SymlinkEntry{
				Name: sl.GetName(),
				Attrs: &vfspb.Attrs{
					Size: 0,
					Perm: uint32(0644),
				},
				Target: sl.GetTarget(),
			}
			vdir.Symlinks = append(vdir.Symlinks, vlink)
			inputFiles[filepath.Join(path, sl.GetName())] = &treeNode{symlink: sl}
			numSymlinks++
		}
		return nil
	}

	err = walkDir(inputTree.Root, "", vroot)
	if err != nil {
		return nil, err
	}

	log.Infof("task contains %d directories, %d files and %d symlinks", numDirs, numFiles, numSymlinks)

	return &CASLazyFileProvider{
		env:                env,
		ctx:                ctx,
		remoteInstanceName: remoteInstanceName,
		digestFunction:     digestFunction,
		inputFiles:         inputFiles,
		root:               vroot,
	}, nil
}

func (p *CASLazyFileProvider) Place(relPath, fullPath string) error {
	treeNode, ok := p.inputFiles[relPath]
	if !ok {
		return status.NotFoundErrorf("unknown file %q", relPath)
	}
	if treeNode.file == nil {
		return status.InvalidArgumentErrorf("file %q is not a real file", relPath)
	}
	fileNode := treeNode.file
	ff := dirtools.NewBatchFileFetcher(p.ctx, p.env, p.remoteInstanceName, p.digestFunction)
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

func (p *CASLazyFileProvider) GetRoot() *vfspb.DirectoryEntry {
	return p.root
}

func (p *CASLazyFileProvider) GetAllFilePaths() []*LazyFile {
	var lazyFiles []*LazyFile
	for p, tn := range p.inputFiles {
		perms := 0644
		if tn.file != nil {
			if tn.file.GetIsExecutable() {
				perms |= 0111
			}
			lazyFiles = append(lazyFiles, &LazyFile{
				Path:  p,
				Size:  tn.file.GetDigest().GetSizeBytes(),
				Perms: fs.FileMode(perms),
			})
		} else if tn.symlink != nil {
			// XXX need to validate path...?
			lazyFiles = append(lazyFiles, &LazyFile{
				Path:   p,
				Size:   0,
				Perms:  fs.FileMode(perms),
				Target: tn.symlink.Target,
			})
		}
	}
	return lazyFiles
}

type DirectClient struct {
	s *Server
}

func NewDirectClient(s *Server) *DirectClient {
	return &DirectClient{s}
}

func (dc *DirectClient) GetLayout(ctx context.Context, in *vfspb.GetLayoutRequest, opts ...grpc.CallOption) (*vfspb.GetLayoutResponse, error) {
	return dc.s.GetLayout(ctx, in)
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

type fsNode struct {
	children map[string]*fsNode
	attrs    *vfspb.Attrs
}

type Server struct {
	env           environment.Env
	workspacePath string

	server *grpc.Server

	mu                 sync.Mutex
	root               *fsNode
	layoutRoot         *vfspb.DirectoryEntry
	remoteInstanceName string
	lazyFiles          map[string]*LazyFile
	lazyFileProvider   LazyFileProvider
	fileHandles        map[uint64]*fileHandle
}

func New(env environment.Env, workspacePath string) *Server {
	return &Server{
		env:           env,
		workspacePath: workspacePath,
		lazyFiles:     make(map[string]*LazyFile),
		fileHandles:   make(map[uint64]*fileHandle),
		root:          &fsNode{attrs: &vfspb.Attrs{Size: 0, Perm: 0755}},
	}
}

// computeLayout computes the tree representation of the workspace by iterating over existing files in the workspace
// and combining them with the lazy files in the `lazyFiles` map. The function returns a new lazy files map that does
// not include files that already present in the workspace.
func computeLayout(workspacePath string, lazyFiles map[string]*LazyFile) (*vfspb.DirectoryEntry, map[string]*LazyFile, error) {
	lazyFilesByDir := make(map[string]map[string]*LazyFile)
	for path, lazyFile := range lazyFiles {
		dir := filepath.Dir(path)
		if dir == "." {
			dir = ""
		}
		if lazyFilesByDir[dir] == nil {
			lazyFilesByDir[dir] = make(map[string]*LazyFile)
		}
		lazyFilesByDir[dir][filepath.Base(path)] = lazyFile
	}

	var walkDir func(path string, parent *vfspb.DirectoryEntry) error
	walkDir = func(relPath string, parent *vfspb.DirectoryEntry) error {
		path := filepath.Join(workspacePath, relPath)
		children, err := os.ReadDir(path)
		if err != nil {
			return err
		}
		lazyFilesInDir := lazyFilesByDir[relPath]
		for _, child := range children {
			childInfo, err := child.Info()

			if child.Type()&os.ModeSocket != 0 {
				continue
			}

			if child.Type().IsRegular() {
				if err != nil {
					return err
				}

				_, isLazyFile := lazyFilesInDir[child.Name()]

				fe := &vfspb.FileEntry{
					Name: child.Name(),
					Attrs: &vfspb.Attrs{
						Size:      childInfo.Size(),
						Perm:      uint32(childInfo.Mode().Perm()),
						Immutable: isLazyFile,
					},
				}
				parent.Files = append(parent.Files, fe)

				if isLazyFile {
					// Delete from list of lazy files if it already exists on disk.
					delete(lazyFilesInDir, child.Name())
				}

				continue
			}

			if child.Type()&os.ModeSymlink != 0 {
				target, err := os.Readlink(filepath.Join(path, child.Name()))
				if err != nil {
					return err
				}

				if strings.HasPrefix(target, "/") {
					if !strings.HasPrefix(filepath.Clean(target), workspacePath) {
						return status.PermissionDeniedErrorf("symlink target %q outside of workspace", target)
					}
					target = filepath.Join("/", strings.TrimPrefix(target, workspacePath))
				} else {
					if !strings.HasPrefix(filepath.Clean(filepath.Join(path, target)), workspacePath) {
						return status.PermissionDeniedErrorf("symlink target %q outside of workspace", target)
					}
				}

				se := &vfspb.SymlinkEntry{
					Name: child.Name(),
					Attrs: &vfspb.Attrs{
						Size: int64(len(target)),
						Perm: uint32(childInfo.Mode().Perm()),
					},
					Target: target,
				}
				parent.Symlinks = append(parent.Symlinks, se)
				continue
			}

			de := &vfspb.DirectoryEntry{
				Name: child.Name(),
				Attrs: &vfspb.Attrs{
					Size: childInfo.Size(),
					Perm: uint32(childInfo.Mode().Perm()),
				},
			}
			parent.Directories = append(parent.Directories, de)
			if err := walkDir(filepath.Join(relPath, child.Name()), de); err != nil {
				return err
			}
		}

		// XXX need to remove symlinks from lazyFilesInDir
		// Add in lazy files that do not exist on disk.
		for name, lazyFile := range lazyFilesInDir {
			if lazyFile.Target == "" {
				fe := &vfspb.FileEntry{
					Name: name,
					Attrs: &vfspb.Attrs{
						Size:      lazyFile.Size,
						Perm:      uint32(lazyFile.Perms),
						Immutable: true,
					},
				}
				parent.Files = append(parent.Files, fe)
			} else {
				fe := &vfspb.SymlinkEntry{
					Name:   name,
					Target: lazyFile.Target,
					Attrs: &vfspb.Attrs{
						Size:      lazyFile.Size,
						Perm:      uint32(lazyFile.Perms),
						Immutable: true,
					},
				}
				parent.Symlinks = append(parent.Symlinks, fe)
			}
		}
		return nil
	}

	root := &vfspb.DirectoryEntry{}
	err := walkDir("", root)
	if err != nil {
		return nil, nil, err
	}

	// Create new lazy files map that does not include files that are already on disk.
	newLazyFiles := make(map[string]*LazyFile)
	for dir, lazyFiles := range lazyFilesByDir {
		for name, lazyFile := range lazyFiles {
			newLazyFiles[filepath.Join(dir, name)] = lazyFile
		}
	}

	return root, newLazyFiles, nil
}

func (p *Server) GetLayout(ctx context.Context, request *vfspb.GetLayoutRequest) (*vfspb.GetLayoutResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return &vfspb.GetLayoutResponse{Root: p.layoutRoot}, nil
}

// Prepare is used to inform the VFS server about files that can be lazily loaded on the first open attempt.
// The list of lazy files is retrieved from the provider during preparation.
func (p *Server) Prepare(lazyFileProvider LazyFileProvider) error {
	log.Infof("Prepare")

	p.mu.Lock()
	defer p.mu.Unlock()
	p.lazyFileProvider = lazyFileProvider

	lazyFiles := make(map[string]*LazyFile)

	for _, lf := range lazyFileProvider.GetAllFilePaths() {
		lazyFiles[lf.Path] = lf
	}

	root := &fsNode{}
	var walkDir func(dir *vfspb.DirectoryEntry, node *fsNode)
	walkDir = func(dir *vfspb.DirectoryEntry, node *fsNode) {
		node.children = make(map[string]*fsNode)
		for _, subDir := range dir.GetDirectories() {
			subNode := &fsNode{attrs: subDir.Attrs}
			node.children[subDir.Name] = subNode
			walkDir(subDir, subNode)
		}
		for _, file := range dir.GetFiles() {
			subNode := &fsNode{attrs: file.Attrs}
			node.children[file.Name] = subNode
		}
		for _, link := range dir.GetSymlinks() {
			subNode := &fsNode{attrs: link.Attrs}
			node.children[link.Name] = subNode
		}
	}
	walkDir(lazyFileProvider.GetRoot(), root)

	p.root = root
	p.layoutRoot = lazyFileProvider.GetRoot()
	p.lazyFiles = lazyFiles

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

func (h *fileHandle) open(fullPath string, req *vfspb.OpenRequest) (*vfspb.OpenResponse, error) {
	flags := int(req.GetFlags())
	f, err := os.OpenFile(fullPath, flags, os.FileMode(req.GetMode()))
	if err != nil {
		return nil, syscallErrStatus(err)
	}
	h.mu.Lock()
	h.f = f
	h.openFlags = req.GetFlags()
	h.mu.Unlock()

	rsp := &vfspb.OpenResponse{}

	//if flags&(os.O_WRONLY|os.O_RDWR) == 0 {
	//	s, err := f.Stat()
	//	if err == nil && s.Size() <= smallFileThresholdBytes {
	//		buf := make([]byte, s.Size())
	//		n, err := f.Read(buf)
	//		if err == nil && int64(n) == s.Size() {
	//			rsp.Data = buf
	//		}
	//	}
	//}

	return rsp, nil
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
	fullPath, err := p.computeFullPath(request.GetPath())
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	parentDir, _ := filepath.Split(request.GetPath())
	_, err = p.lookupNode(parentDir)
	if err != nil {
		log.Infof("Open %q parent dir does not exist", request.GetPath())
		return nil, err
	}

	node, err := p.lookupNode(request.GetPath())
	if err != nil && int(request.Mode)&os.O_CREATE == 0 {
		log.Infof("Open %q file does not exist and not creating file", request.GetPath())
		return nil, err
	}

	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
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
	// We use the file descriptor as the file handle ID so that we can directly
	// use the flock/fcntl syscalls to implement file locking (SetLk, SetLkw,
	// GetLk). This is needed because file locking works on descriptors, not
	// paths.
	handleID := uint64(fh.f.Fd())
	rsp.HandleId = handleID
	p.mu.Lock()
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
	attrs := &vfspb.Attrs{
		Size: fi.Size(),
		Perm: uint32(fi.Mode().Perm()),
	}
	if stat, ok := fi.Sys().(*syscall.Stat_t); ok {
		attrs.Nlink = uint32(stat.Nlink)
	}
	return attrs, nil
}

func (p *Server) lookupNode(path string) (*fsNode, error) {
	node := p.root
	for _, name := range strings.Split(path, string(filepath.Separator)) {
		if name == "" {
			continue
		}
		log.Infof("Lookup node %q", name)
		subNode, ok := node.children[name]
		if !ok {
			log.Infof("Node %q not found", name)
			return nil, syscallErrStatus(syscall.ENOENT)
		}
		node = subNode
	}
	log.Infof("found node for %q: %+v", path, node.attrs)
	return node, nil
}

func (p *Server) GetAttr(ctx context.Context, request *vfspb.GetAttrRequest) (*vfspb.GetAttrResponse, error) {
	log.Infof("GetAttr %q", request.GetPath())
	p.mu.Lock()
	defer p.mu.Unlock()

	node, err := p.lookupNode(request.GetPath())
	if err != nil {
		return nil, err
	}

	return &vfspb.GetAttrResponse{Attrs: node.attrs}, nil
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
	newFullPath, err := p.computeFullPath(request.GetNewPath())
	if err != nil {
		return nil, err
	}

	st, err := os.Lstat(oldFullPath)
	if err != nil {
		return nil, syscallErrStatus(err)
	}
	// If this is a symlink, make sure that moving it does not make it point outside the workspace.
	if st.Mode()&os.ModeSymlink != 0 {
		symlinkTarget, err := os.Readlink(oldFullPath)
		if err != nil {
			return nil, syscallErrStatus(err)
		}
		if !strings.HasPrefix(symlinkTarget, "/") {
			newAbsTarget := filepath.Clean(filepath.Join(filepath.Dir(newFullPath), symlinkTarget))
			if !strings.HasPrefix(newAbsTarget, p.workspacePath) {
				return nil, status.PermissionDeniedErrorf("symlink would point outside the workspace")
			}
		}
	}

	if err := os.Rename(oldFullPath, newFullPath); err != nil {
		return nil, syscallErrStatus(err)
	}

	return &vfspb.RenameResponse{}, nil
}

func (p *Server) Mkdir(ctx context.Context, request *vfspb.MkdirRequest) (*vfspb.MkdirResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	parent, child := filepath.Split(request.GetPath())
	parentNode, err := p.lookupNode(parent)
	if err != nil {
		return nil, err
	}

	if _, ok := parentNode.children[child]; ok {
		return nil, syscallErrStatus(syscall.EEXIST)
	}

	if parentNode.children == nil {
		parentNode.children = make(map[string]*fsNode)
	}
	parentNode.children[child] = &fsNode{
		attrs: &vfspb.Attrs{
			Size: 1000,
			Perm: request.GetPerms(),
		},
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

func (p *Server) Link(ctx context.Context, request *vfspb.LinkRequest) (*vfspb.LinkResponse, error) {
	fullPath, err := p.computeFullPath(request.GetPath())
	if err != nil {
		return nil, err
	}
	targetFullPath, err := p.computeFullPath(request.GetTarget())
	if err != nil {
		return nil, err
	}

	if err := syscall.Link(targetFullPath, fullPath); err != nil {
		return nil, syscallErrStatus(err)
	}

	attrs, err := p.getAttr(fullPath)
	if err != nil {
		if err := syscall.Unlink(fullPath); err != nil {
			log.Warningf("Failed to unlink %q after stat failed during hardlink request: %s", fullPath, err)
		}
		return nil, err
	}

	return &vfspb.LinkResponse{Attrs: attrs}, nil
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
	p.server = grpc.NewServer(grpc_server.CommonGRPCServerOptions(p.env)...)
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
