//go:build linux && !android

package vfs_server

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfscommon"
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
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	vfspb "github.com/buildbuddy-io/buildbuddy/proto/vfs"
	vmfspb "github.com/buildbuddy-io/buildbuddy/proto/vmvfs"
	fusefs "github.com/hanwen/go-fuse/v2/fs"
	bspb "google.golang.org/genproto/googleapis/bytestream"
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

	// Guest content modes say where a guest VFS server gets the contents of
	// CAS inputs, and how.
	//
	// GuestContentRemote fetches each input from the remote CAS over gRPC,
	// through the host, and the guest keeps a copy on its own disk.
	GuestContentRemote = "remote"
	// GuestContentFileCache is like GuestContentRemote, except that the host
	// serves inputs from its file cache, filling it from the remote CAS on a
	// miss.
	GuestContentFileCache = "filecache"
	// GuestContentRaw is like GuestContentFileCache, but uses the raw content
	// protocol served by ServeGuestContent instead of gRPC.
	GuestContentRaw = "raw"
	// GuestContentDirect sends every read of an input over the raw content
	// protocol, and the guest keeps no copy of its own. The guest kernel's
	// page cache still holds what was read.
	GuestContentDirect = "direct"

	// The raw content protocol. The guest sends requests on a connection one
	// at a time, each a contentRequestHeaderSize header and then the input's
	// hash. The header holds the hash length, 1 if the input is executable
	// or else 0, and then, as big-endian int64s, the input's size, the offset
	// to read from and the number of bytes to read. The host answers with a
	// big-endian int64 n and n bytes of content, or, if it cannot, with -n
	// and an n-byte error message.
	contentRequestHeaderSize = 2 + 3*8
	contentChunkSize         = 1 << 20
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

// A range fetcher populates sparse input files before they are read. It must
// only be used with FUSE passthrough disabled.
type inputRangeFetcher interface {
	FetchRange(context.Context, *repb.FileNode, int64, int64) error
}

// InputReader reads part of a CAS input into buf and returns how many bytes
// it read, which is less than len(buf) only at the end of the input.
type InputReader func(ctx context.Context, node *repb.FileNode, buf []byte, offset int64) (int, error)

// WriteMirror receives every change that the server makes to the contents of
// a backing file, keyed by node ID, so that a copy of each file the action
// writes can be kept elsewhere. The server makes the calls for a node in the
// same order as the changes.
type WriteMirror interface {
	Write(id uint64, offset int64, data []byte) error
	Truncate(id uint64, size int64) error
	Allocate(id uint64, mode uint32, offset, length int64) error
}

type fileHandle struct {
	node         *fsNode
	rangeFetcher inputRangeFetcher
	// remote is set for handles of CAS inputs whose reads go to the
	// server's InputReader. These handles have no local file, so f is nil.
	remote InputReader
	// mirror, if set, receives the changes made through this handle.
	mirror WriteMirror

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
	server         *Server
	id             uint64
	nodeType       byte
	fileNode       *repb.FileNode
	target         string
	inputTimestamp uint64

	// writeMu is held across each change to the backing file's contents and
	// the call that mirrors it, so that the mirror sees the changes in order.
	writeMu sync.Mutex

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

// getAttrs initializes CAS attributes on first use. fsn.mu must be held.
func (fsn *fsNode) getAttrs() *vfspb.Attrs {
	if fsn.attrs == nil {
		perms := uint32(0644)
		if fsn.fileNode.GetIsExecutable() {
			perms |= 0111
		}
		fsn.attrs = &vfspb.Attrs{
			Size:       fsn.fileNode.GetDigest().GetSizeBytes(),
			Perm:       perms,
			Immutable:  true,
			Nlink:      1,
			MtimeNanos: fsn.inputTimestamp,
			AtimeNanos: fsn.inputTimestamp,
		}
	}
	return fsn.attrs
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
	oldBlocks := fsn.getAttrs().Blocks
	fsn.attrs = updateAttr(fsn.getAttrs(), func(attr *vfspb.Attrs) {
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
	now := time.Now()
	return &fsNode{
		nodeType: fsDirectoryNode,
		name:     name,
		attrs: &vfspb.Attrs{
			Size:       1000,
			Nlink:      1,
			Perm:       uint32(0755),
			MtimeNanos: uint64(now.UnixNano()),
			AtimeNanos: uint64(now.UnixNano()),
		},
		parent: parent,
	}
}

func newCASFileNode(parent *fsNode, refn *repb.FileNode, timestamp uint64) *fsNode {
	return &fsNode{
		nodeType:       fsFileNode,
		name:           refn.GetName(),
		fileNode:       refn,
		parent:         parent,
		inputTimestamp: timestamp,
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
	inputOpener      func(context.Context, *repb.FileNode) (*os.File, error)
	backingBlockSize int64

	server *grpc.Server

	mu                 sync.Mutex
	blocks             int64
	nextId             atomic.Uint64
	nodes              map[uint64]*fsNode
	internalTaskCtx    context.Context
	root               *fsNode
	inputFetcher       container.InputFetcher
	retryInputFetcher  *casFetcher
	remoteInstanceName string
	// prefetchMissCount is the number of CAS files opened during the current
	// task that the input fetcher had left out of its fetch.
	prefetchMissCount int64
	fileHandles       map[uint64]*fileHandle
	taskError         error

	// cacheFetcher fills the host file cache with the inputs that the guest
	// server or its outputs need. contentFromFileCache serves the guest's
	// ByteStream reads from the file cache instead of the remote cache.
	cacheFetcher         *casFetcher
	contentFromFileCache bool
	// inputReader, when set, serves reads of CAS inputs directly instead of
	// opening a local copy of each input. Handles opened this way have no
	// file descriptor, so they get IDs from nextRemoteHandleID, which starts
	// above any file descriptor.
	inputReader        InputReader
	nextRemoteHandleID atomic.Uint64
	// pruneStale makes Prepare remove entries that the new input tree does
	// not have, instead of merging the tree into the existing namespace.
	pruneStale bool
	// writebackCache is set when the FUSE client lets the kernel cache
	// writes, which changes how backing files must be opened.
	writebackCache bool
	// contentListener accepts raw content protocol connections from the
	// guest, if ServeGuestContent was called.
	contentListener net.Listener
	// What serving the guest's content requests has added up to. See
	// ContentStats.
	contentRequests, contentServeNanos, contentOpens, contentOpenNanos, contentFetches, contentFetchNanos atomic.Int64
	contentReadNanos, contentWriteNanos                                                                   atomic.Int64
	// writeMirror, if set, receives the changes to backing files' contents.
	writeMirror WriteMirror
	// mirrorDir is where the host keeps the files that a guest server
	// mirrors to it, one per node ID, and mirrorListener accepts the guest's
	// connections.
	mirrorDir      string
	mirrorListener net.Listener

	// total number of CAS files in the tree.
	casFileCount int64
	// total size of CAS files in the tree.
	casFileSizeBytes int64
}

func New(env environment.Env, workspacePath string) (*Server, error) {
	s, err := newServer(workspacePath)
	if err != nil {
		return nil, err
	}
	s.env = env
	s.inputOpener = func(ctx context.Context, node *repb.FileNode) (*os.File, error) {
		return env.GetFileCache().Open(ctx, node)
	}
	return s, nil
}

// NewLocal creates a VFS server whose CAS inputs are opened using inputOpener.
func NewLocal(workspacePath string, inputOpener func(context.Context, *repb.FileNode) (*os.File, error)) (*Server, error) {
	s, err := newServer(workspacePath)
	if err != nil {
		return nil, err
	}
	s.inputOpener = inputOpener
	return s, nil
}

// SetPruneStale makes each Prepare remove the entries that its input tree
// does not have, such as the last action's outputs, so that a reused VM sees
// only the new action's inputs. By default Prepare merges the tree into the
// existing namespace.
func (p *Server) SetPruneStale(prune bool) {
	p.mu.Lock()
	p.pruneStale = prune
	p.mu.Unlock()
}

// SetWritebackCache tells the server that its FUSE client mounts with the
// kernel's writeback cache. The kernel then reads pages of files that were
// opened write-only, to fill in the parts of a page it did not write, and it
// computes append offsets itself, so backing files are opened read-write and
// without O_APPEND.
func (p *Server) SetWritebackCache(enabled bool) {
	p.mu.Lock()
	p.writebackCache = enabled
	p.mu.Unlock()
}

// backingOpenFlags returns the flags to open a backing file with, for a FUSE
// open or create with the given flags. The write mirror also needs read
// access, to read back the data that copy_file_range copies.
func (p *Server) backingOpenFlags(flags uint32) uint32 {
	p.mu.Lock()
	writeback := p.writebackCache
	mirror := p.writeMirror != nil
	p.mu.Unlock()
	if (writeback || mirror) && flags&syscall.O_ACCMODE == syscall.O_WRONLY {
		flags = flags&^syscall.O_ACCMODE | syscall.O_RDWR
	}
	if writeback {
		flags &^= syscall.O_APPEND
	}
	return flags
}

// SetInputReader makes CAS inputs opened from now on read through r, or
// through the input opener again if r is nil.
func (p *Server) SetInputReader(r InputReader) {
	p.mu.Lock()
	p.inputReader = r
	p.mu.Unlock()
}

// SetWriteMirror makes the server send every change to the contents of a
// file the action writes to m, or stop if m is nil.
func (p *Server) SetWriteMirror(m WriteMirror) {
	p.mu.Lock()
	p.writeMirror = m
	p.mu.Unlock()
}

// mirrorError records a failure to mirror a change as the task's error,
// since the mirrored copy of the file is now wrong, and returns the errno for
// the process that made the change.
func (p *Server) mirrorError(err error) error {
	p.mu.Lock()
	if p.taskError == nil {
		p.taskError = status.UnavailableErrorf("mirror a write to the host: %s", err)
	}
	p.mu.Unlock()
	return syscall.EIO
}

func newServer(workspacePath string) (*Server, error) {
	rootNode := newDirNode(nil, "")
	rootNode.id = vfscommon.RootInodeId
	nodes := make(map[uint64]*fsNode)
	nodes[vfscommon.RootInodeId] = rootNode

	fs := unix.Statfs_t{}
	err := unix.Statfs(workspacePath, &fs)
	if err != nil {
		return nil, err
	}

	s := &Server{
		workspacePath:    workspacePath,
		backingBlockSize: fs.Bsize,
		fileHandles:      make(map[uint64]*fileHandle),
		root:             rootNode,
		nodes:            nodes,
	}
	s.nextId.Store(vfscommon.RootInodeId + 1)
	s.nextRemoteHandleID.Store(1 << 32)
	return s, nil
}

func (p *Server) Path() string {
	return p.workspacePath
}

// TaskError returns the first input download error exposed to the current task,
// or nil if no download failed after retries. Prepare clears this error.
func (p *Server) TaskError() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.taskError
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
	id := p.nextId.Add(1)
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

func (p *Server) updateLayout(ctx context.Context, inputTree *repb.Tree, digestFunction repb.DigestFunction_Value) (*vfscommon.InodeInvalidations, error) {
	_, dirMap, err := dirtools.DirMapFromTree(inputTree, digestFunction)
	if err != nil {
		return nil, err
	}
	// Reserve space on the initial preparation; later preparations merge with
	// the existing namespace. Shared subtrees can exceed this estimate.
	nodeCount := 1
	for _, dir := range dirMap {
		nodeCount += len(dir.GetDirectories()) + len(dir.GetFiles()) + len(dir.GetSymlinks())
	}
	p.mu.Lock()
	pruneStale := p.pruneStale
	if len(p.nodes) == 1 {
		p.nodes = make(map[uint64]*fsNode, nodeCount)
		p.nodes[p.root.id] = p.root
	}
	p.mu.Unlock()
	timestamp := uint64(time.Now().UnixNano())
	numDirs := 0
	numFiles := 0
	numSymlinks := 0

	newDirInodes := make(map[uint64]struct{})
	modifiedInodesSet := make(map[uint64]struct{})
	var invalidatedEntries []vfscommon.InodeEntryInvalidation

	removeExistingChild := func(parentNode *fsNode, existingChild *fsNode, childName string) error {
		parentNode.mu.Lock()
		parentNodeID := parentNode.id
		parentNode.mu.Unlock()
		// Since we're changing the layout w/o going through the kernel, we
		// will need to tell the kernel that this entry has changed.
		invalidatedEntries = append(invalidatedEntries, vfscommon.InodeEntryInvalidation{
			InodeID: parentNodeID,
			Name:    childName,
		})
		// Clean up the existing node.
		if err := unlink(parentNode, existingChild, childName); err != nil {
			return status.WrapErrorf(err, "error unlinking child %q from existing node %q during layout update", childName, existingChild.Path())
		}
		p.forgetNode(existingChild)
		return nil
	}

	addChild := func(parentNode *fsNode, childNode *fsNode, childName string) (uint64, error) {
		ino := p.addNode(childNode)
		parentNode.mu.Lock()
		modifiedInodesSet[parentNode.id] = struct{}{}
		parentNode.children[childName] = childNode
		parentNode.mu.Unlock()
		return ino, nil
	}

	// removeSubtree unlinks everything under node, so that the backing
	// files of anything the last action created there are deleted.
	var removeSubtree func(node *fsNode) error
	removeSubtree = func(node *fsNode) error {
		node.mu.Lock()
		children := maps.Clone(node.children)
		node.mu.Unlock()
		for name, child := range children {
			if err := removeSubtree(child); err != nil {
				return err
			}
			if err := unlink(node, child, name); err != nil {
				return err
			}
			p.forgetNode(child)
		}
		return nil
	}

	var walkDir func(dir *repb.Directory, parentNode *fsNode) error
	walkDir = func(dir *repb.Directory, parentNode *fsNode) error {
		numDirs++
		childCount := len(dir.GetDirectories()) + len(dir.GetFiles()) + len(dir.GetSymlinks())
		parentNode.mu.Lock()
		if parentNode.children == nil && childCount > 0 {
			parentNode.children = make(map[string]*fsNode, childCount)
		}
		parentNode.mu.Unlock()
		for _, childDirNode := range dir.GetDirectories() {
			childDir, ok := dirMap[digest.NewKey(childDirNode.Digest)]
			if !ok {
				if !digest.IsEmptyHash(childDirNode.Digest, digestFunction) {
					return status.NotFoundErrorf("could not find dir %q", childDirNode.Digest)
				}
				childDir = &repb.Directory{}
			}

			parentNode.mu.Lock()
			existingNode := parentNode.children[childDirNode.Name]
			parentNode.mu.Unlock()
			if existingNode != nil && !existingNode.IsDirectory() {
				if err := removeExistingChild(parentNode, existingNode, childDirNode.GetName()); err != nil {
					return err
				}
				existingNode = nil
			}

			childNode := existingNode
			if existingNode == nil {
				childNode = newDirNode(parentNode, childDirNode.GetName())
				inodeID, err := addChild(parentNode, childNode, childDirNode.GetName())
				if err != nil {
					return err
				}
				newDirInodes[inodeID] = struct{}{}
			}
			if err := walkDir(childDir, childNode); err != nil {
				return err
			}
		}
		for _, childFileNode := range dir.GetFiles() {
			parentNode.mu.Lock()
			existingNode := parentNode.children[childFileNode.GetName()]
			parentNode.mu.Unlock()
			if existingNode != nil && (!existingNode.IsFile() || !digest.Equal(existingNode.fileNode.GetDigest(), childFileNode.GetDigest()) || existingNode.fileNode.IsExecutable != childFileNode.IsExecutable || attrsChanged(existingNode)) {
				if err := removeExistingChild(parentNode, existingNode, childFileNode.GetName()); err != nil {
					return err
				}
				existingNode = nil
			}
			if existingNode == nil {
				fileNode := newCASFileNode(parentNode, childFileNode, timestamp)
				if _, err := addChild(parentNode, fileNode, childFileNode.GetName()); err != nil {
					return err
				}
			} else {
				// The node is reused for the new task, so forget that the
				// previous task opened it.
				existingNode.mu.Lock()
				existingNode.accessed = false
				existingNode.mu.Unlock()
			}
			numFiles++
		}
		for _, childSymlink := range dir.GetSymlinks() {
			parentNode.mu.Lock()
			existingNode := parentNode.children[childSymlink.GetName()]
			parentNode.mu.Unlock()
			if existingNode != nil && (!existingNode.IsSymlink() || existingNode.target != childSymlink.GetTarget()) {
				if err := removeExistingChild(parentNode, existingNode, childSymlink.GetName()); err != nil {
					return err
				}
				existingNode = nil
			}
			if existingNode == nil {
				symlinkNode := newSymlinkNode(parentNode, childSymlink.GetName(), childSymlink.GetTarget())
				if _, err := addChild(parentNode, symlinkNode, childSymlink.GetName()); err != nil {
					return err
				}
			}
			numSymlinks++
		}
		if !pruneStale {
			return nil
		}
		// Remove entries the new tree does not have, such as the last
		// action's outputs or inputs it no longer needs, so that a reused VM
		// sees only the new inputs.
		wanted := make(map[string]struct{}, childCount)
		for _, d := range dir.GetDirectories() {
			wanted[d.GetName()] = struct{}{}
		}
		for _, f := range dir.GetFiles() {
			wanted[f.GetName()] = struct{}{}
		}
		for _, l := range dir.GetSymlinks() {
			wanted[l.GetName()] = struct{}{}
		}
		parentNode.mu.Lock()
		stale := make(map[string]*fsNode)
		for name, child := range parentNode.children {
			if _, ok := wanted[name]; !ok {
				stale[name] = child
			}
		}
		parentNode.mu.Unlock()
		for name, child := range stale {
			if err := removeSubtree(child); err != nil {
				return err
			}
			if err := removeExistingChild(parentNode, child, name); err != nil {
				return err
			}
		}
		return nil
	}

	err = walkDir(inputTree.Root, p.root)
	if err != nil {
		return nil, err
	}

	log.CtxDebugf(ctx, "VFS contains %d directories, %d files and %d symlinks", numDirs, numFiles, numSymlinks)

	for ino := range newDirInodes {
		delete(modifiedInodesSet, ino)
	}

	modifiedInodes := make([]uint64, 0, len(modifiedInodesSet))
	for ino := range modifiedInodesSet {
		modifiedInodes = append(modifiedInodes, ino)
	}

	return &vfscommon.InodeInvalidations{
		Content: modifiedInodes,
		Entry:   invalidatedEntries,
	}, nil
}

// attrsChanged reports whether an input's attributes differ from those of a
// new node for it, other than its times, as when an action changed its mode.
// A reused VM must not see the last action's changes to its inputs.
func attrsChanged(n *fsNode) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.attrs == nil || n.backingPath != "" {
		return n.backingPath != ""
	}
	fresh := newCASFileNode(nil, n.fileNode, 0)
	cur := proto.Clone(n.attrs).(*vfspb.Attrs)
	want := proto.Clone(fresh.getAttrs()).(*vfspb.Attrs)
	cur.MtimeNanos, cur.AtimeNanos, want.MtimeNanos, want.AtimeNanos = 0, 0, 0, 0
	return !proto.Equal(cur, want)
}

// forgetNode drops a node that is no longer in the namespace from the node
// table, once no other link to it remains.
func (p *Server) forgetNode(n *fsNode) {
	n.mu.Lock()
	gone := n.attrs == nil || n.attrs.GetNlink() == 0
	id := n.id
	n.mu.Unlock()
	if gone {
		p.mu.Lock()
		delete(p.nodes, id)
		p.mu.Unlock()
	}
}

func cleanRelativePath(path string) (string, error) {
	path = filepath.Clean(path)
	if path == "." {
		return "", nil
	}
	if filepath.IsAbs(path) || path == ".." || strings.HasPrefix(path, ".."+string(filepath.Separator)) {
		return "", status.InvalidArgumentErrorf("path %q escapes the workspace", path)
	}
	return path, nil
}

func joinRelativePath(base, path string) (string, error) {
	base, err := cleanRelativePath(base)
	if err != nil {
		return "", err
	}
	if filepath.Clean(path) == "." {
		return "", status.InvalidArgumentError("output path cannot be empty")
	}
	if filepath.IsAbs(path) {
		return "", status.InvalidArgumentErrorf("path %q escapes the workspace", path)
	}
	return cleanRelativePath(filepath.Join(base, path))
}

func (p *Server) mkdirAll(path string) error {
	path, err := cleanRelativePath(path)
	if err != nil {
		return err
	}
	if path == "" {
		return nil
	}

	node := p.root
	for name := range strings.SplitSeq(path, string(filepath.Separator)) {
		node.mu.Lock()
		child := node.children[name]
		created := child == nil
		if child == nil {
			if node.children == nil {
				node.children = make(map[string]*fsNode)
			}
			child = newDirNode(node, name)
			node.children[name] = child
		}
		node.mu.Unlock()
		if created {
			p.addNode(child)
		}
		if !child.IsDirectory() {
			return status.FailedPreconditionErrorf("cannot create output directory %q: %q is not a directory", path, child.Path())
		}
		node = child
	}
	return nil
}

func outputPaths(layout *container.FileSystemLayout) []string {
	if len(layout.OutputPaths) > 0 {
		return layout.OutputPaths
	}
	return slices.Concat(layout.OutputFiles, layout.OutputDirectories)
}

func (p *Server) createOutputDirectories(layout *container.FileSystemLayout) error {
	for _, outputPath := range outputPaths(layout) {
		path, err := joinRelativePath(layout.WorkingDirectory, outputPath)
		if err != nil {
			return err
		}
		if err := p.mkdirAll(filepath.Dir(path)); err != nil {
			return err
		}
	}
	for _, outputDirectory := range layout.OutputDirectories {
		path, err := joinRelativePath(layout.WorkingDirectory, outputDirectory)
		if err != nil {
			return err
		}
		if err := p.mkdirAll(path); err != nil {
			return err
		}
	}
	return nil
}

func (p *Server) ComputeStats() *repb.VfsStats {
	stats := &repb.VfsStats{
		CasFilesCount:     p.casFileCount,
		CasFilesSizeBytes: p.casFileSizeBytes,
	}

	p.mu.Lock()
	stats.PrefetchMissCount = p.prefetchMissCount
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
	inputFetcher := p.inputFetcher
	retryInputFetcher := p.retryInputFetcher
	p.mu.Unlock()
	if statsProvider, ok := inputFetcher.(interface{ UpdateIOStats(*repb.VfsStats) }); ok {
		statsProvider.UpdateIOStats(stats)
	}
	if _, ok := inputFetcher.(*casFetcher); !ok && retryInputFetcher != nil {
		retryInputFetcher.UpdateIOStats(stats)
	}

	return stats
}

// UnusedInputs returns a TreeMask that excludes the CAS inputs not opened
// since the last Prepare call.
func (p *Server) UnusedInputs() *repb.TreeMask {
	p.mu.Lock()
	defer p.mu.Unlock()
	var paths []string
	var walkNode func(node *fsNode, path string)
	walkNode = func(node *fsNode, path string) {
		node.mu.Lock()
		if node.fileNode != nil && !node.accessed {
			paths = append(paths, path)
		}
		children := node.children
		node.mu.Unlock()
		for name, child := range children {
			walkNode(child, filepath.Join(path, name))
		}
	}
	walkNode(p.root, "")
	slices.Sort(paths)
	return &repb.TreeMask{ExcludedPaths: paths}
}

// Prepare is used to inform the VFS server about files that can be lazily loaded on the first open attempt.
func (p *Server) Prepare(ctx context.Context, layout *container.FileSystemLayout, inputFetcher container.InputFetcher) (*vfscommon.InodeInvalidations, error) {
	p.mu.Lock()
	p.casFileCount = 0
	p.casFileSizeBytes = 0
	p.prefetchMissCount = 0
	p.mu.Unlock()
	// There may already be nodes in the tree before `Prepare` is called. We
	// merge the known CAS inputs with the tree we already have.
	invalidatedInodes, err := p.updateLayout(ctx, layout.Inputs, layout.DigestFunction)
	if err != nil {
		return nil, err
	}
	if err := p.createOutputDirectories(layout); err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	var retryInputFetcher *casFetcher
	if p.env != nil {
		retryInputFetcher = newCASFetcher(p.env, layout.RemoteInstanceName, layout.DigestFunction)
	}
	if inputFetcher == nil {
		if retryInputFetcher == nil {
			return nil, status.FailedPreconditionError("no input fetcher is configured")
		}
		inputFetcher = retryInputFetcher
	}
	p.inputFetcher = inputFetcher
	p.retryInputFetcher = retryInputFetcher
	p.internalTaskCtx = ctx
	p.taskError = nil
	return invalidatedInodes, nil
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
	childNode.mu.Lock()
	defer childNode.mu.Unlock()
	return &vfspb.LookupResponse{
		Mode:          childNode.mode(),
		Attrs:         childNode.getAttrs(),
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
			Attrs: child.getAttrs(),
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

func (h *fileHandle) fetchRange(ctx context.Context, offset, length int64) error {
	if h.rangeFetcher == nil {
		return nil
	}
	return h.rangeFetcher.FetchRange(ctx, h.node.fileNode, offset, length)
}

func (h *fileHandle) read(ctx context.Context, req *vfspb.ReadRequest) (*vfspb.ReadResponse, error) {
	if req.GetOffset() < 0 || req.GetNumBytes() < 0 {
		return nil, syscallStatusEINVAL
	}
	buf := make([]byte, req.GetNumBytes())
	if h.remote != nil {
		n, err := h.remote(ctx, h.node.fileNode, buf, req.GetOffset())
		if err != nil {
			// As with downloads at open time, record the error so that it
			// fails the task even if the command ignores the failed read.
			p := h.node.server
			err = status.WrapErrorf(err, "read VFS input %q", h.node.Path())
			p.mu.Lock()
			if p.taskError == nil {
				p.taskError = err
			}
			p.mu.Unlock()
			log.CtxWarningf(p.taskCtx(), "%s", err)
			return nil, err
		}
		return &vfspb.ReadResponse{Data: buf[:n]}, nil
	}
	if err := h.fetchRange(ctx, req.GetOffset(), int64(req.GetNumBytes())); err != nil {
		return nil, err
	}
	h.mu.Lock()
	f := h.f
	h.mu.Unlock()

	n, err := f.ReadAt(buf, req.GetOffset())
	if err != nil && err != io.EOF {
		return nil, syscallErrStatus(err)
	}
	return &vfspb.ReadResponse{Data: buf[:n]}, nil
}

func (h *fileHandle) write(req *vfspb.WriteRequest) (*vfspb.WriteResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.remote != nil {
		return nil, syscallErrStatus(syscall.EBADF)
	}
	if h.mirror != nil {
		h.node.writeMu.Lock()
		defer h.node.writeMu.Unlock()
	}
	var n int
	var err error
	offset := req.GetOffset()
	if int(h.openFlags)&os.O_APPEND != 0 {
		n, err = h.f.Write(req.GetData())
		if err == nil && h.mirror != nil {
			var end int64
			end, err = h.f.Seek(0, io.SeekCurrent)
			offset = end - int64(n)
		}
	} else {
		n, err = h.f.WriteAt(req.GetData(), req.GetOffset())
	}

	if err != nil {
		return nil, syscallErrStatus(err)
	}
	if h.mirror != nil {
		if err := h.mirror.Write(h.node.id, offset, req.GetData()[:n]); err != nil {
			return nil, syscallErrStatus(h.node.server.mirrorError(err))
		}
	}
	return &vfspb.WriteResponse{NumBytes: uint32(n)}, nil
}

func (h *fileHandle) fsync(req *vfspb.FsyncRequest) (*vfspb.FsyncResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.remote != nil {
		return &vfspb.FsyncResponse{}, nil
	}
	if err := h.f.Sync(); err != nil {
		return nil, syscallErrStatus(err)
	}
	return &vfspb.FsyncResponse{}, nil
}

func (h *fileHandle) lseek(request *vfspb.LseekRequest) (*vfspb.LseekResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Missing cached ranges are not holes in the logical CAS file, and a
	// remote input has no local file to seek in.
	if (h.rangeFetcher != nil || h.remote != nil) && (request.GetWhence() == unix.SEEK_DATA || request.GetWhence() == unix.SEEK_HOLE) {
		size := uint64(h.node.fileNode.GetDigest().GetSizeBytes())
		if request.GetOffset() >= size {
			return nil, syscallErrStatus(syscall.ENXIO)
		}
		if request.GetWhence() == unix.SEEK_HOLE {
			return &vfspb.LseekResponse{Offset: size}, nil
		}
		return &vfspb.LseekResponse{Offset: request.GetOffset()}, nil
	}
	if h.remote != nil {
		switch request.GetWhence() {
		case unix.SEEK_SET:
			return &vfspb.LseekResponse{Offset: request.GetOffset()}, nil
		case unix.SEEK_END:
			return &vfspb.LseekResponse{Offset: uint64(h.node.fileNode.GetDigest().GetSizeBytes()) + request.GetOffset()}, nil
		}
		return nil, syscallErrStatus(syscall.EINVAL)
	}
	off, err := h.f.Seek(int64(request.GetOffset()), int(request.GetWhence()))
	if err != nil {
		return nil, syscallErrStatus(err)
	}
	return &vfspb.LseekResponse{Offset: uint64(off)}, nil
}

func (h *fileHandle) flush(req *vfspb.FlushRequest) (*vfspb.FlushResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.remote != nil {
		return &vfspb.FlushResponse{}, nil
	}

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

	if h.f != nil {
		if err := h.f.Close(); err != nil {
			return nil, syscallErrStatus(err)
		}
		h.f = nil
	}

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

	if h.remote != nil {
		return nil, syscallErrStatus(syscall.EBADF)
	}
	if h.mirror != nil {
		h.node.writeMu.Lock()
		defer h.node.writeMu.Unlock()
	}
	if err := syscall.Fallocate(int(h.f.Fd()), req.GetMode(), req.GetOffset(), req.GetNumBytes()); err != nil {
		return nil, syscallErrStatus(err)
	}
	if h.mirror != nil {
		if err := h.mirror.Allocate(h.node.id, req.GetMode(), req.GetOffset(), req.GetNumBytes()); err != nil {
			return nil, syscallErrStatus(h.node.server.mirrorError(err))
		}
	}
	return &vfspb.AllocateResponse{}, nil
}

func (p *Server) createNode(nodeType byte, backingPath string, mode uint32, parentNode *fsNode, name string, owner *vfspb.Owner) (*fsNode, error) {
	now := time.Now()

	node := &fsNode{
		nodeType: nodeType,
		name:     name,
		attrs: &vfspb.Attrs{
			Perm:       mode,
			Uid:        owner.GetUid(),
			Gid:        owner.GetGid(),
			Nlink:      1,
			MtimeNanos: uint64(now.UnixNano()),
			AtimeNanos: uint64(now.UnixNano()),
		},
		backingPath: backingPath,
		parent:      parentNode,
	}

	parentNode.mu.Lock()
	inheritSetgid(parentNode, node.attrs, false)
	if parentNode.children == nil {
		parentNode.children = make(map[string]*fsNode)
	}
	parentNode.children[name] = node
	parentNode.mu.Unlock()
	p.addNode(node)
	return node, nil
}

// inheritSetgid gives a new child the group of a setgid parent directory, and
// propagates the setgid bit to child directories. parent.mu must be held.
func inheritSetgid(parent *fsNode, attrs *vfspb.Attrs, isDir bool) {
	if parent.attrs.GetPerm()&syscall.S_ISGID == 0 {
		return
	}
	attrs.Gid = parent.attrs.GetGid()
	if isDir {
		attrs.Perm |= syscall.S_ISGID
	}
}

// checkChown applies the Linux rules for changing ownership: only root may
// change the owner, and the owner may only change the group to its own.
func checkChown(request *vfspb.SetAttrRequest, attrs *vfspb.Attrs) error {
	caller := request.GetCaller()
	if caller.GetUid() == 0 {
		return nil
	}
	isOwner := caller.GetUid() == attrs.GetUid()
	if request.Uid != nil && (!isOwner || request.GetUid() != attrs.GetUid()) {
		return syscall.EPERM
	}
	if request.Gid != nil && (!isOwner || (request.GetGid() != attrs.GetGid() && request.GetGid() != caller.GetGid())) {
		return syscall.EPERM
	}
	return nil
}

func (p *Server) createFile(ctx context.Context, mode uint32, parentNode *fsNode, name string, owner *vfspb.Owner) (*fsNode, error) {
	localFilePath, err := p.generateScratchPath(name)
	if err != nil {
		return nil, syscallErrStatus(err)
	}

	return p.createNode(fsFileNode, localFilePath, mode, parentNode, name, owner)
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

func (cf *casFetcher) downloadToFileCache(ctx context.Context, node *repb.FileNode) error {
	bsClient := cf.env.GetByteStreamClient()
	rn := digest.NewCASResourceName(node.GetDigest(), cf.remoteInstanceName, cf.digestFunction)
	rn.SetCompressor(repb.Compressor_ZSTD)
	w, err := cf.env.GetFileCache().Writer(ctx, node, cf.digestFunction)
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

func (cf *casFetcher) dedupeDownloadToFileCache(ctx context.Context, node *repb.FileNode) error {
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

	dedupeKey := groupIDStringFromContext(ctx) + "-" + node.GetDigest().GetHash()
	_, _, err := downloadDeduper.Do(ctx, dedupeKey, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, cf.downloadToFileCache(ctx, node)
	})
	return err
}

func (cf *casFetcher) Fetch(ctx context.Context, node *repb.FileNode) error {
	if cf.env.GetFileCache().ContainsFile(ctx, node) {
		return nil
	}

	if err := cf.dedupeDownloadToFileCache(ctx, node); err != nil {
		return err
	}

	cf.mu.Lock()
	// N.B. in case of deduping, the download will be counted in the stats of
	// all deduped actions.
	cf.downloadCount++
	cf.downloadSizeBytes += node.GetDigest().GetSizeBytes()
	cf.mu.Unlock()
	return nil
}

func (cf *casFetcher) UpdateIOStats(stats *repb.VfsStats) {
	cf.mu.Lock()
	defer cf.mu.Unlock()
	stats.FileDownloadSizeBytes += cf.downloadSizeBytes
	stats.FileDownloadCount += int64(cf.downloadCount)
	stats.FileDownloadDurationUsec += cf.downloadDuration.Microseconds()
}

func (p *Server) openCASFile(ctx context.Context, node *fsNode) (*os.File, error) {
	p.mu.Lock()
	inputFetcher := p.inputFetcher
	retryInputFetcher := p.retryInputFetcher
	inputOpener := p.inputOpener
	p.mu.Unlock()
	if inputFetcher == nil || inputOpener == nil {
		return nil, status.FailedPreconditionError("no input fetcher is configured")
	}
	if err := inputFetcher.Fetch(ctx, node.fileNode); err != nil {
		// The CAS downloader already handles retryable failures.
		if inputFetcher == retryInputFetcher || retryInputFetcher == nil {
			return nil, err
		}
	} else {
		f, err := inputOpener(ctx, node.fileNode)
		if err == nil {
			return f, nil
		}
		if retryInputFetcher == nil {
			return nil, err
		}
	}
	// A failed prefetcher cannot restart its downloads, so fetch directly
	// from the CAS if the prefetch failed. The file can also be missing from
	// the file cache because it was evicted after the fetch, or because the
	// input fetcher left it out as unused by a previous execution (see
	// dirtools.DownloadTreeOpts.UnusedInputsDigest) and returned without
	// fetching it. Count the latter case so that misses caused by leaving
	// out unused inputs can be measured separately from evictions.
	if prefetcher, ok := inputFetcher.(interface{ Prefetching(*repb.FileNode) bool }); ok && !prefetcher.Prefetching(node.fileNode) {
		p.mu.Lock()
		p.prefetchMissCount++
		p.mu.Unlock()
	}
	if err := retryInputFetcher.Fetch(ctx, node.fileNode); err != nil {
		return nil, err
	}
	return inputOpener(ctx, node.fileNode)
}

func (p *Server) lookupPath(path string) (*fsNode, error) {
	path, err := cleanRelativePath(path)
	if err != nil {
		return nil, err
	}
	node := p.root
	if path == "" {
		return node, nil
	}
	for name := range strings.SplitSeq(path, string(filepath.Separator)) {
		node.mu.Lock()
		child := node.children[name]
		node.mu.Unlock()
		if child == nil {
			return nil, nil
		}
		node = child
	}
	return node, nil
}

func copyFile(source *os.File, destination string, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(destination), 0755); err != nil {
		return err
	}
	if err := os.RemoveAll(destination); err != nil {
		return err
	}
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return err
	}
	destinationFile, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(destinationFile, source)
	closeErr := destinationFile.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func (p *Server) materializeNode(ctx context.Context, node *fsNode, destination string) error {
	node.mu.Lock()
	nodeType := node.nodeType
	mode := os.FileMode(node.getAttrs().GetPerm())
	target := node.target
	backingPath := node.backingPath
	children := make(map[string]*fsNode, len(node.children))
	maps.Copy(children, node.children)
	node.mu.Unlock()

	switch nodeType {
	case fsDirectoryNode:
		if info, err := os.Lstat(destination); err == nil && !info.IsDir() {
			if err := os.RemoveAll(destination); err != nil {
				return err
			}
		}
		if err := os.MkdirAll(destination, mode); err != nil {
			return err
		}
		for name, child := range children {
			if err := p.materializeNode(ctx, child, filepath.Join(destination, name)); err != nil {
				return err
			}
		}
		return nil
	case fsFileNode:
		var source *os.File
		var err error
		if backingPath != "" {
			source, err = os.Open(backingPath)
		} else {
			source, err = p.openCASFile(ctx, node)
		}
		if err != nil {
			return err
		}
		defer source.Close()
		if backingPath == "" {
			p.mu.Lock()
			fetcher, _ := p.inputFetcher.(inputRangeFetcher)
			p.mu.Unlock()
			if fetcher != nil {
				if err := fetcher.FetchRange(ctx, node.fileNode, 0, node.fileNode.GetDigest().GetSizeBytes()); err != nil {
					return err
				}
			}
		}
		return copyFile(source, destination, mode)
	case fsSymlinkNode:
		if err := os.MkdirAll(filepath.Dir(destination), 0755); err != nil {
			return err
		}
		if err := os.RemoveAll(destination); err != nil {
			return err
		}
		return os.Symlink(target, destination)
	default:
		return status.UnimplementedErrorf("cannot materialize VFS node type %d", nodeType)
	}
}

// MaterializeOutputs copies declared outputs into the action workspace.
func (p *Server) MaterializeOutputs(ctx context.Context, layout *container.FileSystemLayout, outputRoot string) error {
	for _, outputPath := range outputPaths(layout) {
		relativePath, err := joinRelativePath(layout.WorkingDirectory, outputPath)
		if err != nil {
			return err
		}
		node, err := p.lookupPath(relativePath)
		if err != nil {
			return err
		}
		if node == nil {
			continue
		}
		if err := p.materializeNode(ctx, node, filepath.Join(outputRoot, relativePath)); err != nil {
			return status.WrapErrorf(err, "materialize output %q", outputPath)
		}
	}
	return nil
}

func (p *Server) Mknod(ctx context.Context, request *vfspb.MknodRequest) (*vfspb.MknodResponse, error) {
	parentNode, err := p.lookupNode(request.GetParentId())
	if err != nil {
		return nil, err
	}

	if request.GetMode()&unix.S_IFREG != 0 {
		node, err := p.createFile(ctx, request.GetMode(), parentNode, request.GetName(), request.GetOwner())
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
		node, err := p.createNode(fsCharDevNode, "", request.GetMode(), parentNode, request.GetName(), request.GetOwner())
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

	node, err := p.createFile(ctx, request.GetMode(), parentNode, request.GetName(), request.GetOwner())
	if err != nil {
		log.CtxWarningf(p.taskCtx(), "Open %q could not create new file: %s", request.GetName(), err)
		return nil, err
	}

	flags := p.backingOpenFlags(request.GetFlags())
	file, err := os.OpenFile(node.backingPath, int(flags), os.FileMode(request.GetMode()))
	if err != nil {
		return nil, syscallErrStatus(err)
	}

	node.mu.Lock()
	id := node.id
	node.mu.Unlock()

	p.mu.Lock()
	mirror := p.writeMirror
	p.mu.Unlock()
	fh := &fileHandle{
		node:      node,
		f:         file,
		openFlags: flags,
		mirror:    mirror,
	}
	// We use the file descriptor as the file handle ID so that we can directly
	// use the flock/fcntl syscalls to implement file locking (SetLk, SetLkw,
	// GetLk). This is needed because file locking works on descriptors, not
	// paths.
	handleID := uint64(fh.f.Fd())
	p.mu.Lock()
	p.fileHandles[handleID] = fh
	p.mu.Unlock()
	return &vfspb.CreateResponse{Id: id, HandleId: handleID, Attrs: node.attrs}, nil
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
		node.attrs = updateAttr(node.getAttrs(), func(attr *vfspb.Attrs) {
			attr.AtimeNanos = uint64(time.Now().UnixNano())
		})
	}
	node.accessed = true
	node.mu.Unlock()
	p.mu.Lock()
	inputReader := p.inputReader
	p.mu.Unlock()
	if backingFile == "" && node.fileNode != nil && inputReader != nil && int(request.GetFlags())&(os.O_WRONLY|os.O_RDWR) == 0 {
		// Reads of this handle go to inputReader, so there is nothing to
		// open or download yet.
		fh := &fileHandle{node: node, remote: inputReader, openFlags: request.GetFlags()}
		handleID := p.nextRemoteHandleID.Add(1)
		p.mu.Lock()
		p.fileHandles[handleID] = fh
		p.mu.Unlock()
		return &vfspb.OpenResponse{HandleId: handleID}, nil
	}
	openFlags := request.GetFlags()
	var mirror WriteMirror
	if backingFile != "" {
		openFlags = p.backingOpenFlags(openFlags)
		p.mu.Lock()
		mirror = p.writeMirror
		p.mu.Unlock()
		truncate := mirror != nil && openFlags&syscall.O_TRUNC != 0
		if truncate {
			node.writeMu.Lock()
		}
		f, err := os.OpenFile(backingFile, int(openFlags), os.FileMode(request.GetFlags()))
		if err == nil && truncate {
			if mErr := mirror.Truncate(node.id, 0); mErr != nil {
				f.Close()
				err = p.mirrorError(mErr)
			}
		}
		if truncate {
			node.writeMu.Unlock()
		}
		if err != nil {
			log.CtxWarningf(p.taskCtx(), "Open %d could not open file %q: %s", request.GetId(), backingFile, err)
			return nil, syscallErrStatus(err)
		}
		openedFile = f
	} else if node.fileNode != nil {
		f, err := p.openCASFile(p.taskCtx(), node)
		if err != nil {
			// FUSE can only return an errno to the process. Record the gRPC
			// error here so that we can later return it, taking precedence
			// over any result that might be returned by command execution.
			if s := gstatus.FromContextError(err); s.Code() != codes.Unknown {
				err = s.Err()
			}
			err = status.WrapErrorf(gstatus.Convert(err).Err(), "download VFS input %q", node.Path())
			p.mu.Lock()
			if p.taskError == nil {
				p.taskError = err
			}
			p.mu.Unlock()
			// TODO: Cancel action execution here to avoid work whose result will
			// be discarded because of this error. Keep the original download error
			// as the action failure so cancellation does not hide its cause.
			log.CtxWarningf(p.taskCtx(), "Open %q could not fetch file from CAS: %s", node.Path(), err)
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
		openFlags: openFlags,
		mirror:    mirror,
	}
	if backingFile == "" && node.fileNode != nil {
		p.mu.Lock()
		fh.rangeFetcher, _ = p.inputFetcher.(inputRangeFetcher)
		p.mu.Unlock()
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
	return fh.read(ctx, request)
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
	return &vfspb.GetAttrResponse{Attrs: node.getAttrs()}, nil
}

func (p *Server) processSetAttr(node *fsNode, file *os.File, request *vfspb.SetAttrRequest, newAttrs *vfspb.Attrs) error {
	// Ownership is tracked virtually; backing files are owned by the executor.
	if err := checkChown(request, newAttrs); err != nil {
		return err
	}
	if request.Uid != nil {
		newAttrs.Uid = request.GetUid()
	}
	if request.Gid != nil {
		newAttrs.Gid = request.GetGid()
	}
	if request.SetPerms != nil {
		newAttrs.Perm = request.SetPerms.Perms
	}

	if request.SetSize != nil {
		p.mu.Lock()
		mirror := p.writeMirror
		p.mu.Unlock()
		if mirror != nil {
			node.writeMu.Lock()
			defer node.writeMu.Unlock()
		}
		if file != nil {
			// The backing path may no longer exist if the file was unlinked.
			if err := file.Truncate(request.SetSize.GetSize()); err != nil {
				return err
			}
		} else if node.backingPath == "" {
			return syscall.EPERM
		} else if err := os.Truncate(node.backingPath, request.SetSize.GetSize()); err != nil {
			return err
		}
		if mirror != nil {
			if err := mirror.Truncate(node.id, request.SetSize.GetSize()); err != nil {
				return p.mirrorError(err)
			}
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

	var file *os.File
	if request.HandleId != nil {
		fh, err := p.getFileHandle(request.GetHandleId())
		if err != nil {
			return nil, err
		}
		fh.mu.Lock()
		defer fh.mu.Unlock()
		if fh.node != node {
			return nil, syscallErrStatus(syscall.EBADF)
		}
		file = fh.f
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	// Not using updateAttr here to avoid putting all the setattr logic
	// inside a nested function.
	newAttrs := proto.Clone(node.getAttrs()).(*vfspb.Attrs)

	if request.SetExtended != nil || request.RemoveExtended != nil {
		if err := p.processSetExtendedAttr(node, request, newAttrs); err != nil {
			return nil, syscallErrStatus(err)
		}
	} else {
		if err := p.processSetAttr(node, file, request, newAttrs); err != nil {
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
	var replacedID uint64
	if newExists {
		replacedID = newChildNode.id
		if err := unlink(newParentNode, newChildNode, request.GetNewName()); err != nil {
			return nil, err
		}
	}

	// If RENAME_WHITEOUT flag is present, we need to create a character
	// device where the old node used to be.
	if request.GetFlags()&unix.RENAME_WHITEOUT != 0 {
		_, err := p.createNode(fsCharDevNode, "", 0644, oldParentNode, request.GetOldName(), nil)
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

	return &vfspb.RenameResponse{ReplacedId: replacedID}, nil
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
			Uid:  request.GetOwner().GetUid(),
			Gid:  request.GetOwner().GetGid(),
		},
		parent: parentNode,
	}
	inheritSetgid(parentNode, newNode.attrs, true)
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
	newAttrs := updateAttr(existingNode.getAttrs(), func(attr *vfspb.Attrs) {
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
	node.attrs.Uid = request.GetOwner().GetUid()
	node.attrs.Gid = request.GetOwner().GetGid()
	inheritSetgid(parentNode, node.attrs, false)
	parentNode.children[request.GetName()] = node
	id := p.addNode(node)
	return &vfspb.SymlinkResponse{Id: id, Attrs: node.attrs}, nil
}

func unlink(parentNode *fsNode, childNode *fsNode, childName string) error {
	childNode.mu.Lock()
	childNode.attrs = updateAttr(childNode.getAttrs(), func(attr *vfspb.Attrs) {
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
		lockPath := ""
		flags := syscall.O_RDWR
		if fh.f != nil {
			lockPath = fh.f.Name()
		} else {
			// A remote input handle has no local file, so lock a per-node
			// stand-in file instead. Every handle of the node locks the
			// same file, which gives the same exclusion as locking the input.
			if err := os.MkdirAll(filepath.Join(p.workspacePath, ".locks"), 0755); err != nil {
				return 0, syscallErrStatus(err)
			}
			lockPath = filepath.Join(p.workspacePath, ".locks", fmt.Sprint(fh.node.id))
			flags |= syscall.O_CREAT
		}
		lockFD, err := syscall.Open(lockPath, flags, 0644)
		if err != nil {
			log.CtxWarningf(p.taskCtx(), "could not open file %q for locking: %s", lockPath, err)
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
	if rh.remote != nil {
		// A remote input has no file descriptor to copy from, so read the
		// range and write it instead, at most 16 MiB at a time. The caller
		// copies the rest with further calls.
		rsp, err := rh.read(ctx, &vfspb.ReadRequest{Offset: int64(request.GetReadHandleOffset()), NumBytes: int32(min(request.GetNumBytes(), 16<<20))})
		if err != nil {
			return nil, err
		}
		wrsp, err := wh.write(&vfspb.WriteRequest{Data: rsp.GetData(), Offset: int64(request.GetWriteHandleOffset())})
		if err != nil {
			return nil, err
		}
		return &vfspb.CopyFileRangeResponse{NumBytesCopied: wrsp.GetNumBytes()}, nil
	}
	if err := rh.fetchRange(ctx, int64(request.GetReadHandleOffset()), int64(request.GetNumBytes())); err != nil {
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
	if wh.mirror != nil {
		wh.node.writeMu.Lock()
		defer wh.node.writeMu.Unlock()
	}

	writeOffset := int64(request.GetWriteHandleOffset())
	n, err := unix.CopyFileRange(int(rh.f.Fd()), &request.ReadHandleOffset, int(wh.f.Fd()), &request.WriteHandleOffset, int(request.GetNumBytes()), int(request.GetFlags()))
	if err != nil {
		return nil, syscallErrStatus(err)
	}
	if wh.mirror != nil && n > 0 {
		// The kernel copied the data without the server seeing it, so read
		// it back to mirror it.
		data := make([]byte, n)
		if _, err := wh.f.ReadAt(data, writeOffset); err != nil {
			return nil, syscallErrStatus(p.mirrorError(err))
		}
		if err := wh.mirror.Write(wh.node.id, writeOffset, data); err != nil {
			return nil, syscallErrStatus(p.mirrorError(err))
		}
	}
	return &vfspb.CopyFileRangeResponse{NumBytesCopied: uint32(n)}, nil
}

func (p *Server) Statfs(ctx context.Context, request *vfspb.StatfsRequest) (*vfspb.StatfsResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	totalBlocks := reportedSizeBytes / p.backingBlockSize

	// We don't enforce a usage limit yet so used blocks may go over the
	// total blocks.
	free := max(totalBlocks-p.blocks, 0)

	return &vfspb.StatfsResponse{
		BlockSize:       p.backingBlockSize,
		TotalBlocks:     uint64(totalBlocks),
		BlocksFree:      uint64(free),
		BlocksAvailable: uint64(free),
	}, nil
}

func (p *Server) Start(lis net.Listener) error {
	p.mu.Lock()
	opts := append(grpc_server.CommonGRPCServerOptions(p.env), grpc.ForceServerCodecV2(&getTreeProxyCodec{encoding.GetCodecV2("proto")}))
	p.server = grpc.NewServer(opts...)
	p.mu.Unlock()
	vfspb.RegisterFileSystemServer(p.server, p)
	repb.RegisterContentAddressableStorageServer(p.server, &contentAddressableStorageProxy{vfsServer: p})
	bspb.RegisterByteStreamServer(p.server, &byteStreamProxy{vfsServer: p})
	go func() {
		_ = p.server.Serve(lis)
	}()
	return nil
}

// PrepareContentProxy authorizes CAS proxy requests using ctx without building
// a host-side VFS namespace. mode is one of the guest content modes.
func (p *Server) PrepareContentProxy(ctx context.Context, layout *container.FileSystemLayout, mode string) {
	p.mu.Lock()
	p.internalTaskCtx = ctx
	p.cacheFetcher = newCASFetcher(p.env, layout.RemoteInstanceName, layout.DigestFunction)
	p.contentFromFileCache = mode != GuestContentRemote
	p.mu.Unlock()
}

// openContent opens a CAS input from the host file cache, downloading it
// there first if it is missing. The file cache keeps executable and other
// inputs apart, so a miss tries the other kind before downloading.
func (p *Server) openContent(ctx context.Context, d *repb.Digest, executable bool) (*os.File, error) {
	p.mu.Lock()
	fetcher := p.cacheFetcher
	p.mu.Unlock()
	if fetcher == nil {
		return nil, status.FailedPreconditionError("the guest VFS content proxy is not prepared")
	}
	fc := p.env.GetFileCache()
	node := &repb.FileNode{Digest: d, IsExecutable: executable}
	if f, err := fc.Open(ctx, node); err == nil {
		return f, nil
	}
	if f, err := fc.Open(ctx, &repb.FileNode{Digest: d, IsExecutable: !executable}); err == nil {
		return f, nil
	}
	start := time.Now()
	err := fetcher.Fetch(ctx, node)
	p.contentFetches.Add(1)
	p.contentFetchNanos.Add(int64(time.Since(start)))
	if err != nil {
		return nil, err
	}
	return fc.Open(ctx, node)
}

// ContentStats describes the raw content protocol requests served since the
// server was created. Requests is how many there were and ServeNanos how long
// they took in total, from reading a request to finishing its response. Opens
// is how many of them had to open their input, and Fetches how many of those
// found it missing from the file cache and downloaded it, which took
// FetchNanos in total. ReadNanos is how long reading the inputs took, and
// WriteNanos how long writing the responses to the guest took.
type ContentStats struct {
	Requests, ServeNanos, Opens, OpenNanos, Fetches, FetchNanos int64
	ReadNanos, WriteNanos                                       int64
}

// ContentStats returns what serving guest content requests has added up to.
func (p *Server) ContentStats() ContentStats {
	return ContentStats{
		Requests:   p.contentRequests.Load(),
		ServeNanos: p.contentServeNanos.Load(),
		Opens:      p.contentOpens.Load(),
		OpenNanos:  p.contentOpenNanos.Load(),
		Fetches:    p.contentFetches.Load(),
		FetchNanos: p.contentFetchNanos.Load(),
		ReadNanos:  p.contentReadNanos.Load(),
		WriteNanos: p.contentWriteNanos.Load(),
	}
}

// ServeGuestContent answers raw content protocol requests on lis from the
// host file cache, until lis is closed.
func (p *Server) ServeGuestContent(lis net.Listener) {
	p.mu.Lock()
	p.contentListener = lis
	p.mu.Unlock()
	for {
		conn, err := lis.Accept()
		if err != nil {
			return
		}
		go p.serveGuestContentConn(conn)
	}
}

func (p *Server) serveGuestContentConn(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	buf := make([]byte, contentChunkSize)
	var hdr [contentRequestHeaderSize]byte
	var rspHdr [8]byte
	// Keep the last input open, since large inputs are read with many
	// requests in a row.
	var f *os.File
	var fHash string
	defer func() {
		if f != nil {
			f.Close()
		}
	}()
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return
		}
		hash := make([]byte, hdr[0])
		if _, err := io.ReadFull(r, hash); err != nil {
			return
		}
		size := int64(binary.BigEndian.Uint64(hdr[2:]))
		offset := int64(binary.BigEndian.Uint64(hdr[10:]))
		length := int64(binary.BigEndian.Uint64(hdr[18:]))
		requestStart := time.Now()
		p.contentRequests.Add(1)
		if f == nil || fHash != string(hash) {
			if f != nil {
				f.Close()
				f = nil
			}
			var err error
			f, err = p.openContent(p.taskCtx(), &repb.Digest{Hash: string(hash), SizeBytes: size}, hdr[1] == 1)
			p.contentOpens.Add(1)
			p.contentOpenNanos.Add(int64(time.Since(requestStart)))
			if err != nil {
				msg := []byte(err.Error())
				binary.BigEndian.PutUint64(rspHdr[:], uint64(-int64(len(msg))))
				if _, err := (&net.Buffers{rspHdr[:], msg}).WriteTo(conn); err != nil {
					return
				}
				continue
			}
			fHash = string(hash)
		}
		n := max(0, min(length, size-offset))
		binary.BigEndian.PutUint64(rspHdr[:], uint64(n))
		// Send the header with the first chunk, so that a small input
		// takes a single write.
		out := net.Buffers{rspHdr[:]}
		for n > 0 || len(out) > 0 {
			k := min(n, int64(len(buf)))
			if k > 0 {
				readStart := time.Now()
				m, err := f.ReadAt(buf[:k], offset)
				p.contentReadNanos.Add(int64(time.Since(readStart)))
				if int64(m) < k {
					// The response has started, so the guest cannot tell an
					// error from content. Closing the connection fails its
					// request instead.
					log.CtxWarningf(p.taskCtx(), "Read guest content %q: %s", hash, err)
					return
				}
				out = append(out, buf[:k])
			}
			writeStart := time.Now()
			_, err := out.WriteTo(conn)
			p.contentWriteNanos.Add(int64(time.Since(writeStart)))
			if err != nil {
				return
			}
			out = nil
			offset += k
			n -= k
		}
		p.contentServeNanos.Add(int64(time.Since(requestStart)))
	}
}

// WriteContentRequest writes a raw content protocol request for length bytes
// of node at offset.
func WriteContentRequest(w io.Writer, node *repb.FileNode, offset, length int64) error {
	hash := node.GetDigest().GetHash()
	// Send the request in a single write, since each write to a vsock
	// connection becomes a separate packet for the VMM to handle.
	req := make([]byte, contentRequestHeaderSize, contentRequestHeaderSize+len(hash))
	req[0] = byte(len(hash))
	if node.GetIsExecutable() {
		req[1] = 1
	}
	binary.BigEndian.PutUint64(req[2:], uint64(node.GetDigest().GetSizeBytes()))
	binary.BigEndian.PutUint64(req[10:], uint64(offset))
	binary.BigEndian.PutUint64(req[18:], uint64(length))
	_, err := w.Write(append(req, hash...))
	return err
}

// ReadContentResponseHeader reads the header of a raw content protocol
// response and returns how many bytes of content follow it.
func ReadContentResponseHeader(r io.Reader) (int64, error) {
	var hdr [8]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, err
	}
	n := int64(binary.BigEndian.Uint64(hdr[:]))
	if n >= 0 {
		return n, nil
	}
	msg := make([]byte, -n)
	if _, err := io.ReadFull(r, msg); err != nil {
		return 0, err
	}
	return 0, status.UnavailableErrorf("host content request: %s", msg)
}

type contentAddressableStorageProxy struct {
	repb.UnimplementedContentAddressableStorageServer
	vfsServer *Server
}

// Forward serialized GetTree responses without decoding or coalescing buffers.
// Other messages use the normal protobuf codec.
type getTreeProxyCodec struct {
	encoding.CodecV2
}

func (c *getTreeProxyCodec) Marshal(v any) (mem.BufferSlice, error) {
	if data, ok := v.(*mem.BufferSlice); ok {
		data.Ref() // gRPC owns the returned references until transmission completes.
		return *data, nil
	}
	return c.CodecV2.Marshal(v)
}

func (c *getTreeProxyCodec) Unmarshal(data mem.BufferSlice, v any) error {
	if out, ok := v.(*mem.BufferSlice); ok {
		data.Ref() // Keep the receive buffers alive until the proxy forwards them.
		*out = data
		return nil
	}
	return c.CodecV2.Unmarshal(data, v)
}

func (p *contentAddressableStorageProxy) GetTree(req *repb.GetTreeRequest, stream repb.ContentAddressableStorage_GetTreeServer) error {
	client := p.vfsServer.env.GetContentAddressableStorageClient()
	if client == nil {
		return status.FailedPreconditionError("content addressable storage client is not configured")
	}
	ctx, cancel := context.WithCancel(p.vfsServer.taskCtx())
	defer cancel()
	stop := context.AfterFunc(stream.Context(), cancel)
	defer stop()
	remote, err := client.GetTree(ctx, req, grpc.ForceCodecV2(&getTreeProxyCodec{encoding.GetCodecV2("proto")}))
	if err != nil {
		return err
	}
	for {
		var data mem.BufferSlice
		err := remote.RecvMsg(&data)
		if err != nil {
			data.Free()
			if err == io.EOF {
				return nil
			}
			return err
		}
		err = stream.SendMsg(&data)
		data.Free()
		if err != nil {
			return err
		}
	}
}

type byteStreamProxy struct {
	bspb.UnimplementedByteStreamServer
	vfsServer *Server
}

func (p *byteStreamProxy) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	p.vfsServer.mu.Lock()
	fromFileCache := p.vfsServer.contentFromFileCache
	p.vfsServer.mu.Unlock()
	if fromFileCache {
		return p.readFromFileCache(req, stream)
	}
	client := p.vfsServer.env.GetByteStreamClient()
	if client == nil {
		return status.FailedPreconditionError("ByteStream client is not configured")
	}
	remote, err := client.Read(p.vfsServer.taskCtx(), req)
	if err != nil {
		return err
	}
	for {
		rsp, err := remote.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(rsp); err != nil {
			return err
		}
	}
}

// taskProxyContext returns a context for forwarding a guest request: it has
// the task's credentials, and is canceled when the guest's request is.
func (p *Server) taskProxyContext(guestCtx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(p.taskCtx())
	stop := context.AfterFunc(guestCtx, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

func (p *contentAddressableStorageProxy) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	client := p.vfsServer.env.GetContentAddressableStorageClient()
	if client == nil {
		return nil, status.FailedPreconditionError("content addressable storage client is not configured")
	}
	ctx, cancel := p.vfsServer.taskProxyContext(ctx)
	defer cancel()
	return client.FindMissingBlobs(ctx, req)
}

func (p *contentAddressableStorageProxy) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	client := p.vfsServer.env.GetContentAddressableStorageClient()
	if client == nil {
		return nil, status.FailedPreconditionError("content addressable storage client is not configured")
	}
	ctx, cancel := p.vfsServer.taskProxyContext(ctx)
	defer cancel()
	return client.BatchUpdateBlobs(ctx, req)
}

func (p *byteStreamProxy) Write(stream bspb.ByteStream_WriteServer) error {
	client := p.vfsServer.env.GetByteStreamClient()
	if client == nil {
		return status.FailedPreconditionError("ByteStream client is not configured")
	}
	ctx, cancel := p.vfsServer.taskProxyContext(stream.Context())
	defer cancel()
	remote, err := client.Write(ctx)
	if err != nil {
		return err
	}
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		// The cache ends the stream early if it already has the blob, and
		// CloseAndRecv then returns its response.
		if err := remote.Send(req); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}
	rsp, err := remote.CloseAndRecv()
	if err != nil {
		return err
	}
	return stream.SendAndClose(rsp)
}

func (p *byteStreamProxy) readFromFileCache(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	rn, err := digest.ParseDownloadResourceName(req.GetResourceName())
	if err != nil {
		return err
	}
	if rn.GetCompressor() != repb.Compressor_IDENTITY {
		return status.InvalidArgumentError("compressed reads are not served from the file cache")
	}
	f, err := p.vfsServer.openContent(p.vfsServer.taskCtx(), rn.GetDigest(), false)
	if err != nil {
		return err
	}
	defer f.Close()
	offset := req.GetReadOffset()
	n := rn.GetDigest().GetSizeBytes() - offset
	if req.GetReadLimit() > 0 {
		n = min(n, req.GetReadLimit())
	}
	buf := make([]byte, min(max(n, 0), contentChunkSize))
	for n > 0 {
		k, err := f.ReadAt(buf[:min(n, int64(len(buf)))], offset)
		if err != nil && err != io.EOF {
			return err
		}
		if k == 0 {
			return status.DataLossError("file cache input is shorter than its digest")
		}
		if err := stream.Send(&bspb.ReadResponse{Data: buf[:k]}); err != nil {
			return err
		}
		offset += int64(k)
		n -= int64(k)
	}
	return nil
}

// WalkOutputs calls fn for each file, directory and symlink under the
// layout's output paths, with each directory before its contents. For a file
// the action wrote, backingPath is where its contents are, and for an input
// the action left alone, the entry has the input's digest.
func (p *Server) WalkOutputs(layout *container.FileSystemLayout, fn func(entry *vmfspb.OutputEntry, backingPath string) error) error {
	for _, outputPath := range outputPaths(layout) {
		relativePath, err := joinRelativePath(layout.WorkingDirectory, outputPath)
		if err != nil {
			return err
		}
		node, err := p.lookupPath(relativePath)
		if err != nil {
			return err
		}
		if node == nil {
			continue
		}
		if err := walkOutputNode(node, relativePath, fn); err != nil {
			return status.WrapErrorf(err, "walk output %q", outputPath)
		}
	}
	return nil
}

func walkOutputNode(node *fsNode, path string, fn func(*vmfspb.OutputEntry, string) error) error {
	node.mu.Lock()
	entry := &vmfspb.OutputEntry{Path: path, Perms: node.getAttrs().GetPerm()}
	backingPath := node.backingPath
	var children map[string]*fsNode
	switch node.nodeType {
	case fsDirectoryNode:
		entry.Type = vmfspb.OutputEntry_DIRECTORY
		children = maps.Clone(node.children)
	case fsFileNode:
		entry.Type = vmfspb.OutputEntry_FILE
		if backingPath == "" {
			entry.InputDigest = node.fileNode.GetDigest()
			entry.Size = node.fileNode.GetDigest().GetSizeBytes()
		} else {
			entry.NodeId = node.id
		}
	case fsSymlinkNode:
		entry.Type = vmfspb.OutputEntry_SYMLINK
		entry.SymlinkTarget = node.target
	default:
		nodeType := node.nodeType
		node.mu.Unlock()
		return status.UnimplementedErrorf("cannot return VFS node type %d as an output", nodeType)
	}
	node.mu.Unlock()
	if entry.GetType() == vmfspb.OutputEntry_FILE {
		if backingPath != "" {
			info, err := os.Stat(backingPath)
			if err != nil {
				return err
			}
			entry.Size = info.Size()
		} else if entry.GetInputDigest() == nil {
			return status.FailedPreconditionErrorf("output file %q has no contents", path)
		}
	}
	if err := fn(entry, backingPath); err != nil {
		return err
	}
	for _, name := range slices.Sorted(maps.Keys(children)) {
		if err := walkOutputNode(children[name], filepath.Join(path, name), fn); err != nil {
			return err
		}
	}
	return nil
}

// WriteOutputs creates the output entries that recv returns under
// outputRoot, until recv returns io.EOF, and returns how many files and bytes
// it wrote. An input the action left alone comes from the host file cache. A
// file the action wrote comes from the messages after its entry, or, if
// mirrored is set, from the file that its mirrored writes went to.
func (p *Server) WriteOutputs(ctx context.Context, recv func() (*vmfspb.ReadOutputsResponse, error), outputRoot string, mirrored bool) (int64, int64, error) {
	p.mu.Lock()
	mirrorDir := p.mirrorDir
	p.mu.Unlock()
	var fileCount, byteCount int64
	// f receives the contents of the current file, which has remaining bytes
	// still to come.
	var f *os.File
	var current string
	var remaining int64
	finishFile := func() error {
		if f == nil {
			return nil
		}
		err := f.Close()
		f = nil
		if remaining != 0 {
			return status.DataLossErrorf("output %q is missing %d bytes", current, remaining)
		}
		return err
	}
	defer func() {
		if f != nil {
			f.Close()
		}
	}()
	for {
		rsp, err := recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, 0, err
		}
		if e := rsp.GetEntry(); e != nil {
			if err := finishFile(); err != nil {
				return 0, 0, err
			}
			if !filepath.IsLocal(e.GetPath()) {
				return 0, 0, status.InvalidArgumentErrorf("output path %q is not under the workspace", e.GetPath())
			}
			current = e.GetPath()
			destination := filepath.Join(outputRoot, e.GetPath())
			mode := os.FileMode(e.GetPerms()) & os.ModePerm
			if e.GetType() == vmfspb.OutputEntry_DIRECTORY {
				if info, err := os.Lstat(destination); err == nil && !info.IsDir() {
					if err := os.RemoveAll(destination); err != nil {
						return 0, 0, err
					}
				}
				if err := os.MkdirAll(destination, mode); err != nil {
					return 0, 0, err
				}
				continue
			}
			if err := os.MkdirAll(filepath.Dir(destination), 0755); err != nil {
				return 0, 0, err
			}
			if err := os.RemoveAll(destination); err != nil {
				return 0, 0, err
			}
			switch {
			case e.GetType() == vmfspb.OutputEntry_SYMLINK:
				if err := os.Symlink(e.GetSymlinkTarget(), destination); err != nil {
					return 0, 0, err
				}
			case e.GetType() != vmfspb.OutputEntry_FILE:
				return 0, 0, status.InvalidArgumentErrorf("output %q has unknown type %s", e.GetPath(), e.GetType())
			case e.GetInputDigest() != nil:
				source, err := p.openContent(ctx, e.GetInputDigest(), mode&0111 != 0)
				if err != nil {
					return 0, 0, status.WrapErrorf(err, "open input for output %q", e.GetPath())
				}
				err = copyFile(source, destination, mode)
				source.Close()
				if err != nil {
					return 0, 0, err
				}
			case mirrored:
				if err := linkMirroredOutput(filepath.Join(mirrorDir, strconv.FormatUint(e.GetNodeId(), 10)), destination, e); err != nil {
					return 0, 0, err
				}
			default:
				f, err = os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
				if err != nil {
					return 0, 0, err
				}
				remaining = e.GetSize()
			}
			if e.GetType() == vmfspb.OutputEntry_FILE {
				fileCount++
				byteCount += e.GetSize()
			}
		}
		if data := rsp.GetData(); len(data) > 0 {
			if f == nil || int64(len(data)) > remaining {
				return 0, 0, status.InvalidArgumentErrorf("unexpected contents after output %q", current)
			}
			if _, err := f.Write(data); err != nil {
				return 0, 0, err
			}
			remaining -= int64(len(data))
		}
	}
	if err := finishFile(); err != nil {
		return 0, 0, err
	}
	return fileCount, byteCount, nil
}

// linkMirroredOutput puts the file that a guest server mirrored at
// destination. A file the action created but never wrote to has no mirrored
// file, so it is created empty.
func linkMirroredOutput(mirrorPath, destination string, e *vmfspb.OutputEntry) error {
	mode := os.FileMode(e.GetPerms()) & os.ModePerm
	info, err := os.Stat(mirrorPath)
	if os.IsNotExist(err) && e.GetSize() == 0 {
		f, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
		if err != nil {
			return err
		}
		return f.Close()
	}
	if err != nil {
		return err
	}
	if info.Size() != e.GetSize() {
		return status.DataLossErrorf("mirrored output %q has %d bytes, but the guest has %d", e.GetPath(), info.Size(), e.GetSize())
	}
	// An output that is a hard link of another one links the same file.
	if err := os.Link(mirrorPath, destination); err != nil {
		return err
	}
	return os.Chmod(destination, mode)
}

// The mirror protocol carries a guest server's changes to the files an action
// writes over a connection to the host. Each frame is a header of an op, a
// node ID, two 8-byte arguments and a 4-byte argument, followed, for
// mirrorWrite, by the data.
const (
	// mirrorWrite writes the length that follows at an offset.
	mirrorWrite = 'W'
	// mirrorTruncate sets the size.
	mirrorTruncate = 'T'
	// mirrorAllocate calls fallocate with an offset, length and mode.
	mirrorAllocate = 'A'
	// mirrorSync asks the host to reply once it has applied every frame
	// before it, with the length of an error message and the message, which
	// is empty if every change was applied.
	mirrorSync = 'S'

	mirrorHeaderSize = 1 + 8 + 8 + 8 + 4
	// maxMirrorWrite is the most data a mirrorWrite frame carries.
	maxMirrorWrite = 1 << 20
)

// MirrorClient is a WriteMirror that sends a guest server's changes to the
// host over a mirror protocol connection.
type MirrorClient struct {
	mu   sync.Mutex
	conn net.Conn
	// err is the first error sending to conn, after which every call fails.
	err error
}

func NewMirrorClient(conn net.Conn) *MirrorClient {
	return &MirrorClient{conn: conn}
}

func (m *MirrorClient) send(op byte, id uint64, a, b int64, c uint32, data []byte) error {
	frame := make([]byte, mirrorHeaderSize, mirrorHeaderSize+len(data))
	frame[0] = op
	binary.BigEndian.PutUint64(frame[1:], id)
	binary.BigEndian.PutUint64(frame[9:], uint64(a))
	binary.BigEndian.PutUint64(frame[17:], uint64(b))
	binary.BigEndian.PutUint32(frame[25:], c)
	// Send each frame in a single write, since each write to a vsock
	// connection becomes a separate packet for the VMM to handle.
	frame = append(frame, data...)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	if _, err := m.conn.Write(frame); err != nil {
		m.err = err
	}
	return m.err
}

func (m *MirrorClient) Write(id uint64, offset int64, data []byte) error {
	for len(data) > 0 {
		n := min(len(data), maxMirrorWrite)
		if err := m.send(mirrorWrite, id, offset, int64(n), 0, data[:n]); err != nil {
			return err
		}
		data = data[n:]
		offset += int64(n)
	}
	return nil
}

func (m *MirrorClient) Truncate(id uint64, size int64) error {
	return m.send(mirrorTruncate, id, size, 0, 0, nil)
}

func (m *MirrorClient) Allocate(id uint64, mode uint32, offset, length int64) error {
	return m.send(mirrorAllocate, id, offset, length, mode, nil)
}

// Sync waits until the host has applied every change sent before it, and
// returns the first error the host hit applying them.
func (m *MirrorClient) Sync() error {
	if err := m.send(mirrorSync, 0, 0, 0, 0, nil); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var hdr [8]byte
	if _, err := io.ReadFull(m.conn, hdr[:]); err != nil {
		m.err = err
		return err
	}
	msg := make([]byte, binary.BigEndian.Uint64(hdr[:]))
	if _, err := io.ReadFull(m.conn, msg); err != nil {
		m.err = err
		return err
	}
	if len(msg) > 0 {
		return status.UnavailableErrorf("host could not apply mirrored writes: %s", msg)
	}
	return nil
}

func (m *MirrorClient) Close() error {
	return m.conn.Close()
}

// SetMirrorDir sets the directory where the files that the guest mirrors
// from now on are kept, one per node ID.
func (p *Server) SetMirrorDir(dir string) {
	p.mu.Lock()
	p.mirrorDir = dir
	p.mu.Unlock()
}

// ServeGuestMirror applies the changes that guest servers mirror over
// connections from lis, until lis is closed.
func (p *Server) ServeGuestMirror(lis net.Listener) {
	p.mu.Lock()
	p.mirrorListener = lis
	p.mu.Unlock()
	for {
		conn, err := lis.Accept()
		if err != nil {
			return
		}
		go p.serveGuestMirrorConn(conn)
	}
}

func (p *Server) serveGuestMirrorConn(conn net.Conn) {
	defer conn.Close()
	// The guest connects once per task, after the host sets its directory.
	p.mu.Lock()
	dir := p.mirrorDir
	p.mu.Unlock()
	files := make(map[uint64]*os.File)
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()
	open := func(id uint64) (*os.File, error) {
		if f, ok := files[id]; ok {
			return f, nil
		}
		if dir == "" {
			return nil, status.FailedPreconditionError("no mirror directory is set")
		}
		f, err := os.OpenFile(filepath.Join(dir, strconv.FormatUint(id, 10)), os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		files[id] = f
		return f, nil
	}
	// applyErr is the first error applying a change. Later frames are still
	// read, to keep reading at frame boundaries, but not applied.
	var applyErr error
	r := bufio.NewReaderSize(conn, 2*maxMirrorWrite)
	buf := make([]byte, maxMirrorWrite)
	var hdr [mirrorHeaderSize]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return
		}
		id := binary.BigEndian.Uint64(hdr[1:])
		a := int64(binary.BigEndian.Uint64(hdr[9:]))
		b := int64(binary.BigEndian.Uint64(hdr[17:]))
		c := binary.BigEndian.Uint32(hdr[25:])
		switch hdr[0] {
		case mirrorWrite:
			if b < 0 || b > maxMirrorWrite {
				log.Warningf("Mirrored write of %d bytes is too large", b)
				return
			}
			if _, err := io.ReadFull(r, buf[:b]); err != nil {
				return
			}
			if applyErr == nil {
				f, err := open(id)
				if err == nil {
					_, err = f.WriteAt(buf[:b], a)
				}
				applyErr = err
			}
		case mirrorTruncate:
			if applyErr == nil {
				f, err := open(id)
				if err == nil {
					err = f.Truncate(a)
				}
				applyErr = err
			}
		case mirrorAllocate:
			if applyErr == nil {
				f, err := open(id)
				if err == nil {
					err = unix.Fallocate(int(f.Fd()), c, a, b)
				}
				applyErr = err
			}
		case mirrorSync:
			var msg []byte
			if applyErr != nil {
				msg = []byte(applyErr.Error())
			}
			rsp := binary.BigEndian.AppendUint64(nil, uint64(len(msg)))
			if _, err := conn.Write(append(rsp, msg...)); err != nil {
				return
			}
		default:
			log.Warningf("Unknown mirror op %q", hdr[0])
			return
		}
	}
}

func (p *Server) Stop() {
	p.mu.Lock()
	for _, fh := range p.fileHandles {
		_, _ = fh.release(&vfspb.ReleaseRequest{})
	}
	server := p.server
	contentListener := p.contentListener
	mirrorListener := p.mirrorListener
	p.mu.Unlock()
	if server != nil {
		server.Stop()
	}
	if contentListener != nil {
		contentListener.Close()
	}
	if mirrorListener != nil {
		mirrorListener.Close()
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
