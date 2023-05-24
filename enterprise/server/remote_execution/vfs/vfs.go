package vfs

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/docker/go-units"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	vfspb "github.com/buildbuddy-io/buildbuddy/proto/vfs"
	gstatus "google.golang.org/grpc/status"
)

// opStats tracks statistics for a single FUSE OP type.
type opStats struct {
	count     int
	timeSpent time.Duration
}

type VFS struct {
	vfsClient  vfspb.FileSystemClient
	mountDir   string
	verbose    bool
	logFUSEOps bool

	server *fuse.Server // Not set until FS is mounted.

	root *Node

	mu             sync.Mutex
	internalTaskID string
	rpcCtx         context.Context
	opStats        map[string]*opStats
}

type Options struct {
	// Verbose enables logging of most per-file operations.
	Verbose bool
	// LogFUSEOps enabled logging of all operations received by the go fuse server from the kernel.
	LogFUSEOps bool
}

func New(vfsClient vfspb.FileSystemClient, mountDir string, options *Options) *VFS {
	// This is so we always have a context set. The real context will be set in the PrepareForTask call.
	rpcCtx, cancel := context.WithCancel(context.Background())
	cancel()

	vfs := &VFS{
		vfsClient:  vfsClient,
		mountDir:   mountDir,
		verbose:    options.Verbose,
		logFUSEOps: options.LogFUSEOps,
		rpcCtx:     rpcCtx,
		opStats:    make(map[string]*opStats),
	}
	root := &Node{vfs: vfs}
	vfs.root = root
	return vfs
}

func (vfs *VFS) getRPCContext() context.Context {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()
	return vfs.rpcCtx
}

func (vfs *VFS) GetMountDir() string {
	return vfs.mountDir
}

type latencyRecorder struct {
	vfs *VFS
}

func (r *latencyRecorder) Add(name string, dt time.Duration) {
	r.vfs.mu.Lock()
	defer r.vfs.mu.Unlock()
	stats, ok := r.vfs.opStats[name]
	if !ok {
		stats = &opStats{}
		r.vfs.opStats[name] = stats
	}
	stats.count++
	stats.timeSpent += dt
}

func (vfs *VFS) Mount() error {
	if vfs.server != nil {
		return status.FailedPreconditionError("VFS already mounted")
	}

	// The filesystem is mutated only through the FUSE filesystem so we can let the kernel cache node and attribute
	// information for an arbitrary amount of time.
	nodeAttrTimeout := 6 * time.Hour
	opts := &fs.Options{
		EntryTimeout: &nodeAttrTimeout,
		AttrTimeout:  &nodeAttrTimeout,
		MountOptions: fuse.MountOptions{
			AllowOther:    true,
			Debug:         vfs.logFUSEOps,
			DisableXAttrs: true,
			// Don't depend on the `fusermount` binary to make things simpler under Firecracker.
			DirectMount: true,
			FsName:      "bbvfs",
			MaxWrite:    fuse.MAX_KERNEL_WRITE,
			EnableLocks: true,
		},
	}
	nodeFS := fs.NewNodeFS(vfs.root, opts)
	server, err := fuse.NewServer(nodeFS, vfs.mountDir, &opts.MountOptions)
	if err != nil {
		return status.UnavailableErrorf("could not mount VFS at %q: %s", vfs.mountDir, err)
	}

	server.RecordLatencies(&latencyRecorder{vfs: vfs})

	go server.Serve()
	if err := server.WaitMount(); err != nil {
		return status.UnavailableErrorf("waiting for VFS mount failed: %s", err)
	}

	vfs.server = server

	return nil
}

// PrepareForTask prepares the virtual filesystem layout according to the provided `fsLayout`.
// `ctx` is used for outgoing RPCs to the server and must stay alive as long as the filesystem is being used.
// The passed `taskID` os only used for logging purposes.
func (vfs *VFS) PrepareForTask(ctx context.Context, taskID string) error {
	vfs.mu.Lock()
	vfs.internalTaskID = taskID
	vfs.rpcCtx = ctx
	vfs.mu.Unlock()

	rsp, err := vfs.vfsClient.GetLayout(ctx, &vfspb.GetLayoutRequest{})
	if err != nil {
		return err
	}

	var walkDir func(dir *vfspb.DirectoryEntry, node *Node) error
	walkDir = func(dir *vfspb.DirectoryEntry, parentNode *Node) error {
		for _, childDirNode := range dir.GetDirectories() {
			child := &Node{
				vfs:         vfs,
				parent:      parentNode,
				cachedAttrs: childDirNode.Attrs,
				immutable:   childDirNode.GetAttrs().GetImmutable(),
			}
			inode := vfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFDIR})
			if !parentNode.AddChild(childDirNode.Name, inode, false) {
				return status.UnknownErrorf("could not add child %q to %q, already exists", childDirNode.Name, parentNode.relativePath())
			}
			if vfs.verbose {
				log.Debugf("[%s] Input directory: %s", vfs.taskID(), child.relativePath())
			}
			if err := walkDir(childDirNode, child); err != nil {
				return err
			}
		}
		for _, childFileNode := range dir.GetFiles() {
			child := &Node{
				vfs:         vfs,
				parent:      parentNode,
				cachedAttrs: childFileNode.Attrs,
				immutable:   childFileNode.GetAttrs().GetImmutable(),
			}
			inode := vfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFREG})
			if !parentNode.AddChild(childFileNode.Name, inode, false) {
				return status.UnknownErrorf("could not add child %q to %q, already exists", childFileNode.Name, parentNode.relativePath())
			}
			if vfs.verbose {
				log.Debugf("[%s] Input file: %s", vfs.taskID(), child.relativePath())
			}
		}
		for _, childSymlinkNode := range dir.GetSymlinks() {
			child := &Node{
				vfs:           vfs,
				parent:        parentNode,
				symlinkTarget: childSymlinkNode.GetTarget(),
				cachedAttrs:   childSymlinkNode.Attrs,
				immutable:     childSymlinkNode.GetAttrs().GetImmutable(),
			}
			inode := vfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFLNK})
			if !parentNode.AddChild(childSymlinkNode.Name, inode, false) {
				return status.UnknownErrorf("could not add child %q to %q, already exists", childSymlinkNode.Name, parentNode.relativePath())
			}
			if vfs.verbose {
				log.Debugf("[%s] Input symlink: %s", vfs.taskID(), child.relativePath())
			}
		}
		return nil
	}

	err = walkDir(rsp.Root, vfs.root)
	if err != nil {
		return err
	}

	return nil
}

func (vfs *VFS) taskID() string {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()
	return vfs.internalTaskID
}

func (vfs *VFS) FinishTask() error {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	// TODO(vadim): propagate stats to ActionResult
	if vfs.verbose {
		var totalTime time.Duration
		log.Debugf("[%s] OP stats:", vfs.internalTaskID)
		for op, s := range vfs.opStats {
			log.Infof("[%s] %-20s num_calls=%-08d time=%.2fs", vfs.internalTaskID, op, s.count, s.timeSpent.Seconds())
			totalTime += s.timeSpent
		}
		log.Debugf("[%s] Total time spent in OPs: %s", vfs.internalTaskID, totalTime)
	}

	vfs.internalTaskID = "unset"

	return nil
}

func (vfs *VFS) Unmount() error {
	if vfs.server == nil {
		return nil
	}

	return vfs.server.Unmount()
}

type Node struct {
	fs.Inode

	vfs       *VFS
	parent    *Node
	immutable bool

	mu            sync.Mutex
	cachedAttrs   *vfspb.Attrs
	symlinkTarget string
}

func (n *Node) relativePath() string {
	return n.Path(nil)
}

func (n *Node) resetCachedAttrs() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cachedAttrs = nil
}

type remoteFile struct {
	ctx       context.Context
	vfsClient vfspb.FileSystemClient
	path      string
	id        uint64
	node      *Node
	// Optional file contents if the server returned the entire file contents inline as part of the Open RPC.
	data []byte

	mu         sync.Mutex
	readBytes  int
	readRPCs   int
	wroteBytes int
	writeRPCs  int
}

// rpcErrToSyscallErrno extracts the syscall errno passed via RPC error status.
// Returns a generic EIO errno if the error does not contain syscall err info.
func rpcErrToSyscallErrno(rpcErr error) syscall.Errno {
	if status.IsCanceledError(rpcErr) {
		return syscall.EINTR
	}

	s, ok := gstatus.FromError(rpcErr)
	if !ok {
		return syscall.EIO
	}
	for _, d := range s.Details() {
		if se, ok := d.(*vfspb.SyscallError); ok {
			return syscall.Errno(se.Errno)
		}
	}
	return syscall.EIO
}

func (f *remoteFile) Allocate(ctx context.Context, off uint64, size uint64, mode uint32) syscall.Errno {
	f.node.resetCachedAttrs()
	allocReq := &vfspb.AllocateRequest{
		HandleId: f.id,
		Mode:     mode,
		Offset:   int64(off),
		NumBytes: int64(size),
	}
	if _, err := f.vfsClient.Allocate(f.ctx, allocReq); err != nil {
		return rpcErrToSyscallErrno(err)
	}
	return fs.OK
}

func (f *remoteFile) Flush(ctx context.Context) syscall.Errno {
	if f.node.vfs.verbose {
		f.mu.Lock()
		log.Debugf("[%s] Flush %q, read %s (%d RPCs), wrote %s (%d RPCs)", f.node.vfs.taskID(), f.path, units.HumanSize(float64(f.readBytes)), f.readRPCs, units.HumanSize(float64(f.wroteBytes)), f.writeRPCs)
		f.mu.Unlock()
	}

	if _, err := f.vfsClient.Flush(f.ctx, &vfspb.FlushRequest{HandleId: f.id}); err != nil {
		return rpcErrToSyscallErrno(err)
	}
	return fs.OK
}

func (f *remoteFile) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	if _, err := f.vfsClient.Fsync(f.ctx, &vfspb.FsyncRequest{HandleId: f.id}); err != nil {
		return rpcErrToSyscallErrno(err)
	}
	return fs.OK
}

func (f *remoteFile) Release(ctx context.Context) syscall.Errno {
	if f.node.vfs.verbose {
		f.mu.Lock()
		log.Debugf("[%s] Release %q, read %s (%d RPCs), wrote %s (%d RPCs)", f.node.vfs.taskID(), f.path, units.HumanSize(float64(f.readBytes)), f.readRPCs, units.HumanSize(float64(f.wroteBytes)), f.writeRPCs)
		f.mu.Unlock()
	}
	if _, err := f.vfsClient.Release(f.ctx, &vfspb.ReleaseRequest{HandleId: f.id}); err != nil {
		return rpcErrToSyscallErrno(err)
	}
	return fs.OK
}

type remoteFileReader struct {
	f        *remoteFile
	offset   int64
	numBytes int
}

func (r *remoteFileReader) Bytes(buf []byte) ([]byte, fuse.Status) {
	numBytes := r.numBytes
	if len(buf) < numBytes {
		numBytes = len(buf)
	}

	// If the file contents was returned inline as part of the Open RPC, read from it directly instead of making
	// additional RPCs.
	if r.f.data != nil {
		dataSize := int64(len(r.f.data))
		if r.offset >= dataSize {
			return nil, fuse.EIO
		}
		if int64(numBytes) > dataSize-r.offset {
			numBytes = int(dataSize - r.offset)
		}
		return r.f.data[r.offset : r.offset+int64(numBytes)], fuse.OK
	}

	readReq := &vfspb.ReadRequest{
		HandleId: r.f.id,
		Offset:   r.offset,
		NumBytes: int32(numBytes),
	}
	rsp, err := r.f.vfsClient.Read(r.f.ctx, readReq)
	if err != nil {
		return nil, fuse.ToStatus(rpcErrToSyscallErrno(err))
	}
	r.f.mu.Lock()
	r.f.readRPCs++
	r.f.readBytes += len(rsp.GetData())
	r.f.mu.Unlock()
	return rsp.GetData(), fuse.OK
}

func (r *remoteFileReader) Size() int {
	return r.numBytes
}

func (r *remoteFileReader) Done() {
}

func (f *remoteFile) Read(ctx context.Context, buf []byte, off int64) (res fuse.ReadResult, errno syscall.Errno) {
	res = &remoteFileReader{f: f, offset: off, numBytes: len(buf)}
	return
}

func (f *remoteFile) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	f.node.resetCachedAttrs()
	writeReq := &vfspb.WriteRequest{
		HandleId: f.id,
		Data:     data,
		Offset:   off,
	}
	rsp, err := f.vfsClient.Write(f.ctx, writeReq)
	if err != nil {
		return 0, rpcErrToSyscallErrno(err)
	}
	f.mu.Lock()
	f.writeRPCs++
	f.wroteBytes += int(rsp.GetNumBytes())
	f.mu.Unlock()
	return rsp.GetNumBytes(), 0
}

func openRemoteFile(ctx context.Context, vfsClient vfspb.FileSystemClient, path string, flags uint32, mode uint32, node *Node) (*remoteFile, error) {
	req := &vfspb.OpenRequest{
		Path:  path,
		Flags: flags,
		Mode:  mode,
	}
	rsp, err := vfsClient.Open(ctx, req)
	if err != nil {
		return nil, rpcErrToSyscallErrno(err)
	}
	return &remoteFile{
		ctx:       ctx,
		vfsClient: vfsClient,
		path:      path,
		id:        rsp.GetHandleId(),
		node:      node,
		data:      rsp.GetData(),
	}, nil
}

func (n *Node) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	// Don't allow writes to input files.
	if n.immutable && (int(flags)&(os.O_WRONLY|os.O_RDWR)) != 0 {
		log.Warningf("[%s] Denied attempt to write to immutable file %q", n.vfs.taskID(), n.relativePath())
		return nil, 0, syscall.EPERM
	}

	if n.vfs.verbose {
		log.Debugf("[%s] Open %q", n.vfs.taskID(), n.relativePath())
	}

	rf, err := openRemoteFile(n.vfs.getRPCContext(), n.vfs.vfsClient, n.relativePath(), flags, 0, n)
	if err != nil {
		return nil, 0, rpcErrToSyscallErrno(err)
	}
	return rf, 0, 0
}

func (n *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if n.vfs.verbose {
		log.Debugf("[%s] Create %q", n.vfs.taskID(), filepath.Join(n.relativePath(), name))
	}

	child := &Node{vfs: n.vfs, parent: n}
	inode := n.vfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFREG})
	if !n.AddChild(name, inode, false) {
		log.Warningf("[%s] Could not add child %q to %q, already exists", n.vfs.taskID(), name, n.relativePath())
		return nil, nil, 0, syscall.EIO
	}

	rf, err := openRemoteFile(n.vfs.getRPCContext(), n.vfs.vfsClient, filepath.Join(n.relativePath(), name), flags, mode, child)
	if err != nil {
		return nil, nil, 0, rpcErrToSyscallErrno(err)
	}

	out.Mode = mode

	return inode, rf, 0, 0
}

func (n *Node) CopyFileRange(ctx context.Context, fhIn fs.FileHandle, offIn uint64, out *fs.Inode, fhOut fs.FileHandle, offOut uint64, len uint64, flags uint64) (uint32, syscall.Errno) {
	n.resetCachedAttrs()

	rf, ok := fhIn.(*remoteFile)
	if !ok {
		log.Warningf("file handle is not a *remoteFile")
		return 0, syscall.EBADF
	}
	wf, ok := fhOut.(*remoteFile)
	if !ok {
		log.Warningf("file handle is not a *remoteFile")
	}

	if n.vfs.verbose {
		log.Debugf("[%s] CopyFileRange %q => %q", n.vfs.taskID(), rf.path, wf.path)
	}

	rsp, err := n.vfs.vfsClient.CopyFileRange(n.vfs.getRPCContext(), &vfspb.CopyFileRangeRequest{
		ReadHandleId:      rf.id,
		ReadHandleOffset:  int64(offIn),
		WriteHandleId:     wf.id,
		WriteHandleOffset: int64(offOut),
		NumBytes:          uint32(len),
		Flags:             uint32(flags),
	})
	if err != nil {
		return 0, rpcErrToSyscallErrno(err)
	}
	return rsp.GetNumBytesCopied(), fs.OK
}

func (n *Node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	newParentNode, ok := newParent.EmbeddedInode().Operations().(*Node)
	if !ok {
		log.Warningf("[%s] Parent is not a *Node", n.vfs.taskID())
		return syscall.EINVAL
	}
	if n.vfs.verbose {
		log.Debugf("[%s] Rename %q => %q", n.vfs.taskID(), filepath.Join(n.relativePath(), name), filepath.Join(newParentNode.relativePath(), newName))
	}

	existingSrcINode := n.GetChild(name)
	if existingSrcINode == nil {
		log.Warningf("[%s] Child %q not found in %q", n.vfs.taskID(), name, n.relativePath())
		return syscall.EINVAL
	}
	existingSrcNode, ok := existingSrcINode.Operations().(*Node)
	if !ok {
		log.Warningf("[%s] Source is not a *Node", n.vfs.taskID())
		return syscall.EINVAL
	}
	if existingSrcNode.immutable {
		log.Warningf("[%s] Denied attempt to rename immutable file %q", n.vfs.taskID(), filepath.Join(n.relativePath(), name))
		return syscall.EPERM
	}

	// TODO(vadim): don't allow directory rename to affect input files

	existingTargetNode := newParent.EmbeddedInode().GetChild(newName)
	if existingTargetNode != nil {
		existingNode, ok := existingTargetNode.Operations().(*Node)
		if !ok {
			log.Warningf("[%s] Existing target is not a *Node", n.vfs.taskID())
			return syscall.EINVAL
		}
		// Don't allow a rename to overwrite an input file.
		if existingNode.immutable {
			log.Warningf("[%s] Denied attempt to rename over immutable file %q", n.vfs.taskID(), existingNode.relativePath())
			return syscall.EPERM
		}
	}

	_, err := n.vfs.vfsClient.Rename(n.vfs.getRPCContext(), &vfspb.RenameRequest{
		OldPath: filepath.Join(n.relativePath(), name),
		NewPath: filepath.Join(newParent.EmbeddedInode().Path(nil), newName),
	})
	if err != nil {
		return rpcErrToSyscallErrno(err)
	}

	return fs.OK
}

func (n *Node) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if n.vfs.verbose {
		log.Debugf("[%s] Getattr %q", n.vfs.taskID(), n.relativePath())
	}

	n.mu.Lock()
	attrs := n.cachedAttrs
	n.mu.Unlock()

	if attrs == nil {
		rsp, err := n.vfs.vfsClient.GetAttr(n.vfs.getRPCContext(), &vfspb.GetAttrRequest{Path: n.relativePath()})
		if err != nil {
			return rpcErrToSyscallErrno(err)
		}
		attrs = rsp.GetAttrs()
		n.mu.Lock()
		n.cachedAttrs = rsp.GetAttrs()
		n.mu.Unlock()
	}
	out.Size = uint64(attrs.GetSize())
	out.Mode = attrs.GetPerm()
	out.Nlink = attrs.GetNlink()
	return fs.OK
}

func (n *Node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if n.vfs.verbose {
		log.Debugf("[%s] Setattr %q", n.vfs.taskID(), n.relativePath())
	}

	// Do not allow modifying attributes of input files.
	if n.immutable {
		log.Warningf("[%s] Denied attempt to change immutable file attributes %q", n.vfs.taskID(), n.relativePath())
		return syscall.EPERM
	}

	req := &vfspb.SetAttrRequest{
		Path: n.relativePath(),
	}
	if m, ok := in.GetMode(); ok {
		req.SetPerms = &vfspb.SetAttrRequest_SetPerms{Perms: m}
	}
	if s, ok := in.GetSize(); ok {
		req.SetSize = &vfspb.SetAttrRequest_SetSize{Size: int64(s)}
	}

	rsp, err := n.vfs.vfsClient.SetAttr(n.vfs.getRPCContext(), req)
	if err != nil {
		return rpcErrToSyscallErrno(err)
	}

	n.mu.Lock()
	n.cachedAttrs = rsp.GetAttrs()
	n.mu.Unlock()

	out.Size = uint64(rsp.GetAttrs().GetSize())
	out.Mode = rsp.GetAttrs().GetPerm()

	return fs.OK
}

func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n.vfs.verbose {
		log.Debugf("Mkdir %q", filepath.Join(n.relativePath(), name))
	}

	path := filepath.Join(n.relativePath(), name)
	_, err := n.vfs.vfsClient.Mkdir(n.vfs.getRPCContext(), &vfspb.MkdirRequest{Path: path, Perms: mode})
	if err != nil {
		return nil, rpcErrToSyscallErrno(err)
	}

	child := &Node{vfs: n.vfs, parent: n}
	inode := n.vfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFDIR})
	if !n.AddChild(name, inode, false) {
		log.Warningf("[%s] Mkdir could not add child %q to %q, already exists", n.vfs.taskID(), name, n.relativePath())
		return nil, syscall.EIO
	}
	return inode, 0
}

func (n *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	if n.vfs.verbose {
		log.Debugf("[%s] Rmdir %q", n.vfs.taskID(), filepath.Join(n.relativePath(), name))
	}
	_, err := n.vfs.vfsClient.Rmdir(n.vfs.getRPCContext(), &vfspb.RmdirRequest{Path: filepath.Join(n.relativePath(), name)})
	if err != nil {
		return rpcErrToSyscallErrno(err)
	}
	return fs.OK
}

func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	if n.vfs.verbose {
		log.Debugf("[%s] Readdir %q", n.vfs.taskID(), n.relativePath())
	}
	// The default implementation in the fuse library has a bug that can return entries in a different order across
	// multiple readdir calls. This can cause filesystem users to get incorrect directory listings.
	// Sorting this list on every Readdir call is potentially inefficient, but it doesn't seem to be a problem in
	// practice.
	var names []string
	for k := range n.Children() {
		names = append(names, k)
	}
	sort.Strings(names)
	var r []fuse.DirEntry
	for _, k := range names {
		ch := n.Children()[k]
		entry := fuse.DirEntry{
			Mode: ch.Mode(),
			Name: k,
			Ino:  ch.StableAttr().Ino,
		}
		r = append(r, entry)
	}
	return fs.NewListDirStream(r), 0
}

func (n *Node) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (node *fs.Inode, errno syscall.Errno) {
	targetNode, ok := target.EmbeddedInode().Operations().(*Node)
	if !ok {
		log.Warningf("[%s] Existing node is not a *Node", n.vfs.taskID())
		return nil, syscall.EINVAL
	}
	if n.vfs.verbose {
		log.Debugf("[%s] Link %q -> %q", n.vfs.taskID(), targetNode.relativePath(), name)
	}

	reqTarget := targetNode.relativePath()
	reqTarget = strings.TrimPrefix(reqTarget, n.vfs.mountDir)

	req := &vfspb.LinkRequest{
		Path:   filepath.Join(n.relativePath(), name),
		Target: reqTarget,
	}
	res, err := n.vfs.vfsClient.Link(n.vfs.getRPCContext(), req)
	if err != nil {
		return nil, rpcErrToSyscallErrno(err)
	}

	child := &Node{vfs: n.vfs, parent: n}
	inode := n.vfs.root.NewPersistentInode(ctx, child, fs.StableAttr{
		Mode: fuse.S_IFREG,
		Ino:  target.EmbeddedInode().StableAttr().Ino,
	})
	if !n.AddChild(name, inode, false) {
		log.Warningf("[%s] Link could not add child %q to %q, already exists", n.vfs.taskID(), name, n.relativePath())
		return nil, syscall.EIO
	}

	out.Attr.FromStat(attrsToStat(res.GetAttrs()))
	return inode, 0
}

func (n *Node) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (node *fs.Inode, errno syscall.Errno) {
	src := filepath.Join(n.relativePath(), name)
	if n.vfs.verbose {
		log.Debugf("[%s] Symlink %q -> %q", n.vfs.taskID(), src, target)
	}

	path := filepath.Join(n.relativePath(), name)

	reqTarget := target
	if strings.HasPrefix(target, n.vfs.mountDir) {
		reqTarget = strings.TrimPrefix(target, n.vfs.mountDir)
	}

	_, err := n.vfs.vfsClient.Symlink(n.vfs.getRPCContext(), &vfspb.SymlinkRequest{Path: path, Target: reqTarget})
	if err != nil {
		return nil, rpcErrToSyscallErrno(err)
	}

	child := &Node{vfs: n.vfs, parent: n, symlinkTarget: target}
	inode := n.vfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFLNK})
	if !n.AddChild(name, inode, false) {
		log.Warningf("[%s] Symlink could not add child %q to %q, already exists", n.vfs.taskID(), name, n.relativePath())
		return nil, syscall.EIO
	}

	return inode, 0
}

func (n *Node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	if n.vfs.verbose {
		log.Debugf("[%s] Readlink %q", n.vfs.taskID(), n.relativePath())
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.symlinkTarget != "" {
		return []byte(n.symlinkTarget), 0
	}
	return nil, syscall.EINVAL
}

func (n *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	relPath := filepath.Join(n.relativePath(), name)
	if n.vfs.verbose {
		log.Debugf("[%s] Unlink %q", n.vfs.taskID(), relPath)
	}
	existingTargetNode := n.GetChild(name)
	if existingTargetNode == nil {
		log.Warningf("[%s] Child %q does not exist in %q", n.vfs.taskID(), name, n.relativePath())
		return syscall.EINVAL
	}

	existingNode, ok := existingTargetNode.Operations().(*Node)
	if !ok {
		log.Warningf("[%s] Existing node is not a *Node", n.vfs.taskID())
		return syscall.EINVAL
	}

	if existingNode.immutable {
		log.Warningf("[%s] Denied attempt to unlink immutable file %q", n.vfs.taskID(), relPath)
		return syscall.EPERM
	}

	_, err := n.vfs.vfsClient.Unlink(n.vfs.getRPCContext(), &vfspb.UnlinkRequest{Path: relPath})
	if err != nil {
		return rpcErrToSyscallErrno(err)
	}

	n.mu.Lock()
	if existingNode.symlinkTarget != "" {
		existingNode.symlinkTarget = ""
	}
	n.mu.Unlock()

	return fs.OK
}

func (n *Node) Getlk(ctx context.Context, f fs.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) (errno syscall.Errno) {
	rf, ok := f.(*remoteFile)
	if !ok {
		log.Warningf("file handle is not a *remoteFile")
		return syscall.EBADF
	}
	req := &vfspb.GetLkRequest{
		HandleId: rf.id,
		Owner:    owner,
		FileLock: fileLockToProto(lk),
		Flags:    flags,
	}
	res, err := n.vfs.vfsClient.GetLk(n.vfs.getRPCContext(), req)
	if err != nil {
		return rpcErrToSyscallErrno(err)
	}
	fl := res.GetFileLock()
	out.Start = fl.GetStart()
	out.End = fl.GetEnd()
	out.Pid = fl.GetPid()
	out.Typ = fl.GetTyp()
	return 0
}

func (n *Node) Setlk(ctx context.Context, f fs.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) (errno syscall.Errno) {
	rf, ok := f.(*remoteFile)
	if !ok {
		log.Warningf("file handle is not a *remoteFile")
		return syscall.EBADF
	}
	req := &vfspb.SetLkRequest{
		HandleId: rf.id,
		Owner:    owner,
		FileLock: fileLockToProto(lk),
		Flags:    flags,
	}
	_, err := n.vfs.vfsClient.SetLk(n.vfs.getRPCContext(), req)
	if err != nil {
		return rpcErrToSyscallErrno(err)
	}
	return 0
}

func (n *Node) Setlkw(ctx context.Context, f fs.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) (errno syscall.Errno) {
	rf, ok := f.(*remoteFile)
	if !ok {
		log.Warningf("file handle is not a *remoteFile")
		return syscall.EBADF
	}
	req := &vfspb.SetLkRequest{
		HandleId: rf.id,
		Owner:    owner,
		FileLock: fileLockToProto(lk),
		Flags:    flags,
	}
	_, err := n.vfs.vfsClient.SetLkw(n.vfs.getRPCContext(), req)
	if err != nil {
		return rpcErrToSyscallErrno(err)
	}
	return 0
}

func fileLockToProto(lk *fuse.FileLock) *vfspb.FileLock {
	return &vfspb.FileLock{
		Start: lk.Start,
		End:   lk.End,
		Typ:   lk.Typ,
		Pid:   lk.Pid,
	}
}
