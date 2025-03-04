//go:build ((linux && !android) || (darwin && !ios)) && (amd64 || arm64)

package vfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
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

type perOpStats map[string]int

type VFS struct {
	vfsClient           vfspb.FileSystemClient
	mountDir            string
	verbose             bool
	logFUSEOps          bool
	logFUSELatencyStats bool
	logFUSEPerFileStats bool

	server *fuse.Server // Not set until FS is mounted.

	root *Node

	mu             sync.Mutex
	internalTaskID string
	rpcCtx         context.Context
	opStats        map[string]*opStats
	perFileStats   map[string]perOpStats
}

type Options struct {
	// Verbose enables logging of most per-file operations.
	Verbose bool
	// LogFUSEOps enabled logging of all operations received by the go fuse
	// server from the kernel.
	LogFUSEOps bool
	// LogFUSELatencyStats enables logging of per-operation latency stats when
	// the filesystem is unmounted. Implicitly enabled when Verbose is true.
	LogFUSELatencyStats bool
	// LogFUSEPerFileStats enables tracking of operation counts on a per-file
	// basis. The stats are logged when the filesystem is unmounted.
	LogFUSEPerFileStats bool
}

func New(vfsClient vfspb.FileSystemClient, mountDir string, options *Options) *VFS {
	// This is so we always have a context set. The real context will be set in the PrepareForTask call.
	rpcCtx, cancel := context.WithCancel(context.Background())
	cancel()

	vfs := &VFS{
		vfsClient:           vfsClient,
		mountDir:            mountDir,
		verbose:             options.Verbose,
		logFUSEOps:          options.LogFUSEOps,
		logFUSELatencyStats: options.LogFUSELatencyStats,
		logFUSEPerFileStats: options.LogFUSEPerFileStats,
		rpcCtx:              rpcCtx,
		opStats:             make(map[string]*opStats),
		perFileStats:        make(map[string]perOpStats),
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
			// Don't depend on `fusermount`.
			// Disable fallback to fusermount as well, since it can cause
			// deadlocks. See https://github.com/hanwen/go-fuse/issues/506
			DirectMountStrict: true,
			FsName:            "bbvfs",
			MaxWrite:          fuse.MAX_KERNEL_WRITE,
			EnableLocks:       true,
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
	return nil
}

func (vfs *VFS) taskID() string {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()
	return vfs.internalTaskID
}

func (vfs *VFS) logStats() {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	if vfs.verbose || vfs.logFUSELatencyStats {
		log.CtxDebugf(vfs.rpcCtx, "OP stats:")

		var opNames []string
		for op := range vfs.opStats {
			opNames = append(opNames, op)
		}
		slices.Sort(opNames)

		var totalTime time.Duration
		for _, op := range opNames {
			s := vfs.opStats[op]
			log.CtxDebugf(vfs.rpcCtx, "%-20s num_calls=%-08d time=%.2fs", op, s.count, s.timeSpent.Seconds())
			totalTime += s.timeSpent
		}
		log.CtxDebugf(vfs.rpcCtx, "Total time spent in OPs: %s", totalTime)
	}

	if vfs.logFUSEPerFileStats {
		var paths []string
		for p := range vfs.perFileStats {
			paths = append(paths, p)
		}
		slices.Sort(paths)

		log.CtxDebugf(vfs.rpcCtx, "%d paths accessed:", len(vfs.perFileStats))
		for _, p := range paths {
			stats := vfs.perFileStats[p]
			var opNames []string
			for op := range stats {
				opNames = append(opNames, op)
			}
			slices.Sort(opNames)
			s := ""
			for _, k := range opNames {
				s += fmt.Sprintf("%s:%d ", k, stats[k])
			}
			log.CtxDebugf(vfs.rpcCtx, "%s %s", p, s)
		}
	}
}

func (vfs *VFS) FinishTask() error {
	// TODO(vadim): propagate stats to ActionResult
	vfs.logStats()

	vfs.mu.Lock()
	defer vfs.mu.Unlock()
	vfs.internalTaskID = "unset"

	return nil
}

func (vfs *VFS) Unmount() error {
	if vfs.server == nil {
		return nil
	}

	vfs.logStats()

	return vfs.server.Unmount()
}

func (vfs *VFS) startOP(path string, op string) {
	if !vfs.logFUSEPerFileStats {
		return
	}

	vfs.mu.Lock()
	defer vfs.mu.Unlock()
	stats, ok := vfs.perFileStats[path]
	if !ok {
		stats = make(perOpStats)
		vfs.perFileStats[path] = stats
	}
	stats[op]++
}

type Node struct {
	fs.Inode

	vfs       *VFS
	immutable bool
	id        uint64

	mu            sync.Mutex
	cachedAttrs   *vfspb.Attrs
	symlinkTarget string
	numHandles    int
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

func (n *Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	req := &vfspb.LookupRequest{
		ParentId: n.StableAttr().Ino,
		Name:     name,
	}
	rsp, err := n.vfs.vfsClient.Lookup(n.vfs.getRPCContext(), req)
	if err != nil {
		return nil, rpcErrToSyscallErrno(err)
	}

	child := &Node{
		vfs:           n.vfs,
		cachedAttrs:   rsp.Attrs,
		symlinkTarget: rsp.SymlinkTarget,
	}
	fillFuseAttr(&out.Attr, rsp.GetAttrs())
	return n.NewInode(ctx, child, fs.StableAttr{Mode: rsp.Mode, Ino: rsp.Id}), 0
}

type dirHandle struct {
	vfs      *VFS
	node     *Node
	children []*vfspb.Node
	pos      int
}

func (d *dirHandle) Readdirent(ctx context.Context) (*fuse.DirEntry, syscall.Errno) {
	d.node.startOP("Readdirent")
	if d.vfs.verbose {
		log.CtxDebugf(d.vfs.rpcCtx, "Readdirent %q", d.node.relativePath())
	}
	if d.pos == -1 {
		rsp, err := d.vfs.vfsClient.GetDirectoryContents(d.vfs.rpcCtx, &vfspb.GetDirectoryContentsRequest{
			Id: d.node.StableAttr().Ino,
		})
		if err != nil {
			return nil, rpcErrToSyscallErrno(err)
		}
		d.children = rsp.Nodes
		d.pos = 0
	}

	if d.pos >= len(d.children) {
		return nil, 0
	}
	node := d.children[d.pos]
	d.pos++
	return &fuse.DirEntry{
		Mode: node.Mode,
		Name: node.Name,
		Ino:  node.Id,
	}, 0
}

func (d *dirHandle) Fsyncdir(ctx context.Context, flags uint32) syscall.Errno {
	return 0
}

func (n *Node) OpendirHandle(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	n.startOP("OpendirHandle")
	if n.vfs.verbose {
		log.CtxDebugf(n.vfs.rpcCtx, "OpendirHandle %q", n.relativePath())
	}
	return &dirHandle{vfs: n.vfs, node: n, pos: -1}, 0, 0
}

func (f *remoteFile) startOP(op string) {
	f.node.vfs.startOP(f.path, op)
}

func (f *remoteFile) PassthroughFd() (int, bool) {
	f.startOP("PassthroughFd")
	if f.node.vfs.verbose {
		log.CtxDebugf(f.node.vfs.rpcCtx, "PassthroughFd %q", f.path)
	}
	return int(f.id), true
}

func (f *remoteFile) Allocate(ctx context.Context, off uint64, size uint64, mode uint32) syscall.Errno {
	f.startOP("Allocate")
	if f.node.vfs.verbose {
		log.CtxDebugf(f.node.vfs.rpcCtx, "Allocate %q", f.path)
	}
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
	f.startOP("Flush")
	if f.node.vfs.verbose {
		f.mu.Lock()
		log.CtxDebugf(f.node.vfs.rpcCtx, "Flush %q (ino %d), read %s (%d RPCs), wrote %s (%d RPCs)", f.path, f.node.StableAttr().Ino, units.HumanSize(float64(f.readBytes)), f.readRPCs, units.HumanSize(float64(f.wroteBytes)), f.writeRPCs)
		f.mu.Unlock()
	}

	if _, err := f.vfsClient.Flush(f.ctx, &vfspb.FlushRequest{HandleId: f.id}); err != nil {
		return rpcErrToSyscallErrno(err)
	}
	f.node.mu.Lock()
	f.node.cachedAttrs = nil
	f.node.mu.Unlock()
	return fs.OK
}

func (f *remoteFile) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	f.startOP("Fsync")
	if f.node.vfs.verbose {
		log.CtxDebugf(f.node.vfs.rpcCtx, "Fsync %q", f.path)
	}
	if _, err := f.vfsClient.Fsync(f.ctx, &vfspb.FsyncRequest{HandleId: f.id}); err != nil {
		return rpcErrToSyscallErrno(err)
	}
	return fs.OK
}

func (f *remoteFile) Fsyncdir(ctx context.Context, flags uint32) syscall.Errno {
	return fs.OK
}

func (f *remoteFile) Release(ctx context.Context) syscall.Errno {
	f.startOP("Release")
	if f.node.vfs.verbose {
		f.mu.Lock()
		log.CtxDebugf(f.node.vfs.rpcCtx, "Release %q (ino %d) handle ID %d, read %s (%d RPCs), wrote %s (%d RPCs)", f.path, f.node.StableAttr().Ino, f.id, units.HumanSize(float64(f.readBytes)), f.readRPCs, units.HumanSize(float64(f.wroteBytes)), f.writeRPCs)
		f.mu.Unlock()
	}
	if _, err := f.vfsClient.Release(f.ctx, &vfspb.ReleaseRequest{HandleId: f.id}); err != nil {
		return rpcErrToSyscallErrno(err)
	}
	f.node.mu.Lock()
	f.node.numHandles--
	f.node.mu.Unlock()
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
	f.startOP("Read")
	res = &remoteFileReader{f: f, offset: off, numBytes: len(buf)}
	return
}

func (f *remoteFile) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	f.startOP("Write")
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

func (n *Node) startOP(op string) {
	n.vfs.startOP(n.relativePath(), op)
}

func describeOpenFlags(flags uint32) string {
	var textFlags []string
	if int(flags)&os.O_WRONLY != 0 {
		textFlags = append(textFlags, "O_WRONLY")
	}
	if int(flags)&os.O_RDONLY != 0 {
		textFlags = append(textFlags, "O_RDONLY")
	}
	if int(flags)&os.O_RDWR != 0 {
		textFlags = append(textFlags, "O_RDWR")
	}
	if int(flags)&os.O_APPEND != 0 {
		textFlags = append(textFlags, "O_APPEND")
	}
	if int(flags)&os.O_CREATE != 0 {
		textFlags = append(textFlags, "O_CREATE")
	}
	if int(flags)&os.O_TRUNC != 0 {
		textFlags = append(textFlags, "O_TRUNC")
	}
	if int(flags)&os.O_EXCL != 0 {
		textFlags = append(textFlags, "O_EXCL")
	}
	if int(flags)&os.O_SYNC != 0 {
		textFlags = append(textFlags, "O_SYNC")
	}
	return strings.Join(textFlags, ",")
}

func (n *Node) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	n.startOP("Open")

	// Don't allow writes to input files.
	if n.immutable && (int(flags)&(os.O_WRONLY|os.O_RDWR)) != 0 {
		log.Warningf("[%s] Denied attempt to write to immutable file %q", n.vfs.taskID(), n.relativePath())
		return nil, 0, syscall.EPERM
	}

	if n.vfs.verbose {
		log.CtxDebugf(n.vfs.rpcCtx, "Open %q (ino %d) with flags %q", n.relativePath(), n.StableAttr().Ino, describeOpenFlags(flags))
	}

	req := &vfspb.OpenRequest{
		Id:    n.StableAttr().Ino,
		Flags: flags,
	}
	rsp, err := n.vfs.vfsClient.Open(ctx, req)
	if err != nil {
		return nil, 0, rpcErrToSyscallErrno(err)
	}
	n.mu.Lock()
	n.cachedAttrs = nil
	n.numHandles++
	n.mu.Unlock()
	return &remoteFile{
		ctx:       ctx,
		vfsClient: n.vfs.vfsClient,
		path:      n.relativePath(),
		id:        rsp.GetHandleId(),
		node:      n,
		data:      rsp.GetData(),
	}, 0, 0

}

func (n *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	n.startOP("Create")

	if n.vfs.verbose {
		log.CtxDebugf(n.vfs.rpcCtx, "Create %q", filepath.Join(n.relativePath(), name))
	}

	child := &Node{vfs: n.vfs, numHandles: 1}

	req := &vfspb.CreateRequest{
		ParentId: n.StableAttr().Ino,
		Name:     name,
		Flags:    flags,
		Mode:     mode,
	}
	rsp, err := n.vfs.vfsClient.Create(ctx, req)
	if err != nil {
		return nil, nil, 0, rpcErrToSyscallErrno(err)
	}

	inode := n.vfs.root.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFREG, Ino: rsp.GetId()})

	rf := &remoteFile{
		ctx:       ctx,
		vfsClient: n.vfs.vfsClient,
		path:      filepath.Join(n.relativePath(), name),
		id:        rsp.GetHandleId(),
		node:      child,
	}

	out.Mode = mode
	out.Nlink = 1

	return inode, rf, 0, 0
}

func (n *Node) CopyFileRange(ctx context.Context, fhIn fs.FileHandle, offIn uint64, out *fs.Inode, fhOut fs.FileHandle, offOut uint64, len uint64, flags uint64) (uint32, syscall.Errno) {
	n.startOP("CopyFileRange")
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
		log.CtxDebugf(n.vfs.rpcCtx, "CopyFileRange %q => %q", rf.path, wf.path)
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
	n.startOP("Rename")
	newParentNode, ok := newParent.EmbeddedInode().Operations().(*Node)
	if !ok {
		log.Warningf("[%s] Parent is not a *Node", n.vfs.taskID())
		return syscall.EINVAL
	}
	if n.vfs.verbose {
		log.CtxDebugf(n.vfs.rpcCtx, "Rename %q => %q", filepath.Join(n.relativePath(), name), filepath.Join(newParentNode.relativePath(), newName))
	}

	_, err := n.vfs.vfsClient.Rename(n.vfs.getRPCContext(), &vfspb.RenameRequest{
		OldParentId: n.StableAttr().Ino,
		OldName:     name,
		NewParentId: newParent.EmbeddedInode().StableAttr().Ino,
		NewName:     newName,
	})
	if err != nil {
		return rpcErrToSyscallErrno(err)
	}

	return fs.OK
}

func fillFuseAttr(out *fuse.Attr, attr *vfspb.Attrs) {
	out.Size = uint64(attr.GetSize())
	out.Mode = attr.GetPerm()
	out.Nlink = attr.GetNlink()

	out.Mtime = attr.MtimeNanos / 1e9
	out.Mtimensec = uint32(attr.MtimeNanos % 1e9)
}

func (n *Node) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	n.startOP("Getattr")
	if n.vfs.verbose {
		log.CtxDebugf(n.vfs.rpcCtx, "Getattr %q", n.relativePath())
	}

	n.mu.Lock()
	attrs := n.cachedAttrs
	n.mu.Unlock()

	if attrs == nil {
		rsp, err := n.vfs.vfsClient.GetAttr(n.vfs.getRPCContext(), &vfspb.GetAttrRequest{Id: n.StableAttr().Ino})
		if err != nil {
			return rpcErrToSyscallErrno(err)
		}
		attrs = rsp.GetAttrs()
		n.mu.Lock()
		if n.numHandles == 0 {
			// Don't cache attributes while a file is opened.
			n.cachedAttrs = rsp.GetAttrs()
		}
		n.mu.Unlock()
	}
	fillFuseAttr(&out.Attr, attrs)
	return fs.OK
}

func (n *Node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	n.startOP("Setattr")
	if n.vfs.verbose {
		log.CtxDebugf(n.vfs.rpcCtx, "Setattr %q", n.relativePath())
	}

	// Do not allow modifying attributes of input files.
	if n.immutable {
		log.Warningf("[%s] Denied attempt to change immutable file attributes %q", n.vfs.taskID(), n.relativePath())
		return syscall.EPERM
	}

	req := &vfspb.SetAttrRequest{
		Id: n.StableAttr().Ino,
	}
	if m, ok := in.GetMode(); ok {
		req.SetPerms = &vfspb.SetAttrRequest_SetPerms{Perms: m}
	}
	if s, ok := in.GetSize(); ok {
		req.SetSize = &vfspb.SetAttrRequest_SetSize{Size: int64(s)}
	}
	if mt, ok := in.GetMTime(); ok {
		req.SetMtime = &vfspb.SetAttrRequest_SetMTime{MtimeNanos: uint64(mt.UnixNano())}
	}

	rsp, err := n.vfs.vfsClient.SetAttr(n.vfs.getRPCContext(), req)
	if err != nil {
		return rpcErrToSyscallErrno(err)
	}

	n.mu.Lock()
	if n.numHandles == 0 {
		n.cachedAttrs = rsp.GetAttrs()
	}
	n.mu.Unlock()

	out.Size = uint64(rsp.GetAttrs().GetSize())
	out.Mode = rsp.GetAttrs().GetPerm()
	out.Mtime = rsp.GetAttrs().MtimeNanos / 1e9
	out.Mtimensec = uint32(rsp.GetAttrs().MtimeNanos % 1e9)

	return fs.OK
}

func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	n.startOP("Mkdir")
	if n.vfs.verbose {
		log.CtxDebugf(ctx, "Mkdir %q", filepath.Join(n.relativePath(), name))
	}

	rsp, err := n.vfs.vfsClient.Mkdir(n.vfs.getRPCContext(), &vfspb.MkdirRequest{ParentId: n.StableAttr().Ino, Name: name, Perms: mode})
	if err != nil {
		return nil, rpcErrToSyscallErrno(err)
	}

	child := &Node{vfs: n.vfs}
	inode := n.vfs.root.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFDIR, Ino: rsp.Id})
	return inode, 0
}

func (n *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	n.startOP("Rmdir")
	if n.vfs.verbose {
		log.CtxDebugf(n.vfs.rpcCtx, "Rmdir %q", filepath.Join(n.relativePath(), name))
	}
	_, err := n.vfs.vfsClient.Rmdir(n.vfs.getRPCContext(), &vfspb.RmdirRequest{ParentId: n.StableAttr().Ino, Name: name})
	if err != nil {
		return rpcErrToSyscallErrno(err)
	}
	return fs.OK
}

func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	n.startOP("Readdir")
	if n.vfs.verbose {
		log.CtxDebugf(n.vfs.rpcCtx, "Readdir %q", n.relativePath())
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
	n.startOP("Link")
	targetNode, ok := target.EmbeddedInode().Operations().(*Node)
	if !ok {
		log.Warningf("[%s] Existing node is not a *Node", n.vfs.taskID())
		return nil, syscall.EINVAL
	}
	if n.vfs.verbose {
		log.CtxDebugf(n.vfs.rpcCtx, "Link %q -> %q", targetNode.relativePath(), name)
	}

	req := &vfspb.LinkRequest{
		ParentId: n.StableAttr().Ino,
		Name:     name,
		TargetId: target.EmbeddedInode().StableAttr().Ino,
	}
	res, err := n.vfs.vfsClient.Link(n.vfs.getRPCContext(), req)
	if err != nil {
		return nil, rpcErrToSyscallErrno(err)
	}

	child := &Node{vfs: n.vfs}
	inode := n.vfs.root.NewInode(ctx, child, fs.StableAttr{
		Mode: fuse.S_IFREG,
		Ino:  target.EmbeddedInode().StableAttr().Ino,
	})
	out.Attr.FromStat(attrsToStat(res.GetAttrs()))
	return inode, 0
}

func (n *Node) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (node *fs.Inode, errno syscall.Errno) {
	n.startOP("Symlink")
	if n.vfs.verbose {
		src := filepath.Join(n.relativePath(), name)
		log.CtxDebugf(n.vfs.rpcCtx, "Symlink %q -> %q", src, target)
	}

	reqTarget := target
	if strings.HasPrefix(target, n.vfs.mountDir) {
		reqTarget = strings.TrimPrefix(target, n.vfs.mountDir)
	}

	rsp, err := n.vfs.vfsClient.Symlink(n.vfs.getRPCContext(), &vfspb.SymlinkRequest{ParentId: n.StableAttr().Ino, Name: name, Target: reqTarget})
	if err != nil {
		return nil, rpcErrToSyscallErrno(err)
	}

	child := &Node{vfs: n.vfs, symlinkTarget: target}
	inode := n.vfs.root.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFLNK, Ino: rsp.GetId()})
	return inode, 0
}

func (n *Node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	n.startOP("Readlink")
	if n.vfs.verbose {
		log.CtxDebugf(n.vfs.rpcCtx, "Readlink %q", n.relativePath())
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.symlinkTarget != "" {
		if n.vfs.verbose {
			log.CtxDebugf(n.vfs.rpcCtx, "Readlink %q returning %q", n.relativePath(), n.symlinkTarget)
		}
		return []byte(n.symlinkTarget), 0
	}
	if n.vfs.verbose {
		log.CtxDebugf(n.vfs.rpcCtx, "Readlink %q returning invalid", n.relativePath())
	}
	return nil, syscall.EINVAL
}

func (n *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	n.startOP("Unlink")
	relPath := filepath.Join(n.relativePath(), name)
	if n.vfs.verbose {
		log.CtxDebugf(n.vfs.rpcCtx, "Unlink %q", relPath)
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

	_, err := n.vfs.vfsClient.Unlink(n.vfs.getRPCContext(), &vfspb.UnlinkRequest{ParentId: n.StableAttr().Ino, Name: name})
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
	n.startOP("Getlk")
	if n.vfs.verbose {
		log.CtxDebugf(n.vfs.rpcCtx, "Getlk %q", n.relativePath())
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
	n.startOP("Setlk")
	if n.vfs.verbose {
		log.CtxDebugf(n.vfs.rpcCtx, "Setlk %q", n.relativePath())
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
	n.startOP("Setlkw")
	if n.vfs.verbose {
		log.CtxDebugf(n.vfs.rpcCtx, "Setlkw %q", n.relativePath())
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
