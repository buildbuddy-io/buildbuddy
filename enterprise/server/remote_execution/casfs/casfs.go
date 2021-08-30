package casfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/dirtools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/docker/go-units"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type CASFS struct {
	scratchDir string
	verbose    bool
	logFUSEOps bool

	server *fuse.Server // Not set until FS is mounted.

	root *Node

	mu             sync.Mutex
	internalTaskID string
	fileFetcher    *dirtools.BatchFileFetcher
	bytesRead      int
	bytesWritten   int
}

type Options struct {
	// Verbose enables logging of most per-file operations.
	Verbose bool
	// LogFUSEOps enabled logging of all operations received by the go fuse server from the kernel.
	LogFUSEOps bool
}

func New(scratchDir string, options *Options) *CASFS {
	cfs := &CASFS{
		scratchDir: scratchDir,
		verbose:    options.Verbose,
		logFUSEOps: options.LogFUSEOps,
	}
	root := &Node{cfs: cfs}
	cfs.root = root
	return cfs
}

func (cfs *CASFS) Mount(dir string) error {
	server, err := fs.Mount(dir, cfs.root, &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther:    true,
			Debug:         cfs.logFUSEOps,
			DisableXAttrs: true,
		},
	})
	if err != nil {
		return status.UnknownErrorf("could not mount CAS FS at %q: %s", dir, err)
	}
	cfs.server = server
	return nil
}

// PrepareForTask prepares the virtual filesystem layout according to the provided `fsLayout`.
// The passed `fileFetcher` is expected to be configured with appropriate credentials necessary to be able to interact
// with the CAS for the given task.
// The passed `taskID` os only used for logging purposes.
func (cfs *CASFS) PrepareForTask(ctx context.Context, fileFetcher *dirtools.BatchFileFetcher, taskID string, fsLayout *container.FileSystemLayout) error {
	cfs.mu.Lock()
	defer cfs.mu.Unlock()

	rootDirectory := fsLayout.Inputs.GetRoot()
	rootDirectoryDigest, err := digest.ComputeForMessage(rootDirectory)
	if err != nil {
		return err
	}
	if rootDirectoryDigest.Hash == digest.EmptySha256 {
		return nil
	}

	dirMap, err := dirtools.BuildDirMap(fsLayout.Inputs)
	if err != nil {
		return err
	}

	cfs.internalTaskID = taskID
	var walkDir func(dir *repb.Directory, node *Node) error
	walkDir = func(dir *repb.Directory, parentNode *Node) error {
		for _, childDirNode := range dir.GetDirectories() {
			child := &Node{cfs: cfs, parent: parentNode}
			inode := cfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFDIR})
			if !parentNode.AddChild(childDirNode.Name, inode, false) {
				return status.UnknownErrorf("could not add child %q to %q, already exists", childDirNode.Name, parentNode.relativePath())
			}
			if cfs.verbose {
				log.Debugf("[%s] Input directory: %s", cfs.taskID(), child.relativePath())
			}
			childDir, ok := dirMap[digest.NewKey(childDirNode.Digest)]
			if !ok {
				return status.NotFoundErrorf("could not find dir %q", childDirNode.Digest)
			}
			if err := walkDir(childDir, child); err != nil {
				return err
			}
		}
		for _, childFileNode := range dir.GetFiles() {
			child := &Node{cfs: cfs, parent: parentNode, fileNode: childFileNode}
			inode := cfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFREG})
			if !parentNode.AddChild(childFileNode.Name, inode, false) {
				return status.UnknownErrorf("could not add child %q to %q, already exists", childFileNode.Name, parentNode.relativePath())
			}
			if cfs.verbose {
				log.Debugf("[%s] Input file: %s", cfs.taskID(), child.relativePath())
			}
		}
		for _, childSymlinkNode := range dir.GetSymlinks() {
			child := &Node{cfs: cfs, parent: parentNode, symlinkTarget: childSymlinkNode.GetTarget()}
			inode := cfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFLNK})
			if !parentNode.AddChild(childSymlinkNode.Name, inode, false) {
				return status.UnknownErrorf("could not add child %q to %q, already exists", childSymlinkNode.Name, parentNode.relativePath())
			}
			if cfs.verbose {
				log.Debugf("[%s] Input symlink: %s", cfs.taskID(), child.relativePath())
			}
		}
		return nil
	}

	err = walkDir(rootDirectory, cfs.root)
	if err != nil {
		return err
	}

	outputDirs := make(map[string]struct{})
	for _, dir := range fsLayout.OutputDirs {
		if cfs.verbose {
			log.Debugf("[%s] Output dir: %s", cfs.taskID(), dir)
		}
		outputDirs[dir] = struct{}{}
	}
	for _, file := range fsLayout.OutputFiles {
		if cfs.verbose {
			log.Debugf("[%s] Output file: %s", cfs.taskID(), file)
		}
		outputDirs[filepath.Dir(file)] = struct{}{}
	}
	for dir := range outputDirs {
		parts := strings.Split(dir, string(os.PathSeparator))
		node := cfs.root
		for _, p := range parts {
			childNode := node.GetChild(p)
			if childNode == nil {
				child := &Node{cfs: cfs, parent: node}
				inode := cfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFDIR})
				if !node.AddChild(p, inode, false) {
					return status.UnknownErrorf("could not add child %q, already exists", p)
				}
				node = child
			} else {
				node = childNode.Operations().(*Node)
			}
		}
	}

	cfs.fileFetcher = fileFetcher

	return nil
}

func (cfs *CASFS) fetchFiles(filesToFetch dirtools.FileMap, opts *dirtools.DownloadTreeOpts) error {
	cfs.mu.Lock()
	ff := cfs.fileFetcher
	taskID := cfs.internalTaskID
	cfs.mu.Unlock()
	if ff == nil {
		return status.FailedPreconditionErrorf("[%s] file fetcher is not set", taskID)
	}
	return ff.FetchFiles(filesToFetch, opts)
}

func (cfs *CASFS) taskID() string {
	cfs.mu.Lock()
	defer cfs.mu.Unlock()
	return cfs.internalTaskID
}

func (cfs *CASFS) CountReadBytes(n int) {
	cfs.mu.Lock()
	defer cfs.mu.Unlock()
	cfs.bytesRead += n
}

func (cfs *CASFS) CountWrittenBytes(n int) {
	cfs.mu.Lock()
	defer cfs.mu.Unlock()
	cfs.bytesWritten += n
}

func (cfs *CASFS) FinishTask() {
	cfs.mu.Lock()
	defer cfs.mu.Unlock()
	cfs.fileFetcher = nil
	cfs.internalTaskID = "unset"
}

func (cfs *CASFS) Unmount() error {
	if cfs.server == nil {
		return nil
	}

	cfs.mu.Lock()
	readBytes := cfs.bytesRead
	wroteBytes := cfs.bytesWritten
	cfs.mu.Unlock()

	log.Infof("[%s] Unmounting. Total read %s, wrote %s.", cfs.taskID(), units.HumanSize(float64(readBytes)), units.HumanSize(float64(wroteBytes)))

	return cfs.server.Unmount()
}

type Node struct {
	fs.Inode

	cfs    *CASFS
	parent *Node

	// Whether this node represents an input to the action.
	// Modifications to inputs are denied.
	isInput bool

	fileNode      *repb.FileNode
	symlinkTarget string

	mu              sync.Mutex
	scratchFilePath string
}

func (n *Node) relativePath() string {
	return n.Path(nil)
}

func (n *Node) computeScratchPath() string {
	return filepath.Join(n.cfs.scratchDir, n.relativePath())
}

func (n *Node) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	// Don't allow writes to input files.
	if n.isInput && (int(flags)&(os.O_WRONLY|os.O_RDWR)) != 0 {
		log.Warningf("[%s] Denied attempt to write to input file %q", n.cfs.taskID(), n.relativePath())
		return nil, 0, syscall.EPERM
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	relPath := n.relativePath()
	scratchPath := n.computeScratchPath()
	if n.scratchFilePath != "" {
		hash := ""
		if n.fileNode != nil {
			hash = fmt.Sprintf(" (%s)", n.fileNode.GetDigest().GetHash())
		}
		if n.cfs.verbose {
			log.Debugf("[%s] Open %q%s: serve from scratch file %q", n.cfs.taskID(), relPath, hash, scratchPath)
		}
		fd, err := syscall.Open(scratchPath, int(flags), 0)
		if err != nil {
			log.Warningf("[%s] Open %q%s failed: %s", n.cfs.taskID(), scratchPath, hash, err)
			return nil, 0, syscall.EIO
		}
		lf := &instrumentedLoopbackFile{FileHandle: fs.NewLoopbackFile(fd), cfs: n.cfs, fd: fd, fullPath: scratchPath}
		return lf, 0, 0
	}

	if n.fileNode != nil {
		if n.cfs.verbose {
			log.Debugf("[%s] Open %q: fetch %s from CAS", n.cfs.taskID(), relPath, n.fileNode.GetDigest().GetHash())
		}

		fileMap := dirtools.FileMap{
			digest.NewKey(n.fileNode.GetDigest()): {&dirtools.FilePointer{
				FullPath:     scratchPath,
				RelativePath: relPath,
				FileNode:     n.fileNode,
			}},
		}

		if err := os.MkdirAll(filepath.Dir(scratchPath), 0755); err != nil {
			log.Warningf("[%s] Could not make dirs for %q: %s", n.cfs.taskID(), scratchPath, err)
			return nil, 0, syscall.EINVAL
		}

		err := n.cfs.fetchFiles(fileMap, &dirtools.DownloadTreeOpts{})
		if err != nil {
			log.Warningf("[%s] Open fetch %q failed: %s", n.cfs.taskID(), n.fileNode.GetDigest().GetHash(), err)
			return nil, 0, syscall.EIO
		}

		n.scratchFilePath = scratchPath

		fd, err := syscall.Open(scratchPath, int(flags), 0)
		if err != nil {
			log.Warningf("[%s] Open %q failed: %s", n.cfs.taskID(), scratchPath, err)
			return nil, 0, syscall.EIO
		}
		lf := &instrumentedLoopbackFile{FileHandle: fs.NewLoopbackFile(fd), cfs: n.cfs, fd: fd, fullPath: scratchPath}
		return lf, 0, 0
	}

	return nil, 0, syscall.ENOTSUP
}

func (n *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if n.cfs.verbose {
		log.Debugf("[%s] Create %q", n.cfs.taskID(), filepath.Join(n.relativePath(), name))
	}

	scratchPath := filepath.Join(n.computeScratchPath(), name)
	if err := os.MkdirAll(filepath.Dir(scratchPath), 0755); err != nil {
		log.Infof("[%s] Could not make dirs for %q: %s", n.cfs.taskID(), scratchPath, err)
		return nil, 0, 0, syscall.EINVAL
	}

	fd, err := syscall.Open(scratchPath, int(flags), mode)
	if err != nil {
		log.Warningf("[%s] Could not open %q: %s", n.cfs.taskID(), scratchPath, err)
		return nil, nil, 0, fs.ToErrno(err)
	}
	lf := &instrumentedLoopbackFile{FileHandle: fs.NewLoopbackFile(fd), cfs: n.cfs, fd: fd, fullPath: scratchPath}
	child := &Node{cfs: n.cfs, parent: n, scratchFilePath: scratchPath}
	inode := n.cfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFREG})
	if !n.AddChild(name, inode, false) {
		log.Warningf("[%s] Could not add child %q to %q, already exists", n.cfs.taskID(), name, n.relativePath())
		return nil, nil, 0, syscall.EIO
	}

	st := syscall.Stat_t{}
	if err := syscall.Fstat(fd, &st); err != nil {
		closeErr := syscall.Close(fd)
		if closeErr != nil {
			log.Warningf("[%s] Could not close file descriptor for %q: %s", n.cfs.taskID(), scratchPath, closeErr)
		}
		return nil, nil, 0, fs.ToErrno(err)
	}
	out.FromStat(&st)

	return inode, lf, 0, 0
}

func (n *Node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	newParentNode, ok := newParent.EmbeddedInode().Operations().(*Node)
	if !ok {
		log.Warningf("[%s] Parent is not a *Node", n.cfs.taskID())
		return syscall.EINVAL
	}
	if n.cfs.verbose {
		log.Debugf("[%s] Rename %q => %q", n.cfs.taskID(), filepath.Join(n.relativePath(), name), filepath.Join(newParentNode.relativePath(), newName))
	}

	existingSrcINode := n.GetChild(name)
	if existingSrcINode == nil {
		log.Warningf("[%s] Child %q not found in %q", n.cfs.taskID(), name, n.relativePath())
		return syscall.EINVAL
	}
	existingSrcNode, ok := existingSrcINode.Operations().(*Node)
	if !ok {
		log.Warningf("[%s] Source is not a *Node", n.cfs.taskID())
		return syscall.EINVAL
	}
	if existingSrcNode.isInput {
		log.Warningf("[%s] Denied attempt to rename input file %q", n.cfs.taskID(), filepath.Join(n.relativePath(), name))
		return syscall.EPERM
	}

	// TODO(vadim): don't allow directory rename to affect input files

	existingTargetNode := newParent.EmbeddedInode().GetChild(newName)
	if existingTargetNode != nil {
		existingNode, ok := existingTargetNode.Operations().(*Node)
		if !ok {
			log.Warningf("[%s] Existing target is not a *Node", n.cfs.taskID())
			return syscall.EINVAL
		}
		// Don't allow a rename to overwrite an input file.
		if existingNode.fileNode != nil {
			log.Warningf("[%s] Denied attempt to rename over input file %q", n.cfs.taskID(), existingNode.relativePath())
			return syscall.EPERM
		}
	}

	p1 := filepath.Join(n.computeScratchPath(), name)
	p2 := filepath.Join(n.cfs.scratchDir, newParent.EmbeddedInode().Path(nil), newName)

	return fs.ToErrno(os.Rename(p1, p2))
}

func (n *Node) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if n.fileNode != nil {
		out.Size = uint64(n.fileNode.GetDigest().SizeBytes)
		out.Mode = 0444
		if n.fileNode.GetIsExecutable() {
			out.Mode |= 0111
		}
		return fs.OK
	}
	if n.scratchFilePath != "" {
		s, err := os.Lstat(n.computeScratchPath())
		if err != nil {
			return fs.ToErrno(err)
		}
		out.Mode = uint32(s.Mode().Perm())
		out.Size = uint64(s.Size())
		return fs.OK
	}
	// Symlink or directory.
	out.Mode = 0777
	return fs.OK
}

func (n *Node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	// Do not allow modifying attributes of input files.
	if n.isInput {
		log.Warningf("[%s] Denied attempt to change input file attributes %q", n.cfs.taskID(), n.relativePath())
		return syscall.EPERM
	}

	if m, ok := in.GetMode(); ok {
		if err := os.Chmod(n.computeScratchPath(), os.FileMode(m)); err != nil {
			return fs.ToErrno(err)
		}
	}

	// TODO(vadim): support setting size

	return n.Getattr(ctx, f, out)
}

func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n.cfs.verbose {
		log.Debugf("Mkdir %q", filepath.Join(n.relativePath(), name))
	}

	scratchPath := filepath.Join(n.computeScratchPath(), name)

	if err := os.MkdirAll(scratchPath, 0755); err != nil {
		log.Warningf("[%s] Could not make dirs for %q: %s", n.cfs.taskID(), scratchPath, err)
		return nil, fs.ToErrno(err)
	}

	child := &Node{cfs: n.cfs, parent: n}
	inode := n.cfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFDIR})
	if !n.AddChild(name, inode, false) {
		log.Warningf("[%s] Mkdir could not add child %q to %q, already exists", n.cfs.taskID(), name, n.relativePath())
		return nil, syscall.EIO
	}
	return inode, 0
}

func (n *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	if n.cfs.verbose {
		log.Debugf("[%s] Rmdir %q", n.cfs.taskID(), filepath.Join(n.relativePath(), name))
	}
	fullPath := filepath.Join(n.computeScratchPath(), name)
	return fs.ToErrno(os.Remove(fullPath))
}

func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	if n.cfs.verbose {
		log.Debugf("[%s] Readdir %q", n.cfs.taskID(), n.relativePath())
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

func (n *Node) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (node *fs.Inode, errno syscall.Errno) {
	src := filepath.Join(n.relativePath(), name)
	if n.cfs.verbose {
		log.Debugf("[%s] Symlink %q -> %q", n.cfs.taskID(), src, target)
	}
	child := &Node{cfs: n.cfs, parent: n, symlinkTarget: target}
	inode := n.cfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFLNK})
	if !n.AddChild(name, inode, false) {
		log.Warningf("[%s] Symlink could not add child %q to %q, already exists", n.cfs.taskID(), name, n.relativePath())
		return nil, syscall.EIO
	}

	return inode, 0
}

func (n *Node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	if n.cfs.verbose {
		log.Debugf("[%s] Readlink %q", n.cfs.taskID(), n.relativePath())
	}
	if n.symlinkTarget != "" {
		return []byte(n.symlinkTarget), 0
	}
	return nil, syscall.EINVAL
}

func (n *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	relPath := filepath.Join(n.relativePath(), name)
	if n.cfs.verbose {
		log.Debugf("[%s] Unlink %q", n.cfs.taskID(), relPath)
	}
	existingTargetNode := n.GetChild(name)
	if existingTargetNode == nil {
		log.Warningf("[%s] Child %q does not exist in %q", n.cfs.taskID(), name, n.relativePath())
		return syscall.EINVAL
	}

	existingNode, ok := existingTargetNode.Operations().(*Node)
	if !ok {
		log.Warningf("[%s] Existing node is not a *Node", n.cfs.taskID())
		return syscall.EINVAL
	}

	if existingNode.fileNode != nil {
		log.Warningf("[%s] Denied attempt to unlink input file %q", n.cfs.taskID(), relPath)
		return syscall.EPERM
	}

	if existingNode.scratchFilePath != "" {
		return fs.ToErrno(os.Remove(existingNode.computeScratchPath()))
	}

	if existingNode.symlinkTarget != "" {
		return fs.OK
	}

	return syscall.ENOTSUP
}

func (n *Node) CopyFileRange(ctx context.Context, fhIn fs.FileHandle, offIn uint64, out *fs.Inode, fhOut fs.FileHandle, offOut uint64, len uint64, flags uint64) (uint32, syscall.Errno) {
	lfIn, ok := fhIn.(*instrumentedLoopbackFile)
	if !ok {
		log.Warningf("[%s] In is not a *instrumentedLoopbackFile", n.cfs.taskID())
		return 0, syscall.ENOSYS
	}
	lfOut, ok := fhOut.(*instrumentedLoopbackFile)
	if !ok {
		log.Warningf("[%s] Out is not a *instrumentedLoopbackFile", n.cfs.taskID())
		return 0, syscall.ENOSYS
	}

	rOffset := int64(offIn)
	wOffset := int64(offOut)
	bytesCopied, err := unix.CopyFileRange(lfIn.fd, &rOffset, lfOut.fd, &wOffset, int(len), int(flags))
	return uint32(bytesCopied), fs.ToErrno(err)
}

type instrumentedReadResult struct {
	fuse.ReadResult
	lf *instrumentedLoopbackFile
}

func (r *instrumentedReadResult) Bytes(buf []byte) ([]byte, fuse.Status) {
	b, s := r.ReadResult.Bytes(buf)
	r.lf.cfs.CountReadBytes(len(b))
	return b, s
}

func (r *instrumentedReadResult) Size() int {
	return r.ReadResult.Size()
}

func (r *instrumentedReadResult) Done() {
	r.ReadResult.Done()
}

type instrumentedLoopbackFile struct {
	fs.FileHandle
	cfs      *CASFS
	fullPath string
	fd       int
}

func (f *instrumentedLoopbackFile) Allocate(ctx context.Context, off uint64, size uint64, mode uint32) syscall.Errno {
	fa, ok := f.FileHandle.(fs.FileAllocater)
	if !ok {
		log.Error("Handle does not implement FileAllocator")
		return syscall.EINVAL
	}
	return fa.Allocate(ctx, off, size, mode)
}

func (f *instrumentedLoopbackFile) Release(ctx context.Context) syscall.Errno {
	fr, ok := f.FileHandle.(fs.FileReleaser)
	if !ok {
		log.Error("Handle does not implement FileReleaser")
		return syscall.EINVAL
	}
	return fr.Release(ctx)
}

func (f *instrumentedLoopbackFile) Flush(ctx context.Context) syscall.Errno {
	ff, ok := f.FileHandle.(fs.FileFlusher)
	if !ok {
		log.Error("Handle does not implement FileFlusher")
		return syscall.EINVAL
	}
	return ff.Flush(ctx)
}

func (f *instrumentedLoopbackFile) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	fs, ok := f.FileHandle.(fs.FileFsyncer)
	if !ok {
		log.Error("Handle does not implement FileFsyncer")
		return syscall.EINVAL
	}
	return fs.Fsync(ctx, flags)
}

func (f *instrumentedLoopbackFile) Read(ctx context.Context, buf []byte, off int64) (res fuse.ReadResult, errno syscall.Errno) {
	fr, ok := f.FileHandle.(fs.FileReader)
	if !ok {
		log.Error("Handle does not implement FileReader")
		return nil, syscall.EINVAL
	}
	res, errno = fr.Read(ctx, buf, off)
	res = &instrumentedReadResult{res, f}
	return
}

func (f *instrumentedLoopbackFile) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	fw, ok := f.FileHandle.(fs.FileWriter)
	if !ok {
		log.Error("Handle does not implement FileWriter")
		return 0, syscall.EINVAL
	}
	n, err := fw.Write(ctx, data, off)
	f.cfs.CountWrittenBytes(len(data))
	return n, err
}
