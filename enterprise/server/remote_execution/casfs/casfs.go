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
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/docker/go-units"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

type Node struct {
	fs.Inode

	cfs    *CASFS
	parent *Node

	name string

	mu            sync.Mutex
	fileNode      *repb.FileNode
	localFile     string
	symlinkTarget string
}

func (n *Node) relativePath() string {
	var paths []string
	var visit func(n *Node)
	visit = func(n *Node) {
		if n.parent != nil {
			visit(n.parent)
		}
		paths = append(paths, n.name)
	}
	visit(n)
	return filepath.Join(paths...)
}

func (n *Node) fullPath() string {
	return filepath.Join(n.cfs.scratchDir, n.relativePath())
}

type instrumentedLoopbackFile struct {
	fs.FileHandle
	fullPath   string
	fd         int
	mu         sync.Mutex
	readBytes  int
	wroteBytes int
	released   bool
}

type instrumentedReadResult struct {
	fuse.ReadResult
	lf *instrumentedLoopbackFile
}

func (r *instrumentedReadResult) Bytes(buf []byte) ([]byte, fuse.Status) {
	b, s := r.ReadResult.Bytes(buf)
	r.lf.mu.Lock()
	r.lf.readBytes += len(b)
	r.lf.mu.Unlock()
	return b, s
}

func (r *instrumentedReadResult) Size() int {
	return r.ReadResult.Size()
}

func (r *instrumentedReadResult) Done() {
	r.ReadResult.Done()
}

func (f *instrumentedLoopbackFile) Read(ctx context.Context, buf []byte, off int64) (res fuse.ReadResult, errno syscall.Errno) {
	res, errno = f.FileHandle.(fs.FileReader).Read(ctx, buf, off)
	res = &instrumentedReadResult{res, f}
	return
}

func (f *instrumentedLoopbackFile) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	n, err := f.FileHandle.(fs.FileWriter).Write(ctx, data, off)
	f.mu.Lock()
	f.wroteBytes += len(data)
	f.mu.Unlock()
	return n, err
}

func (f *instrumentedLoopbackFile) Allocate(ctx context.Context, off uint64, size uint64, mode uint32) syscall.Errno {
	return fs.ToErrno(syscall.Fallocate(f.fd, mode, int64(off), int64(size)))
}

func (f *instrumentedLoopbackFile) Release(ctx context.Context) syscall.Errno {
	errno := f.FileHandle.(fs.FileReleaser).Release(ctx)
	f.mu.Lock()
	f.released = true
	f.mu.Unlock()
	return errno
}

func (f *instrumentedLoopbackFile) Flush(ctx context.Context) syscall.Errno {
	errno := f.FileHandle.(fs.FileFlusher).Flush(ctx)
	//log.Warningf("FLUSH DONE %s", f.fullPath)
	return errno
}

func (f *instrumentedLoopbackFile) Fsync(ctx context.Context, flags uint32) (errno syscall.Errno) {
	errno = f.FileHandle.(fs.FileFsyncer).Fsync(ctx, flags)
	//log.Warningf("FSYNC DONE %s", f.fullPath)
	return errno
}

func (n *Node) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	n.mu.Lock()
	n.mu.Unlock()

	// Don't allow writes to input files.
	if n.fileNode != nil && (int(flags)&(os.O_WRONLY|os.O_RDWR)) != 0 {
		return nil, 0, syscall.EPERM
	}

	relPath := n.relativePath()
	fullPath := filepath.Join(n.cfs.scratchDir, relPath)
	if n.localFile != "" {
		hash := ""
		if n.fileNode != nil {
			hash = fmt.Sprintf(" (%s)", n.fileNode.GetDigest().GetHash())
		}
		log.Infof("Open %q%s: serve from local file %q", n.relativePath(), hash, n.fullPath())
		fd, err := syscall.Open(n.fullPath(), int(flags), 0)
		if err != nil {
			log.Warningf("open %q%s failed: %s", n.relativePath(), hash, err)
			return nil, 0, syscall.EIO
		}
		lf := &instrumentedLoopbackFile{FileHandle: fs.NewLoopbackFile(fd), fd: fd, fullPath: fullPath}
		n.cfs.mu.Lock()
		n.cfs.loopbackFiles = append(n.cfs.loopbackFiles, lf)
		n.cfs.mu.Unlock()
		return lf, 0, 0
	}

	if n.fileNode != nil {
		log.Infof("Open %q: fetch %s from CAS", n.relativePath(), n.fileNode.GetDigest().GetHash())

		fileMap := dirtools.FileMap{
			digest.NewKey(n.fileNode.GetDigest()): {&dirtools.FilePointer{
				FullPath:     fullPath,
				RelativePath: relPath,
				FileNode:     n.fileNode,
			}},
		}

		// XXX: Should we pre-create these dirs?
		if err := os.MkdirAll(filepath.Dir(fullPath), 0777); err != nil {
			log.Infof("Could not make dir %q", fullPath)
			return nil, 0, syscall.EINVAL
		}

		err := n.cfs.fileFetcher.FetchFiles(n.cfs.instanceName, fileMap, &dirtools.GetTreeOpts{})
		if err != nil {
			log.Warningf("fetch failed: %s", err)
			return nil, 0, syscall.EIO
		}

		n.localFile = fullPath

		fd, err := syscall.Open(fullPath, int(flags), 0)
		if err != nil {
			log.Warningf("open failed: %s", err)
			return nil, 0, syscall.EIO
		}
		lf := &instrumentedLoopbackFile{FileHandle: fs.NewLoopbackFile(fd), fd: fd, fullPath: fullPath}
		n.cfs.mu.Lock()
		n.cfs.loopbackFiles = append(n.cfs.loopbackFiles, lf)
		n.cfs.mu.Unlock()
		return lf, 0, 0
	}

	return nil, 0, syscall.ENOTSUP
}

func (n *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.Infof("Create %q", filepath.Join(n.relativePath(), name))

	fullPath := filepath.Join(n.fullPath(), name)
	// XXX: Should we pre-create these dirs?
	if err := os.MkdirAll(filepath.Dir(fullPath), 0777); err != nil {
		log.Infof("Could not make dir %q", fullPath)
		return nil, 0, 0, syscall.EINVAL
	}

	fd, err := syscall.Open(fullPath, int(flags), mode)
	if err != nil {
		log.Infof("Could not open %q: %s", fullPath, err)
		return nil, nil, 0, fs.ToErrno(err)
	}
	lf := &instrumentedLoopbackFile{FileHandle: fs.NewLoopbackFile(fd), fd: fd, fullPath: fullPath}
	n.cfs.mu.Lock()
	n.cfs.loopbackFiles = append(n.cfs.loopbackFiles, lf)
	n.cfs.mu.Unlock()
	child := &Node{cfs: n.cfs, name: name, parent: n, localFile: fullPath}
	inode := n.cfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFREG})
	if !n.AddChild(name, inode, false) {
		log.Warningf("could not add child %q", name)
		return nil, nil, 0, syscall.EIO
	}

	st := syscall.Stat_t{}
	if err := syscall.Fstat(fd, &st); err != nil {
		syscall.Close(fd)
		return nil, nil, 0, fs.ToErrno(err)
	}
	out.FromStat(&st)

	return inode, lf, 0, 0
}

func (n *Node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	newParentNode := newParent.EmbeddedInode().Operations().(*Node)
	log.Infof("Rename %q => %q", filepath.Join(n.relativePath(), name), filepath.Join(newParentNode.relativePath(), newName))

	// Don't allow input files to be renamed.
	if n.fileNode != nil && (int(flags)&(os.O_WRONLY|os.O_RDWR)) != 0 {
		return syscall.EPERM
	}

	existingSrcINode := n.GetChild(name)
	if existingSrcINode == nil {
		log.Warningf("Source inode not found")
		return syscall.EINVAL
	}
	existingSrcNode := existingSrcINode.Operations().(*Node)

	// TODO(vadim): don't allow directory rename to affect input files

	existingTargetNode := newParent.EmbeddedInode().GetChild(newName)
	if existingTargetNode != nil {
		existingNode, ok := existingTargetNode.Operations().(*Node)
		if !ok {
			return syscall.EINVAL
		}
		// Don't allow a rename to overwrite an input file.
		if existingNode.fileNode != nil {
			return syscall.EPERM
		}
	}

	p1 := filepath.Join(n.fullPath(), name)
	p2 := filepath.Join(n.cfs.scratchDir, newParent.EmbeddedInode().Path(nil), newName)

	err := os.Rename(p1, p2)
	if err == nil {
		// Would it be better to create a new INode?
		existingSrcNode.mu.Lock()
		existingSrcNode.name = newName
		existingSrcNode.mu.Unlock()
	}

	return fs.ToErrno(err)
}

func (n *Node) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	//log.Infof("Getattr %q", n.relativePath())

	// XXX check if the fuse lib code automatically sets the file type so we don't need to set it?

	if n.fileNode != nil {
		out.Size = uint64(n.fileNode.GetDigest().SizeBytes)
		out.Mode = fuse.S_IFREG | 0444
		if n.fileNode.GetIsExecutable() {
			out.Mode |= 0111
		}
		return fs.OK
	} else if n.localFile != "" {
		s, err := os.Lstat(n.fullPath())
		if err != nil {
			return fs.ToErrno(err)
		}
		out.Mode = fuse.S_IFREG | uint32(s.Mode().Perm())
		out.Size = uint64(s.Size())
		return fs.OK
	} else {
		out.Mode = fuse.S_IFDIR | 0777
		return fs.OK
	}
}

func (n *Node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	log.Infof("Setattr %q", n.relativePath())

	// Do not allow modifying attributes of input files.
	if n.fileNode != nil {
		return syscall.EPERM
	}

	if m, ok := in.GetMode(); ok {
		if err := os.Chmod(n.fullPath(), os.FileMode(m)); err != nil {
			return fs.ToErrno(err)
		}
	}

	// TODO: support setting size

	return n.Getattr(ctx, f, out)
}

func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Infof("Mkdir %q", filepath.Join(n.relativePath(), name))

	fullPath := filepath.Join(n.fullPath(), name)

	log.Infof("Mkdir %q", fullPath)

	// XXX: get rid of this, pre-create output dirs instead
	if err := os.MkdirAll(fullPath, 0777); err != nil {
		log.Infof("Could not create %q: %s", fullPath, err)
		return nil, fs.ToErrno(err)
	}

	//if err := os.Mkdir(fullPath, os.FileMode(mode)); err != nil {
	//	log.Infof("Could not create %q: %s", fullPath, err)
	//	return nil, fs.ToErrno(err)
	//}
	child := &Node{cfs: n.cfs, name: name, parent: n}
	inode := n.cfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFDIR})
	if !n.AddChild(name, inode, false) {
		log.Warningf("could not add child %q", name)
		return nil, syscall.EIO
	}
	log.Infof("Successfully created %q", fullPath)
	return inode, 0
}

func (n *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	log.Infof("Rmdir %q", filepath.Join(n.relativePath(), name))
	fullPath := filepath.Join(n.fullPath(), name)
	log.Infof("Rmdir %q", fullPath)
	return fs.ToErrno(os.Remove(fullPath))
}

func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// The default implementation has a bug that can return entries in a different order across multiple readdir calls.
	log.Infof("Readdir %q", n.relativePath())
	var names []string
	for k := range n.Children() {
		names = append(names, k)
	}
	sort.Strings(names)
	r := []fuse.DirEntry{}
	for _, k := range names {
		ch := n.Children()[k]
		entry := fuse.DirEntry{
			Mode: ch.Mode(),
			Name: k,
			Ino:  ch.StableAttr().Ino,
		}
		log.Infof("Entry: %+v", entry)
		r = append(r, entry)
	}
	return fs.NewListDirStream(r), 0
}

func (n *Node) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (node *fs.Inode, errno syscall.Errno) {
	src := filepath.Join(n.relativePath(), name)
	log.Infof("Symlink %s -> %s", src, target)
	child := &Node{cfs: n.cfs, name: name, parent: n, symlinkTarget: target}
	inode := n.cfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFLNK})
	if !n.AddChild(name, inode, false) {
		log.Warningf("could not add child %q", name)
		return nil, syscall.EIO
	}

	return inode, 0
}

func (n *Node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	if n.symlinkTarget != "" {
		return []byte(n.symlinkTarget), 0
	}
	return nil, syscall.EINVAL
}

func (n *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	existingTargetNode := n.GetChild(name)
	if existingTargetNode == nil {
		return syscall.EINVAL
	}

	existingNode, ok := existingTargetNode.Operations().(*Node)
	if !ok {
		return syscall.EINVAL
	}

	if existingNode.fileNode != nil {
		return syscall.EPERM
	}

	if existingNode.localFile != "" {
		log.Infof("Unlink %q", existingNode.fullPath())
		return fs.ToErrno(os.Remove(existingNode.fullPath()))
	}

	// XXX: handle directories and symlinks

	return syscall.ENOTSUP
}

func (n *Node) CopyFileRange(ctx context.Context, fhIn fs.FileHandle, offIn uint64, out *fs.Inode, fhOut fs.FileHandle, offOut uint64, len uint64, flags uint64) (uint32, syscall.Errno) {
	lfIn, ok := fhIn.(*instrumentedLoopbackFile)
	if !ok {
		return 0, syscall.ENOSYS
	}
	lfOut, ok := fhOut.(*instrumentedLoopbackFile)
	if !ok {
		return 0, syscall.ENOSYS
	}

	rOffset := int64(offIn)
	wOffset := int64(offOut)
	bytesCopied, err := unix.CopyFileRange(lfIn.fd, &rOffset, lfOut.fd, &wOffset, int(len), int(flags))
	return uint32(bytesCopied), fs.ToErrno(err)
}

// XXX: check for any security implications
// XXX: implement fallocate?

type CASFS struct {
	scratchDir  string
	fileFetcher *dirtools.BatchFileFetcher

	instanceName string

	server *fuse.Server // Not set until FS is mounted.

	root *Node

	mu            sync.Mutex
	loopbackFiles []*instrumentedLoopbackFile
}

func New(fileFetcher *dirtools.BatchFileFetcher, scratchDir string) *CASFS {
	cfs := &CASFS{
		scratchDir:  scratchDir,
		fileFetcher: fileFetcher,
	}
	root := &Node{cfs: cfs}
	cfs.root = root
	return cfs
}

func (cfs *CASFS) Mount(dir string) error {
	server, err := fs.Mount(dir, cfs.root, &fs.Options{
		MountOptions: fuse.MountOptions{
			MaxWrite: fuse.MAX_KERNEL_WRITE,
			// Needed for docker.
			AllowOther:    true,
			Debug:         true,
			DisableXAttrs: true,
		},
	})
	if err != nil {
		return status.UnknownErrorf("could not mount CAS FS at %q: %s", dir, err)
	}
	cfs.server = server
	return nil
}

func (cfs *CASFS) PrepareLayout(ctx context.Context, instanceName string, fsLayout *container.FilesystemLayout) error {
	if len(fsLayout.Inputs) == 0 {
		return nil
	}
	rootDirectory := fsLayout.Inputs[0]
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

	//log.Infof("Set instance name to %q", instanceName)
	cfs.instanceName = instanceName
	var walkDir func(dir *repb.Directory, node *Node) error
	walkDir = func(dir *repb.Directory, parentNode *Node) error {
		for _, childDirNode := range dir.Directories {
			//log.Infof("Walk %q", childDirNode.Name)
			child := &Node{cfs: cfs, name: childDirNode.Name, parent: parentNode}
			log.Infof("Input directory: %s", child.relativePath())

			inode := cfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFDIR})
			if !parentNode.AddChild(childDirNode.Name, inode, false) {
				return status.UnknownErrorf("could not add child %q", childDirNode.Name)
			}
			childDir, ok := dirMap[digest.NewKey(childDirNode.Digest)]
			if !ok {
				return status.NotFoundErrorf("could not find dir %q", childDirNode.Digest)
			}
			if err := walkDir(childDir, child); err != nil {
				return err
			}
		}
		for _, childFileNode := range dir.Files {
			//if childFileNode.Name == "process_wrapper" {
			//	log.Infof("Parent: %s", parentNode.relativePath())
			//	log.Infof("FULL PROTO %d:\n%s", i, proto.MarshalTextString(childFileNode))
			//}
			child := &Node{cfs: cfs, name: childFileNode.Name, parent: parentNode, fileNode: childFileNode}
			log.Infof("Input file: %s", child.relativePath())
			inode := cfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFREG})
			if !parentNode.AddChild(childFileNode.Name, inode, false) {
				return status.UnknownErrorf("could not add child %q", childFileNode.Name)
			}
		}
		if len(dir.Symlinks) > 0 {
			return status.FailedPreconditionErrorf("symlinks are not supported")
		}
		return nil
	}

	dir, ok := dirMap[digest.NewKey(rootDirectoryDigest)]
	if !ok {
		return status.NotFoundErrorf("could not find root dir digest %q", rootDirectoryDigest.String())
	}

	err = walkDir(dir, cfs.root)
	if err != nil {
		return err
	}

	outputDirs := make(map[string]struct{})
	for _, dir := range fsLayout.OutputDirs {
		log.Infof("Output dir: %s", dir)
		outputDirs[dir] = struct{}{}
	}
	for _, file := range fsLayout.OutputFiles {
		log.Infof("Output file: %s", file)
		outputDirs[filepath.Dir(file)] = struct{}{}
	}
	for dir := range outputDirs {
		log.Infof("Process output dir %q", dir)
		parts := strings.Split(dir, string(os.PathSeparator))
		node := cfs.root
		for _, p := range parts {
			childNode := node.GetChild(p)
			if childNode == nil {
				child := &Node{cfs: cfs, name: p, parent: node}
				inode := cfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFDIR})
				if !node.AddChild(p, inode, false) {
					return status.UnknownErrorf("could not add child %q", p)
				}
				node = child
			} else {
				node = childNode.Operations().(*Node)
			}
		}
	}

	return nil
}

func (cfs *CASFS) Unmount() error {
	if cfs.server == nil {
		return nil
	}

	cfs.mu.Lock()
	readBytes := 0
	wroteBytes := 0
	for _, lf := range cfs.loopbackFiles {
		lf.mu.Lock()
		readBytes += lf.readBytes
		wroteBytes += lf.wroteBytes
		if !lf.released {
			log.Warningf("NOT RELEASED %q", lf.fullPath)
		}
		lf.mu.Unlock()
	}
	cfs.mu.Unlock()

	log.Warningf("WORKSPACE %s (total read %s, wrote %s)", cfs.scratchDir, units.HumanSize(float64(readBytes)), units.HumanSize(float64(wroteBytes)))

	return cfs.server.Unmount()
}
