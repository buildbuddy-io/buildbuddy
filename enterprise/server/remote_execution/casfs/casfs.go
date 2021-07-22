package casfs

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/dirtools"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/docker/go-units"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type Node struct {
	fs.Inode

	cfs    *CASFs
	parent *Node

	name      string
	localFile string
	fileNode  *repb.FileNode
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

func (f *instrumentedLoopbackFile) Release(ctx context.Context) syscall.Errno {
	errno := f.FileHandle.(fs.FileReleaser).Release(ctx)
	f.mu.Lock()
	f.released = true
	log.Warningf("RELEASE %s (read %s, wrote %s)", f.fullPath, units.HumanSize(float64(f.readBytes)), units.HumanSize(float64(f.wroteBytes)))
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
	log.Warningf("FSYNC DONE %s", f.fullPath)
	return errno
}

func (n *Node) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	relPath := n.relativePath()
	fullPath := filepath.Join(n.cfs.scratchDir, relPath)
	//log.Infof("Open file at %q", fullPath)

	if n.localFile != "" {
		//log.Infof("Serve local file: %q", n.localFile)
		fd, err := syscall.Open(n.fullPath(), int(flags), 0)
		if err != nil {
			log.Warningf("open failed: %s", err)
			return nil, 0, syscall.EIO
		}
		lf := &instrumentedLoopbackFile{FileHandle: fs.NewLoopbackFile(fd), fullPath: fullPath}
		n.cfs.mu.Lock()
		n.cfs.loopbackFiles = append(n.cfs.loopbackFiles, lf)
		n.cfs.mu.Unlock()
		return lf, 0, 0
	}

	if n.fileNode == nil {
		log.Warningf("Read request for node %q w/o file info", fullPath)
		return nil, 0, syscall.ENOTSUP
	}
	fileMap := dirtools.FileMap{
		digest.NewKey(n.fileNode.GetDigest()): {&dirtools.FilePointer{
			FullPath:     fullPath,
			RelativePath: relPath,
			FileNode:     n.fileNode,
		}},
	}

	rpcCtx := context.Background()
	rpcCtx = context.WithValue(rpcCtx, "x-buildbuddy-jwt", n.cfs.jwt)
	//log.Infof("Using instance name %q", n.cfs.instanceName)
	err := dirtools.FetchFiles(rpcCtx, n.cfs.env, n.cfs.instanceName, fileMap, &dirtools.GetTreeOpts{})
	if err != nil {
		log.Warningf("fetch failed: %s", err)
		return nil, 0, syscall.EIO
	}
	fd, err := syscall.Open(fullPath, int(flags), 0)
	if err != nil {
		log.Warningf("open failed: %s", err)
		return nil, 0, syscall.EIO
	}
	lf := &instrumentedLoopbackFile{FileHandle: fs.NewLoopbackFile(fd), fullPath: fullPath}
	n.cfs.mu.Lock()
	n.cfs.loopbackFiles = append(n.cfs.loopbackFiles, lf)
	n.cfs.mu.Unlock()
	return lf, 0, 0
}

func (n *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	fullPath := filepath.Join(n.fullPath(), name)
	fd, err := syscall.Open(fullPath, int(flags), mode)
	if err != nil {
		return nil, nil, 0, fs.ToErrno(err)
	}
	lf := &instrumentedLoopbackFile{FileHandle: fs.NewLoopbackFile(fd), fullPath: fullPath}
	n.cfs.mu.Lock()
	n.cfs.loopbackFiles = append(n.cfs.loopbackFiles, lf)
	n.cfs.mu.Unlock()
	child := &Node{cfs: n.cfs, name: name, parent: n, localFile: fullPath}
	inode := n.cfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFREG})
	if !n.AddChild(name, inode, false) {
		log.Warningf("could not add child %q", name)
		return nil, nil, 0, syscall.EIO
	}
	return inode, lf, 0, 0
}

func (n *Node) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0777
	if n.fileNode != nil {
		out.Size = uint64(n.fileNode.GetDigest().GetSizeBytes())
	}
	return 0
}

func (n *Node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	log.Infof("Setattr %q: %+v", n.fullPath(), in)
	return 0
}

func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// TODO: check if this is called for existing directories
	fullPath := filepath.Join(n.fullPath(), name)
	log.Infof("Mkdir %q", fullPath)
	if err := os.Mkdir(fullPath, os.FileMode(mode)); err != nil {
		log.Infof("Could not create %q: %s", fullPath, err)
		return nil, fs.ToErrno(err)
	}
	child := &Node{cfs: n.cfs, name: name, parent: n}
	inode := n.cfs.root.NewPersistentInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFDIR})
	if !n.AddChild(name, inode, false) {
		log.Warningf("could not add child %q", name)
		return nil, syscall.EIO
	}
	log.Infof("Successfully created %q", fullPath)
	return inode, 0
}

type CASFs struct {
	env          environment.Env
	instanceName string

	server *fuse.Server // Not set until FS is mounted.

	scratchDir string
	root       *Node
	jwt        string

	mu            sync.Mutex
	loopbackFiles []*instrumentedLoopbackFile
}

func NewCASFs(env environment.Env, scratchDir string) *CASFs {
	cfs := &CASFs{
		env:        env,
		scratchDir: scratchDir,
	}
	root := &Node{cfs: cfs}
	cfs.root = root
	return cfs
}

func (cfs *CASFs) Mount(dir string) error {
	// XXX: investigate which options we should be setting
	server, err := fs.Mount(dir, cfs.root, &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: true,
			//Debug:      true,
		},
	})
	if err != nil {
		return status.UnknownErrorf("could not mount CAS FS at %q: %s", dir, err)
	}
	cfs.server = server
	return nil
}

func (cfs *CASFs) PrepareLayout(ctx context.Context, instanceName string, dirMap map[digest.Key]*repb.Directory, rootDirectoryDigest *repb.Digest) error {
	if rootDirectoryDigest.Hash == digest.EmptySha256 {
		return nil
	}

	//log.Infof("Set instance name to %q", instanceName)
	cfs.instanceName = instanceName
	var walkDir func(dir *repb.Directory, node *Node) error
	walkDir = func(dir *repb.Directory, parentNode *Node) error {
		for _, childDirNode := range dir.Directories {
			//log.Infof("Walk %q", childDirNode.Name)
			child := &Node{cfs: cfs, name: childDirNode.Name, parent: parentNode}

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
			//log.Infof("File: %s", childFileNode.Name)
			//if childFileNode.Name == "process_wrapper" {
			//	log.Infof("Parent: %s", parentNode.relativePath())
			//	log.Infof("FULL PROTO %d:\n%s", i, proto.MarshalTextString(childFileNode))
			//}
			child := &Node{cfs: cfs, name: childFileNode.Name, parent: parentNode, fileNode: childFileNode}
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

	return walkDir(dir, cfs.root)
}

func (cfs *CASFs) Unmount() error {
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

func (cfs *CASFs) SetJWT(jwt string) {
	cfs.jwt = jwt
}
