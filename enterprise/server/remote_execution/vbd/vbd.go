package vbd

import (
	"context"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/hanwen/go-fuse/v2/fuse"

	fusefs "github.com/hanwen/go-fuse/v2/fs"
)

const (
	// FileName is the name of the single file exposed under the mount dir.
	FileName = "file"
)

// BlockDevice is the interface backing VBD IO operations.
type BlockDevice interface {
	io.ReaderAt
	io.WriterAt
	SizeBytes() (int64, error)
}

// FS represents a handle on a VBD FS. Once mounted, the mounted directory
// exposes a single file. The file name is the const FileName. IO operations on
// the file are backed by the wrapped BlockDevice.
type FS struct {
	store     BlockDevice
	root      *Node
	server    *fuse.Server
	mountPath string
}

// New returns a new FS serving the given file.
func New(store BlockDevice) (*FS, error) {
	f := &FS{store: store}
	f.root = &Node{fs: f}
	return f, nil
}

func (f *FS) SetFile(file BlockDevice) {
	f.store = file
}

// Mount mounts the FS to the given directory path.
// It exposes a single file "store" which points to the backing store.
func (f *FS) Mount(path string) error {
	if f.mountPath != "" {
		return status.InternalErrorf("vbd is already mounted")
	}
	f.mountPath = path

	if err := os.MkdirAll(path, 0755); err != nil {
		return err
	}

	nodeAttrTimeout := 6 * time.Hour
	opts := &fusefs.Options{
		EntryTimeout: &nodeAttrTimeout,
		AttrTimeout:  &nodeAttrTimeout,
		MountOptions: fuse.MountOptions{
			AllowOther: true,
			// Debug:         true,
			DisableXAttrs: true,
			// Don't depend on `fusermount`.
			DirectMount: true,
			FsName:      "vbd",
			MaxWrite:    fuse.MAX_KERNEL_WRITE,
		},
	}
	nodeFS := fusefs.NewNodeFS(f.root, opts)
	server, err := fuse.NewServer(nodeFS, path, &opts.MountOptions)
	if err != nil {
		return status.UnavailableErrorf("could not mount VBD to %q: %s", path, err)
	}

	go server.Serve()
	if err := server.WaitMount(); err != nil {
		return status.UnavailableErrorf("waiting for VBD mount failed: %s", err)
	}

	f.server = server

	attr := fusefs.StableAttr{Mode: fuse.S_IFREG}
	child := &Node{fs: f, file: f.store}
	inode := f.root.NewPersistentInode(context.TODO(), child, attr)
	f.root.AddChild(FileName, inode, false /*=overwrite*/)

	return nil
}

func (f *FS) Unmount() error {
	err := f.server.Unmount()
	f.server.Wait()
	f.server = nil
	if err := os.Remove(f.mountPath); err != nil {
		log.Errorf("Failed to unmount vbd: %s", err)
	}
	log.Debugf("Unmounted %s", f.mountPath)
	return err
}

type Node struct {
	fusefs.Inode
	fs   *FS
	file BlockDevice
}

var _ fusefs.NodeOpener = (*Node)(nil)
var _ fusefs.NodeGetattrer = (*Node)(nil)

func (n *Node) Open(ctx context.Context, flags uint32) (fusefs.FileHandle, uint32, syscall.Errno) {
	if n.file == nil {
		log.CtxErrorf(ctx, "open root dir: not supported")
		return nil, 0, syscall.EOPNOTSUPP
	}
	return &fileHandle{file: n.file}, 0, 0
}

func (n *Node) Getattr(ctx context.Context, _ fusefs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if n.file != nil {
		size, err := n.file.SizeBytes()
		if err != nil {
			log.CtxErrorf(ctx, "VBD size failed: %s", err)
			return syscall.EIO
		}
		out.Size = uint64(size)
	}
	return fusefs.OK
}

type fileHandle struct {
	file BlockDevice
}

var _ fusefs.FileReader = (*fileHandle)(nil)
var _ fusefs.FileWriter = (*fileHandle)(nil)

func (h *fileHandle) Read(ctx context.Context, p []byte, off int64) (res fuse.ReadResult, errno syscall.Errno) {
	return &reader{h.file, off, len(p)}, 0
}

func (h *fileHandle) Write(ctx context.Context, p []byte, off int64) (uint32, syscall.Errno) {
	n, err := h.file.WriteAt(p, off)
	if err != nil {
		log.CtxErrorf(ctx, "VBD write failed: %s", err)
		return uint32(n), syscall.EIO
	}
	return uint32(n), 0
}

type reader struct {
	file BlockDevice
	off  int64
	size int
}

var _ fuse.ReadResult = (*reader)(nil)

func (r *reader) Bytes(p []byte) ([]byte, fuse.Status) {
	length := r.size
	if len(p) < length {
		length = len(p)
	}
	_, err := r.file.ReadAt(p[:length], r.off)
	if err != nil {
		log.Errorf("VBD read failed: %s", err)
		return nil, fuse.EIO
	}
	return p[:length], fuse.OK
}

func (r *reader) Size() int {
	return r.size
}

func (r *reader) Done() {}
