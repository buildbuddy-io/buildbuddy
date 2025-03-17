package vbd

import (
	"bufio"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

	// flockSuffix is a suffix given to the lock file associated with the VBD
	// mount. The lock file is created as a sibling of the mount directory, with
	// this suffix appended.
	//
	// Note: normally, a cleaner approach would be to just lock the directory
	// itself. However, that doesn't work in this case, because we're mounting
	// something over the directory path, causing the underlying directory node
	// to change before/after mounting. flock() locks the underlying node, not
	// the path name. See `man 2 flock` for more info.
	flockSuffix = ".lock"
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
	lockFile  *os.File
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
func (f *FS) Mount(ctx context.Context, path string) error {
	if f.mountPath != "" {
		return status.InternalErrorf("vbd is already mounted")
	}
	f.mountPath = path

	if err := os.MkdirAll(path, 0755); err != nil {
		return err
	}

	// Note: we use a sibling lock file rather than just locking the VBD mount
	// directory, since the mount directory path no longer refers to the same
	// underlying node once the FUSE dir is mounted to it.
	lockFile, err := os.Create(path + flockSuffix)
	if err != nil {
		return status.WrapError(err, "create file lock")
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		return status.WrapError(err, "acquire file lock")
	}
	f.lockFile = lockFile

	nodeAttrTimeout := 6 * time.Hour
	opts := &fusefs.Options{
		EntryTimeout: &nodeAttrTimeout,
		AttrTimeout:  &nodeAttrTimeout,
		MountOptions: fuse.MountOptions{
			AllowOther: true,
			// Debug:         true,
			DisableXAttrs: true,
			// Don't depend on `fusermount`.
			// Disable fallback to fusermount as well, since it can cause
			// deadlocks. See https://github.com/hanwen/go-fuse/issues/506
			DirectMountStrict: true,
			FsName:            "vbd",
			MaxWrite:          fuse.MAX_KERNEL_WRITE,
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
	inode := f.root.NewPersistentInode(ctx, child, attr)
	f.root.AddChild(FileName, inode, false /*=overwrite*/)

	return nil
}

func (f *FS) Unmount(ctx context.Context) error {
	// Unmount in the background to prevent tasks from being blocked if it
	// hangs forever.
	// Log an error if this happens, since this is a goroutine leak.
	resultCh := make(chan error)
	go func() {
		defer close(resultCh)
		err := f.unmount(ctx)
		select {
		case resultCh <- err:
			// Since resultCh is unbuffered, this only happens when the outer
			// function received from the channel and will return this err.
		case <-ctx.Done():
			// Nothing is waiting for this result, so log it here.
			if err != nil {
				log.CtxErrorf(ctx, "Failed to unmount %s in the background after context was cancelled: %s", f.mountPath, err)
			} else {
				log.CtxInfof(ctx, "Unmounted %s in the background, even after the context was canceled", f.mountPath)
			}
		}
	}()
	select {
	case err := <-resultCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// FilePath returns the path to the mounted VBD file.
// It returns an error if the VBD is not mounted.
func (f *FS) FilePath() (string, error) {
	if f.mountPath == "" {
		return "", status.UnavailableError("VBD is not mounted")
	}
	return filepath.Join(f.mountPath, FileName), nil
}

func (f *FS) unmount(ctx context.Context) error {
	err := f.server.Unmount()
	f.server.Wait()
	f.server = nil
	if err == nil {
		// If we successfully unmounted, then the mount path should point to
		// an empty dir. Remove it.
		if err := os.Remove(f.mountPath); err != nil {
			log.CtxErrorf(ctx, "Failed to remove vbd mount path %s: %s", f.mountPath, err)
		}
	}
	if err := os.Remove(f.lockFile.Name()); err != nil {
		log.CtxErrorf(ctx, "Failed to remove vbd lock file: %s", err)
	}
	if err := f.lockFile.Close(); err != nil {
		log.CtxErrorf(ctx, "Failed to unlock vbd lock file: %s", err)
	}
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
var _ fusefs.FileFsyncer = (*fileHandle)(nil)

func (h *fileHandle) Read(ctx context.Context, p []byte, off int64) (res fuse.ReadResult, errno syscall.Errno) {
	return &reader{ctx, h.file, off, len(p)}, 0
}

func (h *fileHandle) Write(ctx context.Context, p []byte, off int64) (uint32, syscall.Errno) {
	n, err := h.file.WriteAt(p, off)
	if err != nil {
		log.CtxErrorf(ctx, "VBD write failed: %s", err)
		return uint32(n), syscall.EIO
	}
	return uint32(n), 0
}

func (h *fileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	// Do nothing for now; snaploader will sync contents to disk before adding to cache.
	return fusefs.OK
}

type reader struct {
	ctx  context.Context
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
		log.CtxErrorf(r.ctx, "VBD read failed: %s", err)
		return nil, fuse.EIO
	}
	return p[:length], fuse.OK
}

func (r *reader) Size() int {
	return r.size
}

func (r *reader) Done() {}

// CleanStaleMounts unmounts all VBD mounts on the system that are not currently
// in use.
func CleanStaleMounts() error {
	f, err := os.Open("/proc/mounts")
	if err != nil {
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		name, path := fields[0], fields[1]
		if name != "vbd" {
			continue
		}

		// We keep a lockfile for each VBD mount that determines whether it's
		// still in use. If we can successfully lock it, then it must no longer
		// be in use by any process, and should be safe to unmount.

		f, err := os.Open(path + flockSuffix)
		if err != nil {
			if os.IsNotExist(err) {
				// The dir was removed since we initially listed the mounts;
				// this is normal.
				continue
			}
			return status.InternalErrorf("unmount vbd: open lockfile: %s", err)
		}
		defer f.Close()
		// Try to lock the file but don't block if it's in use.
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			log.Debugf("Not unmounting in-use vbd mount at %q", path)
			continue
		}

		b, err := exec.Command("fusermount", "-u", path).CombinedOutput()
		if err != nil {
			return status.InternalErrorf("unmount vbd: fusermount -u: %q", string(b))
		}

		// Clean up the lock file too.
		if err := os.Remove(f.Name()); err != nil {
			log.Warningf("Failed to remove vbd lockfile: %s", err)
		}

		log.Debugf("Unmounted stale vbd at %q", path)
	}
	return nil
}
