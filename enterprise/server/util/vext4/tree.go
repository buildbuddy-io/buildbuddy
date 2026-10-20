//go:build linux

package vext4

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/cache/dirtools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// lostAndFoundDirName is the directory mke2fs creates in every image and
	// e2fsck expects to find.
	lostAndFoundDirName = "lost+found"
)

// inodeList holds the inodes of a directory tree by inode number minus
// one. Reserved inodes other than the root are nil.
//
// To get an inodeList from an existing on-disk directory, use walkDir. To
// get one from a lazy tree containing only CAS references, use
// walkInputTree.
type inodeList []*inode

// walkDir reads the directory tree at dir into inodes, numbered in walk
// order.
func walkDir(ctx context.Context, dir string) (inodeList, error) {
	b := newBuilder()
	st, err := lstat(dir)
	if err != nil {
		return nil, err
	}
	if st.Mode&syscall.S_IFMT != syscall.S_IFDIR {
		return nil, fmt.Errorf("%s is not a directory", dir)
	}
	root := newInode(kindDir, st)
	b.addRoot(root)
	if err := b.addDir(ctx, root, dir); err != nil {
		return nil, err
	}
	b.addLostFound(root)
	return b.inodes, nil
}

// walkInputTree reads the input tree in spec into inodes, numbered in walk
// order. Every inode is owned by root and timestamped with the current
// time, since the tree records neither.
func walkInputTree(spec *Tree) (inodeList, error) {
	inputs := spec.Inputs
	if inputs == nil {
		inputs = &repb.Tree{}
	}
	if err := dirtools.ValidateTreeNames(inputs, false); err != nil {
		return nil, err
	}
	_, dirs, err := dirtools.DirMapFromTree(inputs, spec.DigestFunction)
	if err != nil {
		return nil, err
	}
	b := newBuilder()
	root := newInode(kindDir, b.entryStat(syscall.S_IFDIR|0755))
	b.addRoot(root)
	if err := b.addProtoDir(root, "", inputs.GetRoot(), dirs, spec); err != nil {
		return nil, err
	}
	for _, dir := range spec.Dirs {
		if err := b.mkdirAll(root, dir); err != nil {
			return nil, err
		}
	}
	b.addLostFound(root)
	return b.inodes, nil
}

type inodeKind uint8

const (
	kindFile inodeKind = iota
	kindDir
	kindSymlink
)

// inode is a file, directory or symlink of the tree. It holds the on-disk
// attributes, a directory's entries or a symlink's target, where a file's
// contents come from, and, once layout has run, the blocks it was given.
type inode struct {
	kind inodeKind
	num  uint32
	// parentNum is a directory's parent, for its ".." entry.
	parentNum uint32
	mode      uint32
	uid       uint32
	gid       uint32
	// size is a file's or symlink target's length. A directory's size is
	// its blocks, see sizeBytes.
	size int64
	// links is the number of directory entries referring to the inode,
	// counting a directory's "." and its children's "..".
	links               uint32
	atime, mtime, ctime syscall.Timespec
	// source is where a file's contents come from.
	source Source
	// children are a directory's entries, in the order they are written.
	children []dirEntry
	// target is a symlink's target.
	target string

	// extents and leaves are set by layout. extents are the blocks holding
	// the inode's data, and leaves its extent index blocks, needed when it
	// has more extents than fit in the inode.
	extents []extent
	leaves  []uint64
}

// dataBlocks returns the number of blocks the inode's extents cover, which
// is a file's size in blocks, a directory's entry blocks, or one block for
// a symlink whose target does not fit in the inode.
func (in *inode) dataBlocks() uint64 {
	switch in.kind {
	case kindFile:
		return (uint64(in.size) + blockSize - 1) / blockSize
	case kindDir:
		return dirBlocksFor(in.children)
	default:
		if in.isFastSymlink() {
			return 0
		}
		return 1
	}
}

// sizeBytes returns the inode's i_size.
func (in *inode) sizeBytes() int64 {
	if in.kind == kindDir {
		return int64(in.dataBlocks()) * blockSize
	}
	return in.size
}

// isFastSymlink returns whether the symlink's target is stored in the
// inode itself rather than in a block.
func (in *inode) isFastSymlink() bool {
	return in.kind == kindSymlink && len(in.target) < fastSymlinkMax
}

// fileType returns the inode's type as a directory entry records it.
func (in *inode) fileType() uint8 {
	switch in.kind {
	case kindDir:
		return fileTypeDir
	case kindSymlink:
		return fileTypeSymlink
	default:
		return fileTypeRegular
	}
}

// child returns the directory's entry with the name, or nil.
func (in *inode) child(name string) *inode {
	for _, e := range in.children {
		if e.name == name {
			return e.inode
		}
	}
	return nil
}

type dirEntry struct {
	name  string
	inode *inode
}

// hostFile is the Source for a file on the host, by path.
type hostFile string

func (p hostFile) Open(context.Context) (*os.File, error) {
	return os.Open(string(p))
}

// hardLinkKey identifies a host file, so that paths that are hard links of
// the same file share an inode.
type hardLinkKey struct {
	dev uint64
	ino uint64
}

// builder collects the inodes of a tree as walkDir or walkInputTree adds them,
// numbering them in order, and remembers the host files and digests seen
// so far, so that hard links share an inode and files with the same
// contents share a Source.
type builder struct {
	inodes inodeList
	// now timestamps the inodes that have no time of their own.
	now       syscall.Timespec
	hardLinks map[hardLinkKey]*inode
	// sources holds the Source of each file digest seen so far, so that
	// files with the same contents share one.
	sources map[digest.Key]Source
}

func newBuilder() *builder {
	return &builder{
		inodes:    make(inodeList, firstInode-1),
		now:       syscall.NsecToTimespec(time.Now().UnixNano()),
		hardLinks: map[hardLinkKey]*inode{},
		sources:   map[digest.Key]Source{},
	}
}

// addRoot records in as the root directory, which has the fixed inode
// number rootInode and is its own parent.
func (b *builder) addRoot(in *inode) {
	in.num = rootInode
	in.parentNum = rootInode
	b.inodes[rootInode-1] = in
}

// addDir reads the directory at path into in and recurses into its
// subdirectories.
func (b *builder) addDir(ctx context.Context, in *inode, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	in.links = 2
	for _, e := range entries {
		name := e.Name()
		childPath := filepath.Join(path, name)
		if len(name) > maxNameLen {
			return fmt.Errorf("%s: name longer than %d bytes", childPath, maxNameLen)
		}
		info, err := e.Info()
		if err != nil {
			return fmt.Errorf("stat %s: %w", childPath, err)
		}
		st := info.Sys().(*syscall.Stat_t)
		var child *inode
		switch st.Mode & syscall.S_IFMT {
		case syscall.S_IFDIR:
			child = b.addInode(kindDir, st)
			child.parentNum = in.num
			in.links++
			if err := b.addDir(ctx, child, childPath); err != nil {
				return err
			}
		case syscall.S_IFREG:
			child = b.addFile(st, childPath)
		case syscall.S_IFLNK:
			child, err = b.addSymlink(st, childPath)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("%s: unsupported file type %#o", childPath, st.Mode&syscall.S_IFMT)
		}
		in.children = append(in.children, dirEntry{name: name, inode: child})
	}
	return nil
}

// addFile returns the inode for the regular file at path. Paths that are
// hard links of the same host file share an inode, as they do with mke2fs.
func (b *builder) addFile(st *syscall.Stat_t, path string) *inode {
	key := hardLinkKey{dev: uint64(st.Dev), ino: st.Ino}
	if st.Nlink > 1 {
		if in := b.hardLinks[key]; in != nil {
			in.links++
			return in
		}
	}
	in := b.addInode(kindFile, st)
	in.links = 1
	in.size = st.Size
	in.source = hostFile(path)
	if st.Nlink > 1 {
		b.hardLinks[key] = in
	}
	return in
}

// addSymlink returns the inode for the symlink at path.
func (b *builder) addSymlink(st *syscall.Stat_t, path string) (*inode, error) {
	target, err := os.Readlink(path)
	if err != nil {
		return nil, fmt.Errorf("readlink: %w", err)
	}
	if len(target) >= blockSize {
		return nil, fmt.Errorf("%s: symlink target longer than %d bytes", path, blockSize-1)
	}
	in := b.addInode(kindSymlink, st)
	in.links = 1
	in.size = int64(len(target))
	in.target = target
	return in, nil
}

// addProtoDir adds the entries of the input tree directory dir, at the
// given path relative to the root, to in and recurses into its
// subdirectories, looking them up by digest in dirs.
func (b *builder) addProtoDir(in *inode, dirPath string, dir *repb.Directory, dirs map[digest.Key]*repb.Directory, spec *Tree) error {
	in.links = 2
	for _, d := range dir.GetDirectories() {
		childPath := path.Join(dirPath, d.GetName())
		if len(d.GetName()) > maxNameLen {
			return fmt.Errorf("%s: name longer than %d bytes", childPath, maxNameLen)
		}
		sub, ok := dirs[digest.NewKey(d.GetDigest())]
		if !ok && !digest.IsEmptyHash(d.GetDigest(), spec.DigestFunction) {
			return fmt.Errorf("%s: directory %s is not in the tree", childPath, digest.String(d.GetDigest()))
		}
		child := b.newEntry(kindDir, syscall.S_IFDIR|0755)
		child.parentNum = in.num
		in.links++
		if err := b.addProtoDir(child, childPath, sub, dirs, spec); err != nil {
			return err
		}
		in.children = append(in.children, dirEntry{name: d.GetName(), inode: child})
	}
	for _, f := range dir.GetFiles() {
		childPath := path.Join(dirPath, f.GetName())
		if len(f.GetName()) > maxNameLen {
			return fmt.Errorf("%s: name longer than %d bytes", childPath, maxNameLen)
		}
		mode := uint32(syscall.S_IFREG | 0644)
		if f.GetIsExecutable() {
			mode |= 0111
		}
		child := b.newEntry(kindFile, mode)
		child.links = 1
		child.size = f.GetDigest().GetSizeBytes()
		key := digest.NewKey(f.GetDigest())
		source := b.sources[key]
		if source == nil {
			source = spec.Source(childPath, f)
			b.sources[key] = source
		}
		child.source = source
		in.children = append(in.children, dirEntry{name: f.GetName(), inode: child})
	}
	for _, s := range dir.GetSymlinks() {
		childPath := path.Join(dirPath, s.GetName())
		if len(s.GetName()) > maxNameLen {
			return fmt.Errorf("%s: name longer than %d bytes", childPath, maxNameLen)
		}
		if len(s.GetTarget()) >= blockSize {
			return fmt.Errorf("%s: symlink target longer than %d bytes", childPath, blockSize-1)
		}
		child := b.newEntry(kindSymlink, syscall.S_IFLNK|0777)
		child.links = 1
		child.size = int64(len(s.GetTarget()))
		child.target = s.GetTarget()
		in.children = append(in.children, dirEntry{name: s.GetName(), inode: child})
	}
	return nil
}

// mkdirAll adds the directory at the slash-separated path p, relative to
// root, along with any missing parents.
func (b *builder) mkdirAll(root *inode, p string) error {
	p = path.Clean(p)
	if p == "." {
		return nil
	}
	if path.IsAbs(p) || p == ".." || strings.HasPrefix(p, "../") {
		return fmt.Errorf("%s: directory outside the tree", p)
	}
	in := root
	for name := range strings.SplitSeq(p, "/") {
		child := in.child(name)
		if child == nil {
			child = b.newEntry(kindDir, syscall.S_IFDIR|0755)
			child.parentNum = in.num
			child.links = 2
			in.links++
			in.children = append(in.children, dirEntry{name: name, inode: child})
		} else if child.kind != kindDir {
			return fmt.Errorf("%s: %s is not a directory", p, name)
		}
		in = child
	}
	return nil
}

// addLostFound adds an empty lost+found directory to the root, unless the
// tree has one, to match mke2fs.
func (b *builder) addLostFound(root *inode) {
	if root.child(lostAndFoundDirName) != nil {
		return
	}
	lf := b.newEntry(kindDir, syscall.S_IFDIR|0700)
	lf.parentNum = root.num
	lf.links = 2
	root.links++
	root.children = append(root.children, dirEntry{name: lostAndFoundDirName, inode: lf})
}

// newEntry records a new inode for an entry that no host file describes,
// such as an input tree entry or lost+found, with the attributes entryStat
// gives it.
func (b *builder) newEntry(kind inodeKind, mode uint32) *inode {
	return b.addInode(kind, b.entryStat(mode))
}

// entryStat describes an entry that has no host file behind it, so it is
// owned by root and timestamped with the build time.
func (b *builder) entryStat(mode uint32) *syscall.Stat_t {
	return &syscall.Stat_t{Mode: mode, Atim: b.now, Mtim: b.now, Ctim: b.now}
}

// addInode records a new inode with the next inode number.
func (b *builder) addInode(kind inodeKind, st *syscall.Stat_t) *inode {
	in := newInode(kind, st)
	in.num = uint32(len(b.inodes) + 1)
	b.inodes = append(b.inodes, in)
	return in
}

func newInode(kind inodeKind, st *syscall.Stat_t) *inode {
	return &inode{
		kind:  kind,
		mode:  st.Mode,
		uid:   st.Uid,
		gid:   st.Gid,
		atime: st.Atim,
		mtime: st.Mtim,
		ctime: st.Ctim,
	}
}

func lstat(path string) (*syscall.Stat_t, error) {
	st := &syscall.Stat_t{}
	if err := syscall.Lstat(path, st); err != nil {
		return nil, fmt.Errorf("lstat %s: %w", path, err)
	}
	return st, nil
}
