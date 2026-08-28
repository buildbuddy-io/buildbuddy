// Package ext4writer builds ext4 disk images directly from a directory tree,
// without shelling out to mke2fs.
//
// Why: `mke2fs -d` populates the image through libext2fs one 4 KiB block at a
// time (one pwrite per block, ~5 syscalls per file) on a single thread. For a
// 10k-file / 330 MB Bazel workspace that is ~1.7 s, which dominates Firecracker
// action preparation. This writer computes the whole layout up front, writes
// metadata with a handful of large writes, and copies file data with
// copy_file_range on a worker pool.
//
// The images are deliberately simple:
//   - 4 KiB blocks, 256-byte inodes, extents, flex_bg (all bitmaps and inode
//     tables live at the start of the image), sparse_super backups.
//   - No journal (workspace disks are throwaway), no checksums
//     (metadata_csum / gdt_csum), no htree directory index (directories are
//     linear; the kernel handles that fine).
//   - Hardlinks in the source tree are preserved (same inode). Symlinks are
//     preserved (fast symlinks when the target fits in the inode). Sockets,
//     FIFOs and device nodes are preserved as inodes without data.
//   - Extended attributes are not copied.
//
// The resulting image is a sparse file: unused inode tables and free blocks are
// never written.
package ext4writer

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	blockSize      = 4096
	blockShift     = 12
	inodeSize      = 256
	blocksPerGroup = 32768 // 8 * blockSize
	maxExtentLen   = 32768 // blocks per extent (ee_len must be < 32768+1 for initialized extents)
	extentSize     = 12
	extentsInInode = 4
	extentsPerLeaf = (blockSize - 12) / extentSize
	rootInode      = 2
	lostFoundInode = 11
	firstInode     = 11 // s_first_ino
	superMagic     = 0xEF53
	extentMagic    = 0xF30A

	// Superblock feature flags.
	featureIncompatFiletype = 0x0002
	featureIncompatExtents  = 0x0040
	featureIncompatFlexBG   = 0x0200
	featureROSparseSuper    = 0x0001
	featureROLargeFile      = 0x0002
	featureROHugeFile       = 0x0008
	featureRODirNlink       = 0x0020
	featureROExtraIsize     = 0x0040

	// Inode flags.
	inodeFlagExtents = 0x80000

	// Directory entry file types.
	ftUnknown = 0
	ftRegular = 1
	ftDir     = 2
	ftChrdev  = 3
	ftBlkdev  = 4
	ftFifo    = 5
	ftSock    = 6
	ftSymlink = 7

	// maxFileBytes bounds a single input file (ext4 with 4K blocks and 32-bit
	// block numbers can't address more than 16 TiB anyway).
	maxFileBytes = int64(1) << 40

	// MinImageSizeBytes is the smallest image we'll produce (one block group
	// worth of blocks is always allocated anyway, since group sizes are fixed).
	MinImageSizeBytes = int64(blocksPerGroup) * blockSize
)

// Options controls image creation.
type Options struct {
	// SizeBytes is the requested image size. It is rounded up to a whole
	// number of block groups (128 MiB each) and increased if the tree doesn't
	// fit. The image file is sparse, so a larger size costs nothing on disk.
	SizeBytes int64
	// SlackBytes is free space to leave in the image beyond what the tree
	// needs (for outputs). The image size is max(SizeBytes, needed+SlackBytes).
	SlackBytes int64
	// SlackFraction adds this fraction of the data size as extra free space
	// (e.g. 0.2 for 20% headroom, like ext4.DirectoryToImageAutoSize).
	SlackFraction float64
	// Concurrency is the number of parallel data-copy workers (default: min(8, NumCPU)).
	Concurrency int
	// ExtraInodes reserves this many extra inodes beyond what the tree needs,
	// for files the guest will create (default: 1 per 16 KiB of image, like mke2fs).
	ExtraInodes int
	// Now is the timestamp used for the filesystem metadata (default: time.Now()).
	Now time.Time
	// Reflink attempts FICLONERANGE for block-aligned file prefixes (works on
	// XFS with reflink=1 and btrfs; silently falls back elsewhere).
	Reflink bool
	// Xattrs copies extended attributes (needed for container images; Bazel
	// inputs never have them, so it's off by default to save syscalls).
	Xattrs bool
	// CopyMode selects how file data is copied: "mmap" (default: map the
	// image MAP_SHARED and pread source files directly into the mapping;
	// parallelizes well since writes are not serialized on the image inode
	// lock) or "cfr" (copy_file_range; required for Reflink).
	CopyMode string
}

// Stats describes what was written.
type Stats struct {
	Files, Dirs, Symlinks, Hardlinks int
	Xattrs                           int
	DataBytes                        int64
	ReflinkedBytes                   int64 // bytes shared with the source via FICLONERANGE
	ImageBytes                       int64
	BlockGroups                      int
	Inodes                           uint32
	WalkDuration, LayoutDuration     time.Duration
	MetadataDuration, DataDuration   time.Duration
}

// node is one filesystem object in the source tree.
type node struct {
	name     string // base name
	path     string // full source path
	mode     os.FileMode
	rawMode  uint32 // st_mode
	uid, gid uint32
	size     int64
	mtime    time.Time
	rdev     uint64
	target   string // symlink target
	children []*node
	parent   *node
	ino      uint32 // assigned inode number
	// Data layout.
	nblocks  uint32     // data blocks (excluding extent index blocks)
	extents  []extentRg // allocated data extents
	leaf     uint32     // extent leaf block if depth==1, else 0
	dirData  []byte     // rendered directory blocks
	hardlink *node      // if this path is a hardlink to an already-seen node
	links    int        // number of directory entries referencing this inode (files)
	fileNode *repb.FileNode // when building from a Tree: the input node (content via writer.open)
	ranges   []blockRange   // logical block ranges holding data (nil = [0, nblocks) i.e. dense)
	xattrs   []xattr        // extended attributes
	xattrBlk uint32         // external xattr block, if the attributes don't fit in the inode
}

// blockRange is a run of logical blocks that contain data (sparse files have
// gaps between ranges).
type blockRange struct {
	start uint32 // logical block
	n     uint32
}

type extentRg struct {
	logical uint32
	start   uint32
	len     uint32
}

func fileType(m os.FileMode) uint8 {
	switch {
	case m.IsRegular():
		return ftRegular
	case m.IsDir():
		return ftDir
	case m&os.ModeSymlink != 0:
		return ftSymlink
	case m&os.ModeNamedPipe != 0:
		return ftFifo
	case m&os.ModeSocket != 0:
		return ftSock
	case m&os.ModeCharDevice != 0:
		return ftChrdev
	case m&os.ModeDevice != 0:
		return ftBlkdev
	}
	return ftUnknown
}

func ceilDiv(a, b int64) int64 { return (a + b - 1) / b }

// DirectoryToImage creates an ext4 image at outputFile containing the
// contents of inputDir. The image is at least opts.SizeBytes large.
func DirectoryToImage(ctx context.Context, inputDir, outputFile string, opts *Options) (*Stats, error) {
	if opts == nil {
		opts = &Options{}
	}
	w := &writer{opts: *opts, stats: &Stats{}}
	if w.opts.Concurrency <= 0 {
		w.opts.Concurrency = min(8, defaultConcurrency())
	}
	if w.opts.Now.IsZero() {
		w.opts.Now = time.Now()
	}
	w.open = func(n *node) (*os.File, error) { return os.Open(n.path) }
	start := time.Now()
	if _, err := w.walk(inputDir); err != nil {
		return nil, status.WrapError(err, "walk input directory")
	}
	w.stats.WalkDuration = time.Since(start)
	return w.finish(ctx, outputFile)
}

func defaultConcurrency() int { return runtime.NumCPU() }

// finish lays out and writes the image for the already-walked tree.
func (w *writer) finish(ctx context.Context, outputFile string) (*Stats, error) {
	w.assignInodes()
	root := w.root
	start := time.Now()
	if err := w.layout(root); err != nil {
		return nil, status.WrapError(err, "compute layout")
	}
	w.stats.LayoutDuration = time.Since(start)

	f, err := os.OpenFile(outputFile, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0644)
	if err != nil {
		return nil, status.WrapError(err, "create image file")
	}
	defer f.Close()
	// Don't leave a half-written image behind on failure.
	ok := false
	defer func() {
		if !ok {
			os.Remove(outputFile)
		}
	}()
	if err := f.Truncate(w.stats.ImageBytes); err != nil {
		return nil, status.WrapError(err, "truncate image file")
	}

	start = time.Now()
	if err := w.writeMetadata(f); err != nil {
		return nil, status.WrapError(err, "write metadata")
	}
	w.stats.MetadataDuration = time.Since(start)

	start = time.Now()
	if err := w.copyData(ctx, f); err != nil {
		return nil, status.WrapError(err, "copy file data")
	}
	w.stats.DataDuration = time.Since(start)
	ok = true
	return w.stats, nil
}

type writer struct {
	opts  Options
	stats *Stats
	open  func(n *node) (*os.File, error) // content source for regular files
	root  *node

	nodes    []*node // all nodes in inode order (index 0 = root)
	files    []*node // regular files with data (excluding hardlink duplicates)
	hardlink map[devIno]*node

	// Geometry.
	ngroups        uint32
	blocksCount    uint32
	inodesPerGroup uint32
	inodesCount    uint32
	itBlocksPerGrp uint32 // inode table blocks per group
	gdtBlocks      uint32
	backupGroups   []uint32 // groups with superblock+GDT backups

	// Per-group metadata locations (flex_bg: all in group 0).
	blockBitmapBlk []uint32
	inodeBitmapBlk []uint32
	inodeTableBlk  []uint32

	// Allocation state.
	dataStartBlock uint32 // first block after the metadata area
	nextBlock  uint32
	usedBlocks uint32
	blockBmp   []byte // one bit per block for the whole fs
	inodeBmp   []byte
	nextInode  uint32
	dirsCount  uint32
}

type devIno struct{ dev, ino uint64 }

// walk reads the source tree and assigns inode numbers in a deterministic
// order (root, lost+found, then depth-first in sorted name order).
func (w *writer) walk(dir string) (*node, error) {
	w.hardlink = map[devIno]*node{}
	st, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() {
		return nil, status.InvalidArgumentErrorf("%q is not a directory", dir)
	}
	root := w.newNode("", dir, st)
	if w.opts.Xattrs {
		xs, err := readXattrs(dir)
		if err != nil {
			return nil, status.WrapErrorf(err, "read xattrs of %q", dir)
		}
		root.xattrs = xs
		w.stats.Xattrs += len(xs)
	}
	// lost+found: keep e2fsck happy.
	lf := &node{name: "lost+found", mode: os.ModeDir | 0700, rawMode: syscall.S_IFDIR | 0700, mtime: w.opts.Now, parent: root}
	root.children = append(root.children, lf)
	if err := w.walkDir(root); err != nil {
		return nil, err
	}
	w.root = root
	return root, nil
}

// assignInodes numbers every node (root=2, lost+found=11, then depth-first in
// name order from 12) and fills w.nodes. Hardlink duplicates get the number of
// their original in a second pass, so the original is always numbered first
// regardless of tree order.
func (w *writer) assignInodes() {
	w.nodes = w.nodes[:0]
	next := uint32(lostFoundInode + 1)
	var visit func(n *node)
	visit = func(n *node) {
		switch {
		case n == w.root:
			n.ino = rootInode
		case n.parent == w.root && n.name == "lost+found":
			n.ino = lostFoundInode
		case n.hardlink != nil:
			return // numbered in the second pass
		default:
			n.ino = next
			next++
		}
		w.nodes = append(w.nodes, n)
		for _, ch := range n.children {
			visit(ch)
		}
	}
	visit(w.root)
	w.nextInode = next
	var fix func(n *node)
	fix = func(n *node) {
		if n.hardlink != nil {
			n.ino = n.hardlink.ino
		}
		for _, ch := range n.children {
			fix(ch)
		}
	}
	fix(w.root)
}

func (w *writer) newNode(name, path string, st os.FileInfo) *node {
	n := &node{name: name, path: path, mode: st.Mode(), size: st.Size(), mtime: st.ModTime()}
	if sys, ok := st.Sys().(*syscall.Stat_t); ok {
		n.rawMode = sys.Mode
		n.uid = sys.Uid
		n.gid = sys.Gid
		n.rdev = uint64(sys.Rdev)
	} else {
		n.rawMode = uint32(st.Mode().Perm())
	}
	return n
}

func (w *writer) walkDir(d *node) error {
	entries, err := os.ReadDir(d.path)
	if err != nil {
		return err
	}
	// os.ReadDir returns entries sorted by name.
	for _, e := range entries {
		st, err := e.Info() // lstat
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return err
		}
		n := w.newNode(e.Name(), filepath.Join(d.path, e.Name()), st)
		n.parent = d
		d.children = append(d.children, n)
		if w.opts.Xattrs {
			xs, err := readXattrs(n.path)
			if err != nil {
				return status.WrapErrorf(err, "read xattrs of %q", n.path)
			}
			n.xattrs = xs
			w.stats.Xattrs += len(xs)
		}
		if sys, ok := st.Sys().(*syscall.Stat_t); ok && !st.IsDir() && sys.Nlink > 1 {
			key := devIno{uint64(sys.Dev), sys.Ino}
			if orig, ok := w.hardlink[key]; ok {
				if orig.links >= 65000 {
					return status.InvalidArgumentErrorf("%q has more than 65000 hard links", orig.path)
				}
				n.hardlink = orig
				orig.links++
				w.stats.Hardlinks++
				continue
			}
			w.hardlink[key] = n
		}
		n.links = 1
		switch {
		case st.IsDir():
			w.stats.Dirs++
			if err := w.walkDir(n); err != nil {
				return err
			}
		case st.Mode()&os.ModeSymlink != 0:
			w.stats.Symlinks++
			target, err := os.Readlink(n.path)
			if err != nil {
				return err
			}
			n.target = target
			n.size = int64(len(target))
		case st.Mode().IsRegular():
			if n.size < 0 || n.size > maxFileBytes {
				return status.InvalidArgumentErrorf("%q: unsupported file size %d", n.path, n.size)
			}
			if sys, ok := st.Sys().(*syscall.Stat_t); ok && n.size >= 2*blockSize && sys.Blocks*512+blockSize < n.size {
				// Sparse file: only map the data ranges.
				ranges, err := dataRanges(n.path, n.size)
				if err != nil {
					return status.WrapErrorf(err, "find data ranges of %q", n.path)
				}
				n.ranges = ranges
			}
			w.stats.Files++
			w.stats.DataBytes += n.size
		}
	}
	return nil
}

// dataRanges finds the logical block ranges of a file that contain data,
// using SEEK_DATA/SEEK_HOLE. Falls back to a single dense range.
func dataRanges(path string, size int64) ([]blockRange, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []blockRange
	var off int64
	for off < size {
		dataStart, err := unix.Seek(int(f.Fd()), off, unix.SEEK_DATA)
		if err != nil {
			if errors.Is(err, unix.ENXIO) {
				break // no more data
			}
			if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) {
				return nil, nil // filesystem doesn't support it: treat as dense
			}
			return nil, err
		}
		holeStart, err := unix.Seek(int(f.Fd()), dataStart, unix.SEEK_HOLE)
		if err != nil {
			return nil, err
		}
		if holeStart > size {
			holeStart = size
		}
		// Round to whole blocks.
		sb := uint32(dataStart / blockSize)
		eb := uint32(ceilDiv(holeStart, blockSize))
		if len(out) > 0 && out[len(out)-1].start+out[len(out)-1].n >= sb {
			// Merge with previous (block rounding can make ranges touch).
			last := &out[len(out)-1]
			if eb > last.start+last.n {
				last.n = eb - last.start
			}
		} else if eb > sb {
			out = append(out, blockRange{start: sb, n: eb - sb})
		}
		off = holeStart
		if len(out) > maxExtentsList {
			return nil, status.InvalidArgumentErrorf("%q has too many data ranges", path)
		}
	}
	return out, nil
}

func sortChildren(d *node) {
	sort.Slice(d.children, func(i, j int) bool { return d.children[i].name < d.children[j].name })
}

// linkCount returns the number of hard links to a node's inode.
func linkCount(n *node) uint16 {
	if n.mode.IsDir() {
		c := 2
		for _, ch := range n.children {
			if ch.mode.IsDir() && ch.hardlink == nil {
				c++
			}
		}
		if c > 65000 {
			return 1 // dir_nlink: "many"
		}
		return uint16(c)
	}
	return uint16(max(n.links, 1))
}

// direntLen returns the on-disk size of a directory entry for a name.
func direntLen(name string) int {
	return (8 + len(name) + 3) &^ 3
}

// renderDir builds the directory data blocks for d.
func renderDir(d *node) []byte {
	type ent struct {
		ino  uint32
		name string
		ft   uint8
	}
	parentIno := d.ino
	if d.parent != nil {
		parentIno = d.parent.ino
	}
	ents := []ent{{d.ino, ".", ftDir}, {parentIno, "..", ftDir}}
	for _, ch := range d.children {
		ents = append(ents, ent{ch.ino, ch.name, fileType(ch.mode)})
	}
	var blocks []byte
	cur := make([]byte, 0, blockSize)
	flush := func() {
		if len(cur) == 0 {
			return
		}
		// Extend the last entry's rec_len to the end of the block.
		// Find the last entry start: we track it via lastOff.
		blocks = append(blocks, cur...)
		blocks = append(blocks, make([]byte, blockSize-len(cur))...)
		cur = cur[:0]
	}
	lastOff := -1
	for _, e := range ents {
		l := direntLen(e.name)
		if len(cur)+l > blockSize {
			// Fix up rec_len of the last entry to reach the block end.
			binary.LittleEndian.PutUint16(cur[lastOff+4:], uint16(blockSize-lastOff))
			flush()
			lastOff = -1
		}
		lastOff = len(cur)
		var hdr [8]byte
		binary.LittleEndian.PutUint32(hdr[0:], e.ino)
		binary.LittleEndian.PutUint16(hdr[4:], uint16(l))
		hdr[6] = uint8(len(e.name))
		hdr[7] = e.ft
		cur = append(cur, hdr[:]...)
		cur = append(cur, e.name...)
		for len(cur)%4 != 0 {
			cur = append(cur, 0)
		}
	}
	if lastOff >= 0 {
		binary.LittleEndian.PutUint16(cur[lastOff+4:], uint16(blockSize-lastOff))
	}
	flush()
	return blocks
}

func isBackupGroup(g uint32) bool {
	if g == 0 || g == 1 {
		return true
	}
	for _, p := range []uint32{3, 5, 7} {
		x := p
		for x < g {
			x *= p
		}
		if x == g {
			return true
		}
	}
	return false
}

// layout computes the filesystem geometry and allocates every block and inode.
func (w *writer) layout(root *node) error {
	// --- Per-node block requirements.
	var dataBlocks int64
	for _, n := range w.nodes {
		switch {
		case n.mode.IsDir():
			n.dirData = renderDir(n)
			n.nblocks = uint32(len(n.dirData) / blockSize)
		case n.mode.IsRegular():
			if n.ranges != nil {
				for _, r := range n.ranges {
					n.nblocks += r.n
				}
			} else {
				n.nblocks = uint32(ceilDiv(n.size, blockSize))
			}
		case n.mode&os.ModeSymlink != 0:
			if len(n.target) >= 60 {
				n.nblocks = uint32(ceilDiv(n.size, blockSize))
			}
		}
		dataBlocks += int64(n.nblocks)
		if len(n.xattrs) > 0 && !xattrsFitInInode(n.xattrs) {
			dataBlocks++
		}
		// Files with more than 4 extents need an extent-tree leaf block. Extents
		// are split at 32768 blocks and around reserved backup blocks, so
		// reserve a leaf for anything over 2 max-size extents; the general
		// slack below covers rare extra splits.
		if n.nblocks > 2*maxExtentLen {
			dataBlocks++
		}
	}

	// --- Geometry.
	inodesNeeded := int64(w.nextInode - 1)
	extraInodes := int64(w.opts.ExtraInodes)
	sizeBytes := max(w.opts.SizeBytes, MinImageSizeBytes)
	// Metadata estimate: GDT + bitmaps + inode tables. Iterate once since inode
	// count depends on size.
	if w.opts.SizeBytes < 0 || w.opts.SlackBytes < 0 || w.opts.ExtraInodes < 0 {
		return status.InvalidArgumentError("negative size option")
	}
	var ngroups, ipg, itb, gdtBlocks int64
	converged := false
	for iter := 0; iter < 16 && !converged; iter++ {
		ngroups = ceilDiv(sizeBytes, int64(blocksPerGroup)*blockSize)
		if w.opts.ExtraInodes == 0 {
			// 1 inode per 16 KiB of image, like mke2fs -N 0. This must be
			// recomputed whenever the size grows, otherwise an action could
			// hit ENOSPC on inodes with gigabytes of free blocks.
			extraInodes = ngroups * blocksPerGroup * blockSize / 16384
		}
		totalInodes := inodesNeeded + extraInodes
		// A group holds at most 32768 inodes (one bitmap block); grow the
		// image if the inodes alone need more groups.
		if minGroups := ceilDiv(totalInodes, blocksPerGroup); minGroups > ngroups {
			sizeBytes = minGroups * blocksPerGroup * blockSize
			continue
		}
		ipg = ceilDiv(totalInodes, ngroups)
		ipg = (ipg + 15) &^ 15
		if ipg > blocksPerGroup {
			ipg = blocksPerGroup
		}
		itb = ceilDiv(ipg*inodeSize, blockSize)
		ipg = itb * blockSize / inodeSize // fill whole table blocks
		if ipg > blocksPerGroup {
			ipg = blocksPerGroup
			itb = ipg * inodeSize / blockSize
		}
		gdtBlocks = ceilDiv(ngroups*32, blockSize)
		nBackup := int64(0)
		for g := int64(0); g < ngroups; g++ {
			if isBackupGroup(uint32(g)) {
				nBackup++
			}
		}
		meta := nBackup*(1+gdtBlocks) + 2*ngroups + ngroups*itb
		need := (meta+dataBlocks+256)*blockSize + w.opts.SlackBytes + int64(float64(dataBlocks*blockSize)*w.opts.SlackFraction)
		if need > sizeBytes {
			sizeBytes = need
			continue
		}
		converged = true
	}
	if !converged {
		return status.InternalError("image sizing did not converge")
	}
	w.ngroups = uint32(ngroups)
	w.blocksCount = uint32(ngroups * blocksPerGroup)
	w.inodesPerGroup = uint32(ipg)
	w.inodesCount = uint32(ngroups * ipg)
	w.itBlocksPerGrp = uint32(itb)
	w.gdtBlocks = uint32(gdtBlocks)
	w.stats.ImageBytes = int64(w.blocksCount) * blockSize
	w.stats.BlockGroups = int(w.ngroups)
	w.stats.Inodes = w.inodesCount
	if w.inodesCount < uint32(inodesNeeded) {
		return status.InternalErrorf("inode count %d < needed %d", w.inodesCount, inodesNeeded)
	}
	w.blockBmp = make([]byte, w.blocksCount/8)
	w.inodeBmp = make([]byte, w.inodesCount/8+1)

	// --- Reserve superblock/GDT backups.
	for g := uint32(0); g < w.ngroups; g++ {
		if isBackupGroup(g) {
			w.backupGroups = append(w.backupGroups, g)
			base := g * blocksPerGroup
			for b := base; b < base+1+w.gdtBlocks; b++ {
				w.markBlock(b)
			}
		}
	}
	// --- Bitmaps and inode tables, packed at the front (flex_bg lets them
	// live anywhere). Skip blocks already reserved for backup superblocks,
	// which matters once the metadata area grows past group 0 (~8 GiB
	// images at default inode density).
	w.nextBlock = 1 + w.gdtBlocks
	takeBlocks := func(n uint32) (uint32, error) {
		// Find n contiguous free blocks starting at nextBlock.
		for {
			for w.nextBlock < w.blocksCount && w.isBlockUsed(w.nextBlock) {
				w.nextBlock++
			}
			start := w.nextBlock
			ok := true
			for b := start; b < start+n; b++ {
				if b >= w.blocksCount {
					return 0, status.InternalErrorf("metadata does not fit in %d blocks", w.blocksCount)
				}
				if w.isBlockUsed(b) {
					ok = false
					w.nextBlock = b + 1
					break
				}
			}
			if !ok {
				continue
			}
			for b := start; b < start+n; b++ {
				w.markBlock(b)
			}
			w.nextBlock = start + n
			return start, nil
		}
	}
	w.blockBitmapBlk = make([]uint32, w.ngroups)
	w.inodeBitmapBlk = make([]uint32, w.ngroups)
	w.inodeTableBlk = make([]uint32, w.ngroups)
	var err error
	for g := uint32(0); g < w.ngroups; g++ {
		if w.blockBitmapBlk[g], err = takeBlocks(1); err != nil {
			return err
		}
	}
	for g := uint32(0); g < w.ngroups; g++ {
		if w.inodeBitmapBlk[g], err = takeBlocks(1); err != nil {
			return err
		}
	}
	for g := uint32(0); g < w.ngroups; g++ {
		if w.inodeTableBlk[g], err = takeBlocks(w.itBlocksPerGrp); err != nil {
			return err
		}
	}
	w.dataStartBlock = w.nextBlock

	// --- Inodes 1..10 are reserved.
	for i := uint32(1); i <= 10; i++ {
		w.markInode(i)
	}
	for _, n := range w.nodes {
		w.markInode(n.ino)
		if n.mode.IsDir() {
			w.dirsCount++
		}
	}

	// --- Allocate data: directories and symlinks first (metadata locality),
	// then files in tree order.
	for _, n := range w.nodes {
		if n.nblocks > 0 && !n.mode.IsRegular() {
			if err := w.allocExtents(n); err != nil {
				return err
			}
		}
	}
	for _, n := range w.nodes {
		if n.mode.IsRegular() {
			if n.nblocks > 0 {
				if err := w.allocExtents(n); err != nil {
					return err
				}
			}
			w.files = append(w.files, n)
		}
	}
	// External xattr blocks.
	for _, n := range w.nodes {
		if len(n.xattrs) > 0 && !xattrsFitInInode(n.xattrs) {
			for w.nextBlock < w.blocksCount && w.isBlockUsed(w.nextBlock) {
				w.nextBlock++
			}
			if w.nextBlock >= w.blocksCount {
				return status.ResourceExhaustedErrorf("image too small: out of blocks")
			}
			n.xattrBlk = w.nextBlock
			w.markBlock(n.xattrBlk)
			w.nextBlock++
		}
	}
	return nil
}

func (w *writer) markBlock(b uint32) {
	w.blockBmp[b/8] |= 1 << (b % 8)
	w.usedBlocks++
}

func (w *writer) markInode(i uint32) {
	i-- // inode numbers are 1-based
	w.inodeBmp[i/8] |= 1 << (i % 8)
}

func (w *writer) isBlockUsed(b uint32) bool {
	return w.blockBmp[b/8]&(1<<(b%8)) != 0
}

// allocExtents allocates n.nblocks contiguous-as-possible blocks, splitting
// around reserved backup blocks and the 32768-block extent limit.
func (w *writer) allocExtents(n *node) error {
	ranges := n.ranges
	if ranges == nil {
		ranges = []blockRange{{start: 0, n: n.nblocks}}
	}
	for _, r := range ranges {
		if err := w.allocRange(n, r); err != nil {
			return err
		}
	}
	if len(n.extents) > extentsInInode {
		if len(n.extents) > extentsPerLeaf {
			return status.UnimplementedErrorf("file %q needs %d extents; more than %d is unsupported", n.path, len(n.extents), extentsPerLeaf)
		}
		// Allocate one leaf block for the extent tree.
		for w.nextBlock < w.blocksCount && w.isBlockUsed(w.nextBlock) {
			w.nextBlock++
		}
		if w.nextBlock >= w.blocksCount {
			return status.ResourceExhaustedErrorf("image too small: out of blocks")
		}
		n.leaf = w.nextBlock
		w.markBlock(n.leaf)
		w.nextBlock++
	}
	return nil
}

// allocRange allocates physical blocks for one logical range of n.
func (w *writer) allocRange(n *node, r blockRange) error {
	remaining := r.n
	logical := r.start
	for remaining > 0 {
		// Skip used (reserved) blocks.
		for w.nextBlock < w.blocksCount && w.isBlockUsed(w.nextBlock) {
			w.nextBlock++
		}
		if w.nextBlock >= w.blocksCount {
			return status.ResourceExhaustedErrorf("image too small: out of blocks")
		}
		start := w.nextBlock
		// Extend the run until a reserved block, the extent limit, or done.
		l := uint32(0)
		for l < remaining && l < maxExtentLen && start+l < w.blocksCount && !w.isBlockUsed(start+l) {
			l++
		}
		for b := start; b < start+l; b++ {
			w.markBlock(b)
		}
		w.nextBlock = start + l
		n.extents = append(n.extents, extentRg{logical: logical, start: start, len: l})
		logical += l
		remaining -= l
	}
	return nil
}

// ---------------------------------------------------------------------------
// Metadata encoding
// ---------------------------------------------------------------------------

func (w *writer) superblock() []byte {
	sb := make([]byte, 1024)
	le := binary.LittleEndian
	freeBlocks := w.blocksCount - w.usedBlocks
	usedInodes := w.nextInode - 1 // inodes are allocated contiguously from 1
	now := uint32(w.opts.Now.Unix())
	le.PutUint32(sb[0x00:], w.inodesCount)
	le.PutUint32(sb[0x04:], w.blocksCount)
	le.PutUint32(sb[0x08:], 0) // reserved blocks
	le.PutUint32(sb[0x0C:], freeBlocks)
	le.PutUint32(sb[0x10:], w.inodesCount-usedInodes)
	le.PutUint32(sb[0x14:], 0) // first data block
	le.PutUint32(sb[0x18:], 2) // log block size: 1024 << 2
	le.PutUint32(sb[0x1C:], 2) // log cluster size
	le.PutUint32(sb[0x20:], blocksPerGroup)
	le.PutUint32(sb[0x24:], blocksPerGroup)
	le.PutUint32(sb[0x28:], w.inodesPerGroup)
	le.PutUint32(sb[0x2C:], 0)   // mtime
	le.PutUint32(sb[0x30:], now) // wtime
	le.PutUint16(sb[0x34:], 0)   // mnt count
	le.PutUint16(sb[0x36:], 0xFFFF)
	le.PutUint16(sb[0x38:], superMagic)
	le.PutUint16(sb[0x3A:], 1) // state: clean
	le.PutUint16(sb[0x3C:], 1) // errors: continue
	le.PutUint16(sb[0x3E:], 0)
	le.PutUint32(sb[0x40:], now) // lastcheck
	le.PutUint32(sb[0x44:], 0)
	le.PutUint32(sb[0x48:], 0) // creator os: linux
	le.PutUint32(sb[0x4C:], 1) // rev level: dynamic
	le.PutUint16(sb[0x50:], 0)
	le.PutUint16(sb[0x52:], 0)
	le.PutUint32(sb[0x54:], firstInode)
	le.PutUint16(sb[0x58:], inodeSize)
	le.PutUint16(sb[0x5A:], 0)
	compat := uint32(0)
	if w.stats.Xattrs > 0 {
		compat |= featureCompatExtAttr
	}
	le.PutUint32(sb[0x5C:], compat)
	le.PutUint32(sb[0x60:], featureIncompatFiletype|featureIncompatExtents|featureIncompatFlexBG)
	le.PutUint32(sb[0x64:], featureROSparseSuper|featureROLargeFile|featureROHugeFile|featureRODirNlink|featureROExtraIsize)
	// uuid: derive something non-zero but deterministic-looking from time.
	uuid := sb[0x68:0x78]
	le.PutUint64(uuid[0:], uint64(w.opts.Now.UnixNano()))
	le.PutUint64(uuid[8:], uint64(w.blocksCount)<<32|uint64(w.inodesCount))
	uuid[6] = (uuid[6] & 0x0F) | 0x40
	uuid[8] = (uuid[8] & 0x3F) | 0x80
	// hash seed / version (unused without dir_index, but keep sane).
	copy(sb[0xEC:], uuid)
	sb[0xFC] = 1 // half_md4
	le.PutUint32(sb[0x100:], 0)  // default mount opts
	le.PutUint32(sb[0x108:], now) // mkfs time
	le.PutUint16(sb[0x15C:], 32) // min extra isize
	le.PutUint16(sb[0x15E:], 32) // want extra isize
	le.PutUint32(sb[0x160:], 1)  // flags: signed dir hash
	sb[0x174] = 4                // log groups per flex (16)
	return sb
}

func (w *writer) groupDescriptors() []byte {
	gdt := make([]byte, w.gdtBlocks*blockSize)
	le := binary.LittleEndian
	usedInodes := w.nextInode - 1
	dirsPerGroup := make([]uint32, w.ngroups)
	for _, n := range w.nodes {
		if n.mode.IsDir() {
			dirsPerGroup[(n.ino-1)/w.inodesPerGroup]++
		}
	}
	for g := uint32(0); g < w.ngroups; g++ {
		d := gdt[g*32:]
		le.PutUint32(d[0x0:], w.blockBitmapBlk[g])
		le.PutUint32(d[0x4:], w.inodeBitmapBlk[g])
		le.PutUint32(d[0x8:], w.inodeTableBlk[g])
		// Free blocks in this group.
		free := uint32(0)
		base := g * blocksPerGroup
		for b := base; b < base+blocksPerGroup; b++ {
			if !w.isBlockUsed(b) {
				free++
			}
		}
		le.PutUint16(d[0xC:], uint16(free))
		// Free inodes / used dirs in this group.
		firstIno := g*w.inodesPerGroup + 1
		lastIno := firstIno + w.inodesPerGroup - 1
		usedHere := uint32(0)
		if usedInodes >= firstIno {
			usedHere = min(usedInodes, lastIno) - firstIno + 1
		}
		le.PutUint16(d[0xE:], uint16(w.inodesPerGroup-usedHere))
		le.PutUint16(d[0x10:], uint16(dirsPerGroup[g]))
		le.PutUint16(d[0x12:], 0) // no uninit_bg feature => no flags
		le.PutUint16(d[0x1C:], uint16(w.inodesPerGroup-usedHere)) // itable unused
	}
	return gdt
}

func (w *writer) encodeInode(n *node) []byte {
	b := make([]byte, inodeSize)
	le := binary.LittleEndian
	mode := uint16(n.rawMode & 0xFFFF)
	if n.mode.IsDir() {
		mode = syscall.S_IFDIR | uint16(n.rawMode&0o7777)
	}
	le.PutUint16(b[0x0:], mode)
	le.PutUint16(b[0x2:], uint16(n.uid))
	size := n.size
	if n.mode.IsDir() {
		size = int64(len(n.dirData))
	}
	le.PutUint32(b[0x4:], uint32(size))
	t := uint32(n.mtime.Unix())
	le.PutUint32(b[0x8:], t)  // atime
	le.PutUint32(b[0xC:], t)  // ctime
	le.PutUint32(b[0x10:], t) // mtime
	le.PutUint16(b[0x18:], uint16(n.gid))
	le.PutUint16(b[0x1A:], linkCount(n))
	blocks := uint64(n.nblocks) * (blockSize / 512)
	if n.leaf != 0 {
		blocks += blockSize / 512
	}
	le.PutUint32(b[0x1C:], uint32(blocks))
	le.PutUint16(b[0x74:], uint16(blocks>>32))
	le.PutUint32(b[0x6C:], uint32(size>>32))
	le.PutUint16(b[0x78:], uint16(n.uid>>16))
	le.PutUint16(b[0x7A:], uint16(n.gid>>16))
	le.PutUint16(b[0x80:], 32) // extra isize
	le.PutUint32(b[0x90:], t)  // crtime
	if len(n.xattrs) > 0 {
		if n.xattrBlk != 0 {
			le.PutUint32(b[0x68:], n.xattrBlk) // i_file_acl_lo
			blocks += blockSize / 512
			le.PutUint32(b[0x1C:], uint32(blocks))
			le.PutUint16(b[0x74:], uint16(blocks>>32))
		} else {
			encodeInodeXattrs(b, n.xattrs)
		}
	}
	iblock := b[0x28:0x64]
	switch {
	case n.mode&os.ModeSymlink != 0 && n.nblocks == 0:
		// Fast symlink: target stored inline.
		copy(iblock, n.target)
	case n.mode&(os.ModeDevice|os.ModeCharDevice) != 0:
		// New-style device number encoding in i_block[1].
		major := unix.Major(n.rdev)
		minor := unix.Minor(n.rdev)
		le.PutUint32(iblock[4:], (minor&0xff)|(major<<8)|((minor&^0xff)<<12))
	case n.mode&(os.ModeNamedPipe|os.ModeSocket) != 0:
		// no data
	default:
		le.PutUint32(b[0x20:], inodeFlagExtents)
		w.encodeExtentTree(n, iblock)
	}
	return b
}

func putExtent(dst []byte, e extentRg) {
	le := binary.LittleEndian
	le.PutUint32(dst[0:], e.logical)
	le.PutUint16(dst[4:], uint16(e.len))
	le.PutUint16(dst[6:], 0) // start hi
	le.PutUint32(dst[8:], e.start)
}

func (w *writer) encodeExtentTree(n *node, iblock []byte) {
	le := binary.LittleEndian
	le.PutUint16(iblock[0:], extentMagic)
	le.PutUint16(iblock[4:], extentsInInode) // eh_max
	if n.leaf == 0 {
		le.PutUint16(iblock[2:], uint16(len(n.extents)))
		le.PutUint16(iblock[6:], 0) // depth
		for i, e := range n.extents {
			putExtent(iblock[12+i*extentSize:], e)
		}
		return
	}
	// Depth 1: one index entry pointing at the leaf block.
	le.PutUint16(iblock[2:], 1)
	le.PutUint16(iblock[6:], 1)
	le.PutUint32(iblock[12:], 0)      // ei_block
	le.PutUint32(iblock[16:], n.leaf) // ei_leaf_lo
	le.PutUint16(iblock[20:], 0)      // ei_leaf_hi
}

func (w *writer) encodeLeaf(n *node) []byte {
	b := make([]byte, blockSize)
	le := binary.LittleEndian
	le.PutUint16(b[0:], extentMagic)
	le.PutUint16(b[2:], uint16(len(n.extents)))
	le.PutUint16(b[4:], extentsPerLeaf)
	le.PutUint16(b[6:], 0)
	for i, e := range n.extents {
		putExtent(b[12+i*extentSize:], e)
	}
	return b
}

// writeMetadata writes superblocks, GDTs, bitmaps, inode tables, directory
// blocks, extent leaves and slow symlink targets.
func (w *writer) writeMetadata(f io.WriterAt) error {
	sb := w.superblock()
	gdt := w.groupDescriptors()
	// Primary superblock lives at byte 1024; backups at the first block of
	// each backup group.
	for _, g := range w.backupGroups {
		off := int64(g) * blocksPerGroup * blockSize
		if g == 0 {
			if _, err := f.WriteAt(sb, 1024); err != nil {
				return err
			}
		} else {
			bsb := append([]byte(nil), sb...)
			binary.LittleEndian.PutUint16(bsb[0x5A:], uint16(g)) // block group nr
			if _, err := f.WriteAt(bsb, off); err != nil {
				return err
			}
		}
		if _, err := f.WriteAt(gdt, off+blockSize); err != nil {
			return err
		}
	}
	// Block bitmaps: one block per group (contiguous unless a backup
	// superblock interrupted the run, so write in contiguous runs).
	if err := writeRuns(f, w.blockBitmapBlk, func(g uint32) []byte {
		return w.blockBmp[g*blockSize : (g+1)*blockSize]
	}); err != nil {
		return err
	}
	// Inode bitmaps: one block per group, bits beyond inodesPerGroup set.
	nbytes := w.inodesPerGroup / 8
	if err := writeRuns(f, w.inodeBitmapBlk, func(g uint32) []byte {
		blk := make([]byte, blockSize)
		for i := range blk {
			blk[i] = 0xFF
		}
		copy(blk[:nbytes], w.inodeBmp[g*nbytes:(g+1)*nbytes])
		return blk
	}); err != nil {
		return err
	}
	// Inode tables: only the used prefix of each group's table.
	// Group inode tables are contiguous in group 0, so gather all used inodes
	// into per-group buffers.
	type grpBuf struct {
		buf []byte
		max uint32 // highest local index written
	}
	bufs := make([]grpBuf, w.ngroups)
	put := func(ino uint32, data []byte) {
		g := (ino - 1) / w.inodesPerGroup
		idx := (ino - 1) % w.inodesPerGroup
		if bufs[g].buf == nil {
			bufs[g].buf = make([]byte, w.inodesPerGroup*inodeSize)
		}
		copy(bufs[g].buf[idx*inodeSize:], data)
		if idx+1 > bufs[g].max {
			bufs[g].max = idx + 1
		}
	}
	// Reserved inodes 1..10: zeroed but present (bitmap marks them used).
	put(10, make([]byte, inodeSize))
	for _, n := range w.nodes {
		put(n.ino, w.encodeInode(n))
	}
	for g := range bufs {
		if bufs[g].buf == nil {
			continue
		}
		end := (int64(bufs[g].max)*inodeSize + blockSize - 1) &^ (blockSize - 1)
		if _, err := f.WriteAt(bufs[g].buf[:end], int64(w.inodeTableBlk[g])*blockSize); err != nil {
			return err
		}
	}
	// Directory blocks, extent leaves, slow symlinks, xattr blocks.
	for _, n := range w.nodes {
		if n.xattrBlk != 0 {
			blk, err := encodeXattrBlock(n.xattrs)
			if err != nil {
				return err
			}
			if _, err := f.WriteAt(blk, int64(n.xattrBlk)*blockSize); err != nil {
				return err
			}
		}
		if n.leaf != 0 {
			if _, err := f.WriteAt(w.encodeLeaf(n), int64(n.leaf)*blockSize); err != nil {
				return err
			}
		}
		switch {
		case n.mode.IsDir():
			if err := writeExtents(f, n.extents, n.dirData); err != nil {
				return err
			}
		case n.mode&os.ModeSymlink != 0 && n.nblocks > 0:
			data := make([]byte, int(n.nblocks)*blockSize)
			copy(data, n.target)
			if err := writeExtents(f, n.extents, data); err != nil {
				return err
			}
		}
	}
	return nil
}

// writeRuns writes one block per group at the given block numbers, coalescing
// adjacent blocks into single writes.
func writeRuns(f io.WriterAt, blocks []uint32, data func(g uint32) []byte) error {
	var buf []byte
	var start uint32
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		_, err := f.WriteAt(buf, int64(start)*blockSize)
		buf = buf[:0]
		return err
	}
	for g := uint32(0); g < uint32(len(blocks)); g++ {
		if len(buf) > 0 && blocks[g] != start+uint32(len(buf)/blockSize) {
			if err := flush(); err != nil {
				return err
			}
		}
		if len(buf) == 0 {
			start = blocks[g]
		}
		buf = append(buf, data(g)...)
	}
	return flush()
}

// writeExtents writes data to the physical blocks described by extents.
func writeExtents(f io.WriterAt, extents []extentRg, data []byte) error {
	for _, e := range extents {
		off := int64(e.logical) * blockSize
		end := min(int64(len(data)), off+int64(e.len)*blockSize)
		if _, err := f.WriteAt(data[off:end], int64(e.start)*blockSize); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Data copy
// ---------------------------------------------------------------------------

// reflinkState remembers whether FICLONERANGE works between the source
// files and the image, so we stop trying after the first hard failure.
type reflinkState struct {
	mu       sync.Mutex
	disabled bool
	bytes    int64
}

func (r *reflinkState) add(n int64) {
	r.mu.Lock()
	r.bytes += n
	r.mu.Unlock()
}

func (r *reflinkState) enabled() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.disabled
}

func (r *reflinkState) disable() {
	r.mu.Lock()
	r.disabled = true
	r.mu.Unlock()
}

func (w *writer) copyData(ctx context.Context, f *os.File) error {
	// Sort files by size descending so big copies start first (better tail).
	files := make([]*node, 0, len(w.files))
	for _, n := range w.files {
		if n.size > 0 {
			files = append(files, n)
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].size > files[j].size })
	ch := make(chan *node)
	eg, ctx := errgroup.WithContext(ctx)
	dstFD := int(f.Fd())
	var bufPool sync.Pool
	reflink := &reflinkState{disabled: !w.opts.Reflink}
	var mapping []byte
	if w.opts.CopyMode != "cfr" && len(files) > 0 {
		// Writing through a MAP_SHARED mapping means a failed page allocation
		// (ENOSPC on the host filesystem) would surface as SIGBUS and kill the
		// process. Reserve the data region up front so that ENOSPC is reported
		// here instead. Data blocks are allocated in one contiguous run after
		// the metadata, so this is a single fallocate.
		dataStart := int64(w.dataStartBlock) * blockSize
		dataLen := int64(w.nextBlock)*blockSize - dataStart
		if dataLen > 0 {
			if err := unix.Fallocate(dstFD, unix.FALLOC_FL_KEEP_SIZE, dataStart, dataLen); err != nil {
				if errors.Is(err, unix.ENOSPC) {
					return status.ResourceExhaustedErrorf("fallocate %d bytes for workspace image: %s", dataLen, err)
				}
				// Filesystem doesn't support fallocate: fall back to
				// copy_file_range which reports errors normally.
				mapping = nil
			} else {
				m, err := unix.Mmap(dstFD, 0, int(w.stats.ImageBytes), unix.PROT_WRITE|unix.PROT_READ, unix.MAP_SHARED)
				if err != nil {
					return status.WrapError(err, "mmap image")
				}
				mapping = m
				defer unix.Munmap(m)
			}
		}
	}
	for i := 0; i < w.opts.Concurrency; i++ {
		eg.Go(func() error {
			for n := range ch {
				src, err := w.open(n)
				if err != nil {
					return status.WrapErrorf(err, "open %q", n.name)
				}
				if mapping != nil {
					err = copyFileMmapSafe(ctx, src, n, mapping)
				} else {
					err = copyFile(ctx, src, n, dstFD, &bufPool, reflink)
				}
				src.Close()
				if err != nil {
					return status.WrapErrorf(err, "copy %q", n.path)
				}
			}
			return nil
		})
	}
	eg.Go(func() error {
		defer close(ch)
		for _, n := range files {
			select {
			case ch <- n:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	err := eg.Wait()
	w.stats.ReflinkedBytes = reflink.bytes
	return err
}

// copyFile copies n's contents into the image at its allocated extents.
//
// On filesystems with reflink support (XFS with reflink=1, btrfs) the
// block-aligned prefix of each file is cloned with FICLONERANGE, which shares
// the filecache's on-disk extents with the image instead of copying bytes.
// Otherwise (or for the unaligned tail) copy_file_range is used, falling back
// to read/write.
func copyFile(ctx context.Context, src *os.File, n *node, dstFD int, pool *sync.Pool, reflink *reflinkState) error {
	srcFD := int(src.Fd())
	for _, e := range n.extents {
		srcOff := int64(e.logical) * blockSize
		end := min(n.size, int64(e.logical+e.len)*blockSize)
		dstOff := int64(e.start) * blockSize
		if reflink.enabled() {
			// Clone the block-aligned part of this extent; the tail (if any)
			// is copied below.
			alignedEnd := end &^ (blockSize - 1)
			if alignedEnd > srcOff {
				err := unix.IoctlFileCloneRange(dstFD, &unix.FileCloneRange{
					Src_fd: int64(srcFD), Src_offset: uint64(srcOff), Src_length: uint64(alignedEnd - srcOff), Dest_offset: uint64(dstOff),
				})
				if err == nil {
					reflink.add(alignedEnd - srcOff)
					dstOff += alignedEnd - srcOff
					srcOff = alignedEnd
				} else if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.EXDEV) || errors.Is(err, unix.ENOTTY) || errors.Is(err, unix.EINVAL) {
					reflink.disable()
				}
			}
		}
		for srcOff < end {
			if err := ctx.Err(); err != nil {
				return err
			}
			nn, err := unix.CopyFileRange(srcFD, &srcOff, dstFD, &dstOff, int(min(end-srcOff, 1<<30)), 0)
			if err != nil {
				if errors.Is(err, unix.EXDEV) || errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.EINVAL) {
					// Fall back to a buffered copy for the rest of this extent.
					if err := copyRangeBuffered(src, srcOff, end, dstFD, dstOff, pool); err != nil {
						return err
					}
					break
				}
				return err
			}
			if nn == 0 {
				return io.ErrUnexpectedEOF
			}
		}
	}
	return nil
}

// copyFileMmapSafe runs copyFileMmap with memory faults on the mapping turned
// into recoverable panics, so an I/O error on the image file (which would
// otherwise SIGBUS and kill the whole executor) surfaces as an error.
func copyFileMmapSafe(ctx context.Context, src *os.File, n *node, mapping []byte) (err error) {
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	defer func() {
		if r := recover(); r != nil {
			err = status.UnavailableErrorf("memory fault while writing workspace image (I/O error on the host filesystem?): %v", r)
		}
	}()
	return copyFileMmap(ctx, src, n, mapping)
}

// copyFileMmap preads the source file directly into the MAP_SHARED image
// mapping, so the kernel copies from the source page cache straight into the
// image's page cache pages.
func copyFileMmap(ctx context.Context, src *os.File, n *node, mapping []byte) error {
	for _, e := range n.extents {
		srcOff := int64(e.logical) * blockSize
		end := min(n.size, int64(e.logical+e.len)*blockSize)
		dstOff := int64(e.start) * blockSize
		for srcOff < end {
			if err := ctx.Err(); err != nil {
				return err
			}
			chunk := min(end-srcOff, int64(64<<20))
			nr, err := src.ReadAt(mapping[dstOff:dstOff+chunk], srcOff)
			if err != nil && !(errors.Is(err, io.EOF) && nr > 0) {
				return err
			}
			if nr == 0 {
				return io.ErrUnexpectedEOF
			}
			srcOff += int64(nr)
			dstOff += int64(nr)
		}
	}
	return nil
}

// copyRangeBuffered copies [srcOff, end) of src to dstOff with read/write.
func copyRangeBuffered(src *os.File, srcOff, end int64, dstFD int, dstOff int64, pool *sync.Pool) error {
	bufp, _ := pool.Get().(*[]byte)
	if bufp == nil {
		b := make([]byte, 1<<20)
		bufp = &b
	}
	defer pool.Put(bufp)
	buf := *bufp
	for srcOff < end {
		chunk := min(end-srcOff, int64(len(buf)))
		nr, err := src.ReadAt(buf[:chunk], srcOff)
		if err != nil && !(errors.Is(err, io.EOF) && nr > 0) {
			return err
		}
		if nr == 0 {
			return io.ErrUnexpectedEOF
		}
		for w := 0; w < nr; {
			nw, err := unix.Pwrite(dstFD, buf[w:nr], dstOff+int64(w))
			if err != nil {
				return err
			}
			if nw == 0 {
				return io.ErrShortWrite
			}
			w += nw
		}
		srcOff += int64(nr)
		dstOff += int64(nr)
	}
	return nil
}

// String renders stats for logs.
func (s *Stats) String() string {
	return fmt.Sprintf("files=%d dirs=%d symlinks=%d hardlinks=%d xattrs=%d data=%dMB reflinked=%dMB image=%dMB groups=%d inodes=%d walk=%s layout=%s meta=%s data_copy=%s",
		s.Files, s.Dirs, s.Symlinks, s.Hardlinks, s.Xattrs, s.DataBytes>>20, s.ReflinkedBytes>>20, s.ImageBytes>>20, s.BlockGroups, s.Inodes,
		s.WalkDuration.Round(time.Millisecond), s.LayoutDuration.Round(time.Millisecond), s.MetadataDuration.Round(time.Millisecond), s.DataDuration.Round(time.Millisecond))
}
