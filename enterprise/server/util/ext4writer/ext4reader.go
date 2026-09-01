package ext4writer

// A small read-only ext4 parser used to extract action outputs from the
// workspace image without shelling out to debugfs.
//
// Why not debugfs: e2fsprogs fsync()s the image file right after opening it,
// which forces every dirty page of the (throwaway) workspace image out to disk
// — hundreds of milliseconds for a few hundred MB of inputs, and needless disk
// write traffic. It also costs a process spawn per action.
//
// Supported: 4 KiB (or other) block sizes, 32/64-byte group descriptors,
// extent-mapped files of any depth, uninitialized extents (read as zeros),
// holes (preserved as sparse), linear and htree directories (read linearly),
// fast and slow symlinks, hard links (extracted as separate files).
// Not supported: inline_data, encryption, block-mapped (non-extent) files.

import (
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	incompatInlineData = 0x8000
	incompatEncrypt    = 0x10000
	incompat64Bit      = 0x0080
	inodeFlagInline    = 0x10000000
)

type ext4Image struct {
	f              io.ReaderAt
	closer         io.Closer
	dirCache       map[uint32][]dirent // parsed directories, by inode
	linked         map[uint32]string   // first extracted path per inode (for hardlinks)
	size           int64               // image file size; every read is bounds-checked against it
	blockSize      int64
	inodeSize      int64
	inodesPerGroup uint32
	ngroups        uint32
	firstDataBlock uint32
	descSize       int64
	gdtOffset      int64
	incompat       uint32
}

// Limits that protect the host from a malicious or corrupt guest-written image.
const (
	maxDirBytes    = 1 << 30 // 1 GiB of directory blocks
	maxExtentsList = 1 << 20 // extents per file
	maxNameLen     = 255
)

// readAt reads exactly len(dst) bytes at off, refusing reads outside the image.
func (img *ext4Image) readAt(dst []byte, off int64) error {
	if off < 0 || off+int64(len(dst)) > img.size {
		return status.InvalidArgumentErrorf("read [%d,+%d) outside image of %d bytes", off, len(dst), img.size)
	}
	_, err := img.f.ReadAt(dst, off)
	return err
}

func openImage(path string) (*ext4Image, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	img, err := openImageReader(f, st.Size())
	if err != nil {
		f.Close()
		return nil, err
	}
	img.closer = f
	return img, nil
}

// openImageReader parses the superblock of an ext4 image served by any
// ReaderAt (a file, or a virtual block device).
func openImageReader(r io.ReaderAt, size int64) (*ext4Image, error) {
	img := &ext4Image{f: r, size: size, dirCache: map[uint32][]dirent{}, linked: map[uint32]string{}}
	sb := make([]byte, 1024)
	if err := img.readAt(sb, 1024); err != nil {
		return nil, status.WrapError(err, "read superblock")
	}
	le := binary.LittleEndian
	bad := func(format string, args ...any) (*ext4Image, error) {
		return nil, status.InvalidArgumentErrorf("invalid ext4 image: "+format, args...)
	}
	if le.Uint16(sb[0x38:]) != superMagic {
		return bad("bad magic")
	}
	logBlockSize := le.Uint32(sb[0x18:])
	if logBlockSize > 6 {
		return bad("log block size %d", logBlockSize)
	}
	img.blockSize = 1024 << logBlockSize
	img.inodesPerGroup = le.Uint32(sb[0x28:])
	blocksPerGrp := le.Uint32(sb[0x20:])
	blocksCount := uint64(le.Uint32(sb[0x04:]))
	img.firstDataBlock = le.Uint32(sb[0x14:])
	img.incompat = le.Uint32(sb[0x60:])
	if img.incompat&incompat64Bit != 0 {
		blocksCount |= uint64(le.Uint32(sb[0x150:])) << 32
	}
	if img.inodesPerGroup == 0 || int64(img.inodesPerGroup) > img.blockSize*8 || blocksPerGrp == 0 || int64(blocksPerGrp) > img.blockSize*8 {
		return bad("inodes/blocks per group %d/%d", img.inodesPerGroup, blocksPerGrp)
	}
	if blocksCount == 0 || int64(blocksCount)*img.blockSize > img.size+img.blockSize {
		return bad("block count %d exceeds image size %d", blocksCount, img.size)
	}
	img.ngroups = uint32((blocksCount - uint64(img.firstDataBlock) + uint64(blocksPerGrp) - 1) / uint64(blocksPerGrp))
	img.inodeSize = 128
	if le.Uint32(sb[0x4C:]) >= 1 {
		img.inodeSize = int64(le.Uint16(sb[0x58:]))
	}
	if img.inodeSize < 128 || img.inodeSize > img.blockSize || img.inodeSize&(img.inodeSize-1) != 0 {
		return bad("inode size %d", img.inodeSize)
	}
	img.descSize = 32
	if img.incompat&incompat64Bit != 0 {
		img.descSize = int64(le.Uint16(sb[0xFE:]))
		if img.descSize < 64 || img.descSize > img.blockSize || img.descSize&(img.descSize-1) != 0 {
			return bad("descriptor size %d", img.descSize)
		}
	}
	// Reject anything we don't know how to read rather than misparse it.
	const supportedIncompat = featureIncompatFiletype | featureIncompatExtents | featureIncompatFlexBG | incompat64Bit |
		0x0004 /*recover*/ | 0x0010 /*journal_dev? no: 0x8 is journal_dev; 0x10 is meta_bg*/
	// Explicitly: allow filetype, extents, flex_bg, 64bit, recover, extra_isize(0x2000), mmp(0x100), large_dir(0x4000), csum_seed(0x2000? no 0x2000 is csum_seed)
	allowed := uint32(featureIncompatFiletype | featureIncompatExtents | featureIncompatFlexBG | incompat64Bit | 0x0004 | 0x0100 | 0x2000 | 0x4000)
	_ = supportedIncompat
	if unknown := img.incompat &^ allowed; unknown != 0 {
		return nil, status.UnimplementedErrorf("ext4 image has unsupported incompat features 0x%x", unknown)
	}
	// GDT starts at the block after the superblock.
	img.gdtOffset = int64(img.firstDataBlock+1) * img.blockSize
	if img.blockSize == 1024 {
		img.gdtOffset = 2048
	}
	return img, nil
}

func (img *ext4Image) Close() error {
	if img.closer != nil {
		return img.closer.Close()
	}
	return nil
}

func (img *ext4Image) readBlock(blk uint64, dst []byte) error {
	if blk > uint64(img.size/img.blockSize) {
		return status.InvalidArgumentErrorf("block %d outside image", blk)
	}
	return img.readAt(dst, int64(blk)*img.blockSize)
}

type inode struct {
	num       uint32
	mode      uint16
	uid, gid  uint32
	size      int64
	flags     uint32
	linksCnt  uint16
	iblock    []byte // 60 bytes
	extraSize uint16
}

func (img *ext4Image) readInode(num uint32) (*inode, error) {
	if num == 0 {
		return nil, status.InvalidArgumentError("inode 0")
	}
	group := (num - 1) / img.inodesPerGroup
	index := int64((num - 1) % img.inodesPerGroup)
	if group >= img.ngroups {
		return nil, status.InvalidArgumentErrorf("inode %d is beyond the last block group", num)
	}
	desc := make([]byte, img.descSize)
	if err := img.readAt(desc, img.gdtOffset+int64(group)*img.descSize); err != nil {
		return nil, status.WrapErrorf(err, "read group descriptor %d", group)
	}
	le := binary.LittleEndian
	table := uint64(le.Uint32(desc[0x8:]))
	if img.descSize >= 64 {
		table |= uint64(le.Uint32(desc[0x28:])) << 32
	}
	raw := make([]byte, img.inodeSize)
	if table > uint64(img.size/img.blockSize) {
		return nil, status.InvalidArgumentErrorf("inode table for group %d outside image", group)
	}
	if err := img.readAt(raw, int64(table)*img.blockSize+index*img.inodeSize); err != nil {
		return nil, status.WrapErrorf(err, "read inode %d", num)
	}
	in := &inode{
		num:      num,
		mode:     le.Uint16(raw[0x0:]),
		uid:      uint32(le.Uint16(raw[0x2:])) | uint32(le.Uint16(raw[0x78:]))<<16,
		gid:      uint32(le.Uint16(raw[0x18:])) | uint32(le.Uint16(raw[0x7A:]))<<16,
		size:     int64(le.Uint32(raw[0x4:])) | int64(le.Uint32(raw[0x6C:]))<<32,
		flags:    le.Uint32(raw[0x20:]),
		linksCnt: le.Uint16(raw[0x1A:]),
		iblock:   append([]byte(nil), raw[0x28:0x64]...),
	}
	if img.inodeSize > 128 {
		in.extraSize = le.Uint16(raw[0x80:])
	}
	if in.size < 0 || in.size > img.size {
		// A regular file can be sparse, but nothing legitimately claims to be
		// larger than the whole image.
		return nil, status.InvalidArgumentErrorf("inode %d claims size %d > image size %d", num, in.size, img.size)
	}
	return in, nil
}

// extent is a mapped range of a file.
type extent struct {
	logical uint32
	len     uint32
	start   uint64
	uninit  bool
}

// extents walks the extent tree rooted in the inode.
func (img *ext4Image) extents(in *inode) ([]extent, error) {
	if in.flags&inodeFlagExtents == 0 {
		return nil, status.UnimplementedErrorf("inode %d does not use extents", in.num)
	}
	var out []extent
	visited := map[uint64]bool{}
	imgBlocks := uint64(img.size / img.blockSize)
	var nextLogical uint64 // extents must be in increasing, non-overlapping logical order
	var walk func(node []byte, wantDepth int) error
	walk = func(node []byte, wantDepth int) error {
		le := binary.LittleEndian
		if le.Uint16(node[0:]) != extentMagic {
			return status.InvalidArgumentErrorf("bad extent header magic in inode %d", in.num)
		}
		entries := int(le.Uint16(node[2:]))
		max := int(le.Uint16(node[4:]))
		depth := int(le.Uint16(node[6:]))
		if wantDepth >= 0 && depth != wantDepth {
			return status.InvalidArgumentErrorf("extent node depth %d != expected %d in inode %d", depth, wantDepth, in.num)
		}
		if depth > 5 {
			return status.InvalidArgumentErrorf("extent tree too deep (%d) in inode %d", depth, in.num)
		}
		if entries > max || 12+max*12 > len(node) {
			return status.InvalidArgumentErrorf("extent header %d/%d entries does not fit in inode %d", entries, max, in.num)
		}
		for i := 0; i < entries; i++ {
			if len(out) >= maxExtentsList {
				return status.InvalidArgumentErrorf("inode %d has too many extents", in.num)
			}
			e := node[12+i*12:]
			if depth == 0 {
				l := uint32(le.Uint16(e[4:]))
				uninit := false
				if l > maxExtentLen {
					l -= maxExtentLen
					uninit = true
				}
				logical := le.Uint32(e[0:])
				start := uint64(le.Uint32(e[8:])) | uint64(le.Uint16(e[6:]))<<32
				if l == 0 || uint64(logical) < nextLogical || start == 0 || start+uint64(l) > imgBlocks {
					return status.InvalidArgumentErrorf("invalid extent (logical %d, len %d, start %d) in inode %d", logical, l, start, in.num)
				}
				nextLogical = uint64(logical) + uint64(l)
				out = append(out, extent{logical: logical, len: l, start: start, uninit: uninit})
			} else {
				leaf := uint64(le.Uint32(e[4:])) | uint64(le.Uint16(e[8:]))<<32
				if leaf == 0 || leaf >= imgBlocks || visited[leaf] {
					return status.InvalidArgumentErrorf("invalid extent index block %d in inode %d", leaf, in.num)
				}
				visited[leaf] = true
				child := make([]byte, img.blockSize)
				if err := img.readBlock(leaf, child); err != nil {
					return err
				}
				if err := walk(child, depth-1); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(in.iblock, -1); err != nil {
		return nil, err
	}
	return out, nil
}

// dirent is one directory entry.
type dirent struct {
	inode uint32
	name  string
	ftype uint8
}

// readDir lists a directory linearly. htree index blocks are skipped
// naturally: dx_root hides the index behind the ".." entry's rec_len, and
// dx_node blocks are fake entries with inode 0.
func (img *ext4Image) readDir(in *inode) ([]dirent, error) {
	if ents, ok := img.dirCache[in.num]; ok {
		return ents, nil
	}
	ents, err := img.readDirUncached(in)
	if err != nil {
		return nil, err
	}
	img.dirCache[in.num] = ents
	return ents, nil
}

func (img *ext4Image) readDirUncached(in *inode) ([]dirent, error) {
	if in.flags&inodeFlagInline != 0 {
		return nil, status.UnimplementedErrorf("inline directory (inode %d) is not supported", in.num)
	}
	if in.size > maxDirBytes {
		return nil, status.InvalidArgumentErrorf("directory inode %d is too large (%d bytes)", in.num, in.size)
	}
	exts, err := img.extents(in)
	if err != nil {
		return nil, err
	}
	var out []dirent
	seen := map[string]bool{}
	blk := make([]byte, img.blockSize)
	le := binary.LittleEndian
	for _, e := range exts {
		for i := uint32(0); i < e.len; i++ {
			if int64(e.logical+i)*img.blockSize >= in.size {
				break
			}
			if e.uninit {
				continue
			}
			if err := img.readBlock(e.start+uint64(i), blk); err != nil {
				return nil, err
			}
			off := 0
			for off+8 <= len(blk) {
				ino := le.Uint32(blk[off:])
				recLen := int(le.Uint16(blk[off+4:]))
				nameLen := int(blk[off+6])
				ftype := blk[off+7]
				if recLen < 8 || recLen%4 != 0 || off+recLen > len(blk) || 8+nameLen > recLen {
					return nil, status.InvalidArgumentErrorf("corrupt directory entry in inode %d", in.num)
				}
				if ino != 0 {
					name := string(blk[off+8 : off+8+nameLen])
					if name != "." && name != ".." {
						if name == "" || strings.ContainsAny(name, "/\x00") {
							return nil, status.InvalidArgumentErrorf("invalid entry name %q in inode %d", name, in.num)
						}
						if seen[name] {
							// Duplicate names could be used to make us write
							// through a symlink we created a moment ago.
							return nil, status.InvalidArgumentErrorf("duplicate entry %q in inode %d", name, in.num)
						}
						seen[name] = true
						out = append(out, dirent{inode: ino, name: name, ftype: ftype})
					}
				}
				off += recLen
			}
		}
	}
	return out, nil
}

// lookup resolves a slash-separated path relative to root (no symlink
// following). Returns nil inode if not found.
func (img *ext4Image) lookup(path string) (*inode, error) {
	cur, err := img.readInode(rootInode)
	if err != nil {
		return nil, err
	}
	for part := range strings.SplitSeq(strings.Trim(path, "/"), "/") {
		if part == "" || part == "." {
			continue
		}
		if cur.mode&syscall.S_IFMT != syscall.S_IFDIR {
			return nil, nil
		}
		ents, err := img.readDir(cur)
		if err != nil {
			return nil, err
		}
		var next uint32
		for _, e := range ents {
			if e.name == part {
				next = e.inode
				break
			}
		}
		if next == 0 {
			return nil, nil
		}
		cur, err = img.readInode(next)
		if err != nil {
			return nil, err
		}
	}
	return cur, nil
}

// extractFile writes a regular file's contents to dst, preserving holes.
func (img *ext4Image) extractFile(in *inode, dst string) error {
	if in.flags&inodeFlagInline != 0 {
		return status.UnimplementedErrorf("inline data (inode %d) is not supported", in.num)
	}
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_EXCL, os.FileMode(in.mode&0o777)|0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	defer f.Chmod(os.FileMode(in.mode & 0o777))
	if err := f.Truncate(in.size); err != nil {
		return err
	}
	if in.size == 0 {
		return nil
	}
	exts, err := img.extents(in)
	if err != nil {
		return err
	}
	buf := make([]byte, min(int64(1<<20), img.blockSize*max(1, (1<<20)/img.blockSize)))
	for _, e := range exts {
		if e.uninit {
			continue
		}
		logicalOff := int64(e.logical) * img.blockSize
		remaining := min(int64(e.len)*img.blockSize, in.size-logicalOff)
		srcOff := int64(e.start) * img.blockSize
		for remaining > 0 {
			n := min(remaining, int64(len(buf)))
			if err := img.readAt(buf[:n], srcOff); err != nil {
				return err
			}
			if _, err := f.WriteAt(buf[:n], logicalOff); err != nil {
				return err
			}
			srcOff += n
			logicalOff += n
			remaining -= n
		}
	}
	return nil
}

func (img *ext4Image) readlink(in *inode) (string, error) {
	if in.flags&inodeFlagExtents == 0 {
		if in.size > int64(len(in.iblock)) {
			return "", status.InvalidArgumentErrorf("fast symlink inode %d claims size %d", in.num, in.size)
		}
		return string(in.iblock[:in.size]), nil
	}
	if in.size > 4096 {
		return "", status.InvalidArgumentErrorf("symlink inode %d target too long (%d)", in.num, in.size)
	}
	exts, err := img.extents(in)
	if err != nil {
		return "", err
	}
	data := make([]byte, in.size)
	blk := make([]byte, img.blockSize)
	for _, e := range exts {
		if e.uninit {
			continue
		}
		for i := uint32(0); i < e.len; i++ {
			off := int64(e.logical+i) * img.blockSize
			if off >= in.size {
				break
			}
			if err := img.readBlock(e.start+uint64(i), blk); err != nil {
				return "", err
			}
			copy(data[off:], blk)
		}
	}
	return string(data), nil
}

// extractTree recursively extracts the object at inode `in` to dst.
func (img *ext4Image) extractTree(ctx context.Context, in *inode, dst string, depth int) error {
	if depth > 256 {
		return status.InternalError("directory nesting too deep")
	}
	isDir := in.mode&syscall.S_IFMT == syscall.S_IFDIR
	if st, err := os.Lstat(dst); err == nil {
		// Only the top-level requested directory may already exist (e.g. the
		// output dir itself when extracting "/"); everything below must be new,
		// otherwise a crafted image could make us write through a symlink.
		if !(depth == 0 && isDir && st.IsDir()) {
			return status.InvalidArgumentErrorf("refusing to overwrite existing path %q", dst)
		}
	}
	switch in.mode & syscall.S_IFMT {
	case syscall.S_IFDIR:
		if err := os.Mkdir(dst, os.FileMode(in.mode&0o777)|0o700); err != nil && !(depth == 0 && os.IsExist(err)) {
			return err
		}
		ents, err := img.readDir(in)
		if err != nil {
			return err
		}
		for _, e := range ents {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			child, err := img.readInode(e.inode)
			if err != nil {
				return err
			}
			if err := img.extractTree(ctx, child, filepath.Join(dst, e.name), depth+1); err != nil {
				return err
			}
		}
		// Apply the real mode after populating (it may be read-only).
		return os.Chmod(dst, os.FileMode(in.mode&0o777))
	case syscall.S_IFREG:
		if in.linksCnt > 1 {
			if first, ok := img.linked[in.num]; ok {
				// Hard link to something we already extracted.
				return os.Link(first, dst)
			}
			img.linked[in.num] = dst
		}
		return img.extractFile(in, dst)
	case syscall.S_IFLNK:
		target, err := img.readlink(in)
		if err != nil {
			return err
		}
		return os.Symlink(target, dst)
	default:
		// FIFOs, sockets, devices: skip (debugfs rdump does the same).
		return nil
	}
}

// ImageToDirectory extracts the given paths (files or directories, relative
// to the image root) from the ext4 image into outputDir. Missing paths are
// silently ignored, matching ext4.ImageToDirectory.
func ImageToDirectory(ctx context.Context, imagePath, outputDir string, paths []string) error {
	img, err := openImage(imagePath)
	if err != nil {
		return err
	}
	defer img.Close()
	return img.extractPaths(ctx, outputDir, paths)
}

// ReaderToDirectory is ImageToDirectory for an image served by a ReaderAt
// (e.g. a virtual workspace block device).
func ReaderToDirectory(ctx context.Context, r io.ReaderAt, size int64, outputDir string, paths []string) error {
	img, err := openImageReader(r, size)
	if err != nil {
		return err
	}
	return img.extractPaths(ctx, outputDir, paths)
}

func (img *ext4Image) extractPaths(ctx context.Context, outputDir string, paths []string) error {
	for _, p := range paths {
		clean := filepath.Clean("/" + p)
		in, err := img.lookup(clean)
		if err != nil {
			return status.WrapErrorf(err, "lookup %q", p)
		}
		if in == nil {
			continue
		}
		dst := filepath.Join(outputDir, clean)
		if !strings.HasPrefix(dst, filepath.Clean(outputDir)) {
			return status.InvalidArgumentErrorf("path %q escapes output dir", p)
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
			return err
		}
		if err := img.extractTree(ctx, in, dst, 0); err != nil {
			return status.WrapErrorf(err, "extract %q", p)
		}
	}
	return nil
}
