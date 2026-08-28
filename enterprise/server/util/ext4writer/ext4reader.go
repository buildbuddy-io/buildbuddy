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
	"errors"
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
	f              *os.File
	blockSize      int64
	inodeSize      int64
	inodesPerGroup uint32
	firstDataBlock uint32
	descSize       int64
	gdtOffset      int64
	incompat       uint32
	buf            []byte
}

func openImage(path string) (*ext4Image, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	sb := make([]byte, 1024)
	if _, err := f.ReadAt(sb, 1024); err != nil {
		f.Close()
		return nil, status.WrapError(err, "read superblock")
	}
	le := binary.LittleEndian
	if le.Uint16(sb[0x38:]) != superMagic {
		f.Close()
		return nil, status.InvalidArgumentError("not an ext4 image (bad magic)")
	}
	img := &ext4Image{
		f:              f,
		blockSize:      1024 << le.Uint32(sb[0x18:]),
		inodesPerGroup: le.Uint32(sb[0x28:]),
		firstDataBlock: le.Uint32(sb[0x14:]),
		incompat:       le.Uint32(sb[0x60:]),
	}
	img.inodeSize = 128
	if le.Uint32(sb[0x4C:]) >= 1 {
		img.inodeSize = int64(le.Uint16(sb[0x58:]))
	}
	img.descSize = 32
	if img.incompat&incompat64Bit != 0 {
		img.descSize = int64(le.Uint16(sb[0xFE:]))
		if img.descSize < 64 {
			img.descSize = 64
		}
	}
	if img.incompat&incompatEncrypt != 0 {
		f.Close()
		return nil, status.UnimplementedError("encrypted ext4 images are not supported")
	}
	// GDT starts at the block after the superblock.
	img.gdtOffset = int64(img.firstDataBlock+1) * img.blockSize
	if img.blockSize == 1024 {
		img.gdtOffset = 2048
	}
	img.buf = make([]byte, img.blockSize)
	return img, nil
}

func (img *ext4Image) Close() error { return img.f.Close() }

func (img *ext4Image) readBlock(blk uint64, dst []byte) error {
	_, err := img.f.ReadAt(dst, int64(blk)*img.blockSize)
	if err != nil && !(errors.Is(err, io.EOF)) {
		return err
	}
	return nil
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
	desc := make([]byte, img.descSize)
	if _, err := img.f.ReadAt(desc, img.gdtOffset+int64(group)*img.descSize); err != nil {
		return nil, status.WrapErrorf(err, "read group descriptor %d", group)
	}
	le := binary.LittleEndian
	table := uint64(le.Uint32(desc[0x8:]))
	if img.descSize >= 64 {
		table |= uint64(le.Uint32(desc[0x28:])) << 32
	}
	raw := make([]byte, img.inodeSize)
	if _, err := img.f.ReadAt(raw, int64(table)*img.blockSize+index*img.inodeSize); err != nil {
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
	var walk func(node []byte, depthLimit int) error
	walk = func(node []byte, depthLimit int) error {
		le := binary.LittleEndian
		if le.Uint16(node[0:]) != extentMagic {
			return status.InternalErrorf("bad extent header magic in inode %d", in.num)
		}
		entries := int(le.Uint16(node[2:]))
		depth := int(le.Uint16(node[6:]))
		if depthLimit < 0 {
			return status.InternalErrorf("extent tree too deep in inode %d", in.num)
		}
		if 12+entries*12 > len(node) {
			return status.InternalErrorf("extent header claims %d entries in inode %d", entries, in.num)
		}
		for i := 0; i < entries; i++ {
			e := node[12+i*12:]
			if depth == 0 {
				l := uint32(le.Uint16(e[4:]))
				uninit := false
				if l > maxExtentLen {
					l -= maxExtentLen
					uninit = true
				}
				out = append(out, extent{
					logical: le.Uint32(e[0:]),
					len:     l,
					start:   uint64(le.Uint32(e[8:])) | uint64(le.Uint16(e[6:]))<<32,
					uninit:  uninit,
				})
			} else {
				leaf := uint64(le.Uint32(e[4:])) | uint64(le.Uint16(e[8:]))<<32
				child := make([]byte, img.blockSize)
				if err := img.readBlock(leaf, child); err != nil {
					return err
				}
				if err := walk(child, depthLimit-1); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(in.iblock, 8); err != nil {
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
	if in.flags&inodeFlagInline != 0 {
		return nil, status.UnimplementedErrorf("inline directory (inode %d) is not supported", in.num)
	}
	exts, err := img.extents(in)
	if err != nil {
		return nil, err
	}
	var out []dirent
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
				if recLen < 8 || off+recLen > len(blk) || 8+nameLen > recLen {
					return nil, status.InternalErrorf("corrupt directory entry in inode %d", in.num)
				}
				if ino != 0 {
					name := string(blk[off+8 : off+8+nameLen])
					if name != "." && name != ".." {
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
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(in.mode&0o777))
	if err != nil {
		return err
	}
	defer f.Close()
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
			if _, err := img.f.ReadAt(buf[:n], srcOff); err != nil && !errors.Is(err, io.EOF) {
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
	if in.size < 60 && in.flags&inodeFlagExtents == 0 {
		return string(in.iblock[:in.size]), nil
	}
	exts, err := img.extents(in)
	if err != nil {
		return "", err
	}
	data := make([]byte, 0, in.size)
	blk := make([]byte, img.blockSize)
	for _, e := range exts {
		for i := uint32(0); i < e.len && int64(len(data)) < in.size; i++ {
			if err := img.readBlock(e.start+uint64(i), blk); err != nil {
				return "", err
			}
			data = append(data, blk[:min(img.blockSize, in.size-int64(len(data)))]...)
		}
	}
	return string(data), nil
}

// extractTree recursively extracts the object at inode `in` to dst.
func (img *ext4Image) extractTree(ctx context.Context, in *inode, dst string, depth int) error {
	if depth > 256 {
		return status.InternalError("directory nesting too deep")
	}
	switch in.mode & syscall.S_IFMT {
	case syscall.S_IFDIR:
		if err := os.MkdirAll(dst, os.FileMode(in.mode&0o777)|0o700); err != nil {
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
			if strings.ContainsRune(e.name, '/') || e.name == "." || e.name == ".." {
				return status.InternalErrorf("invalid directory entry name %q", e.name)
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


