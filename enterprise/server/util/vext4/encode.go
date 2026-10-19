//go:build linux

// This file contains the constants of the ext4 on-disk format and the
// functions that encode its structures into bytes.

package vext4

import (
	"encoding/binary"
	"fmt"
	"syscall"
)

// Geometry fixed for every image.
const (
	blockSize = 4096
	// A block bitmap is one block, so a group holds blockSize*8 blocks.
	blocksPerGroup = blockSize * 8
	// An inode bitmap is one block too.
	maxInodesPerGroup = blockSize * 8
	inodeSize         = 256
	descSize          = 32
	// The superblock sits after the first KiB, which is left for a boot
	// sector.
	superblockOffset = 1024
	superblockSize   = 1024
	rootInode        = 2
	// Inodes before firstInode are reserved.
	firstInode = 11
	maxNameLen = 255
	// Symlink targets shorter than the inode's i_block array are stored
	// there instead of in a block.
	inodeBlockSize = 60
	fastSymlinkMax = inodeBlockSize
	// Inodes use 32 bytes past the classic 128 for extra timestamp bits.
	extraIsize = 32
)

// Values that every image's superblock carries unchanged, namely the magic
// number, state, revision, and the feature flags and defaults this package
// writes images with.
const (
	superblockMagic = 0xEF53
	stateClean      = 1
	errorsContinue  = 1
	revisionDynamic = 1
	// log2 of the block size in units of 1024.
	logBlockSize4K = 2

	compatExtAttr       = 0x8
	compatDirIndex      = 0x20
	incompatFiletype    = 0x2
	incompatExtents     = 0x40
	incompatFlexBG      = 0x200
	roCompatSparseSuper = 0x1
	roCompatLargeFile   = 0x2
	roCompatHugeFile    = 0x8
	roCompatDirNlink    = 0x20
	roCompatExtraIsize  = 0x40

	// The default mount options, user_xattr and acl, as mke2fs sets them.
	defaultMountOpts = 0xC
	// The s_flags bit saying directory hashes use signed chars.
	flagSignedHash = 0x1
	// The s_def_hash_version value for half_md4.
	hashHalfMD4 = 1
)

// Superblock field offsets (struct ext4_super_block).
const (
	sbInodesCount      = 0x0
	sbBlocksCountLo    = 0x4
	sbFreeBlocksLo     = 0xC
	sbFreeInodes       = 0x10
	sbLogBlockSize     = 0x18
	sbLogClusterSize   = 0x1C
	sbBlocksPerGroup   = 0x20
	sbClustersPerGroup = 0x24
	sbInodesPerGroup   = 0x28
	sbWtime            = 0x30
	sbMaxMntCount      = 0x36
	sbMagic            = 0x38
	sbState            = 0x3A
	sbErrors           = 0x3C
	sbLastCheck        = 0x40
	sbRevLevel         = 0x4C
	sbFirstIno         = 0x54
	sbInodeSize        = 0x58
	sbBlockGroupNr     = 0x5A
	sbFeatureCompat    = 0x5C
	sbFeatureIncompat  = 0x60
	sbFeatureRoCompat  = 0x64
	sbUUID             = 0x68
	sbHashSeed         = 0xEC
	sbDefHashVersion   = 0xFC
	sbDefaultMountOpts = 0x100
	sbMkfsTime         = 0x108
	sbMinExtraIsize    = 0x15C
	sbWantExtraIsize   = 0x15E
	sbFlags            = 0x160
	sbLogGroupsPerFlex = 0x174
)

// Group descriptor field offsets (struct ext4_group_desc).
const (
	gdBlockBitmapLo = 0x0
	gdInodeBitmapLo = 0x4
	gdInodeTableLo  = 0x8
	gdFreeBlocksLo  = 0xC
	gdFreeInodesLo  = 0xE
	gdUsedDirsLo    = 0x10
)

// Inode field offsets (struct ext4_inode).
const (
	inoMode       = 0x0
	inoUid        = 0x2
	inoSizeLo     = 0x4
	inoAtime      = 0x8
	inoCtime      = 0xC
	inoMtime      = 0x10
	inoGid        = 0x18
	inoLinksCount = 0x1A
	inoBlocksLo   = 0x1C
	inoFlags      = 0x20
	inoBlock      = 0x28
	inoSizeHigh   = 0x6C
	inoBlocksHigh = 0x74
	inoUidHigh    = 0x78
	inoGidHigh    = 0x7A
	inoExtraIsize = 0x80
	inoCtimeExtra = 0x84
	inoMtimeExtra = 0x88
	inoAtimeExtra = 0x8C
	inoCrtime     = 0x90

	inodeFlagExtents = 0x80000
)

// Extent tree structures. A node is a header followed by extents (depth 0)
// or index entries (depth 1), 12 bytes each. An inode's i_block holds 4
// entries, and a leaf block holds leafExtents.
const (
	extentMagic      = 0xF30A
	extentHeaderSize = 12
	extentSize       = 12
	maxExtentLen     = 32768
	inodeExtents     = (inodeBlockSize - extentHeaderSize) / extentSize
	leafExtents      = (blockSize - extentHeaderSize) / extentSize

	ehMagic   = 0
	ehEntries = 2
	ehMax     = 4
	ehDepth   = 6
	eeBlock   = 0
	eeLen     = 4
	eeStartHi = 6
	eeStartLo = 8
	eiBlock   = 0
	eiLeafLo  = 4
	eiLeafHi  = 8
)

// Directory entries (struct ext4_dir_entry_2), each a header followed by
// the name padded to a multiple of 4 bytes.
const (
	deInode    = 0
	deRecLen   = 4
	deNameLen  = 6
	deFileType = 7
	deName     = 8

	fileTypeRegular = 1
	fileTypeDir     = 2
	fileTypeSymlink = 7
)

var (
	le = binary.LittleEndian
)

// superblock is a structured representation of the ext4 superblock, with
// only the fields that vary between images. encodeSuperblock fills in the
// rest, such as the block size, the feature flags and the inode size, from
// constants.
type superblock struct {
	inodes, blocks, freeBlocks, freeInodes, inodesPerGroup uint32
	// logGroupsPerFlex is log2 of the groups whose metadata is packed
	// together.
	logGroupsPerFlex uint8
	uuid, hashSeed   [16]byte
	now              uint32
}

// encodeSuperblock returns the superblock, with s_block_group_nr left for
// the caller to set on each copy.
func encodeSuperblock(s superblock) []byte {
	buf := make([]byte, superblockSize)
	le.PutUint32(buf[sbInodesCount:], s.inodes)
	le.PutUint32(buf[sbBlocksCountLo:], s.blocks)
	le.PutUint32(buf[sbFreeBlocksLo:], s.freeBlocks)
	le.PutUint32(buf[sbFreeInodes:], s.freeInodes)
	le.PutUint32(buf[sbLogBlockSize:], logBlockSize4K)
	le.PutUint32(buf[sbLogClusterSize:], logBlockSize4K)
	le.PutUint32(buf[sbBlocksPerGroup:], blocksPerGroup)
	le.PutUint32(buf[sbClustersPerGroup:], blocksPerGroup)
	le.PutUint32(buf[sbInodesPerGroup:], s.inodesPerGroup)
	le.PutUint32(buf[sbWtime:], s.now)
	le.PutUint16(buf[sbMaxMntCount:], 0xFFFF)
	le.PutUint16(buf[sbMagic:], superblockMagic)
	le.PutUint16(buf[sbState:], stateClean)
	le.PutUint16(buf[sbErrors:], errorsContinue)
	le.PutUint32(buf[sbLastCheck:], s.now)
	le.PutUint32(buf[sbRevLevel:], revisionDynamic)
	le.PutUint32(buf[sbFirstIno:], firstInode)
	le.PutUint16(buf[sbInodeSize:], inodeSize)
	le.PutUint32(buf[sbFeatureCompat:], compatExtAttr|compatDirIndex)
	le.PutUint32(buf[sbFeatureIncompat:], incompatFiletype|incompatExtents|incompatFlexBG)
	le.PutUint32(buf[sbFeatureRoCompat:], roCompatSparseSuper|roCompatLargeFile|roCompatHugeFile|roCompatDirNlink|roCompatExtraIsize)
	copy(buf[sbUUID:], s.uuid[:])
	copy(buf[sbHashSeed:], s.hashSeed[:])
	buf[sbDefHashVersion] = hashHalfMD4
	le.PutUint32(buf[sbDefaultMountOpts:], defaultMountOpts)
	le.PutUint32(buf[sbMkfsTime:], s.now)
	le.PutUint16(buf[sbMinExtraIsize:], extraIsize)
	le.PutUint16(buf[sbWantExtraIsize:], extraIsize)
	le.PutUint32(buf[sbFlags:], flagSignedHash)
	buf[sbLogGroupsPerFlex] = s.logGroupsPerFlex
	return buf
}

// groupDesc is a structured representation of an ext4 group descriptor,
// with only the fields that vary between groups. Those are where the
// group's bitmaps and inode table start, how many of its blocks and inodes
// are free, and how many of its inodes are directories. encodeGroupDesc
// writes it. The descriptor's flags and checksum stay zero, since the image
// uses neither uninit_bg nor metadata_csum.
type groupDesc struct {
	blockBitmap, inodeBitmap, inodeTable uint64
	freeBlocks, freeInodes, usedDirs     uint16
}

func encodeGroupDesc(buf []byte, d groupDesc) {
	le.PutUint32(buf[gdBlockBitmapLo:], uint32(d.blockBitmap))
	le.PutUint32(buf[gdInodeBitmapLo:], uint32(d.inodeBitmap))
	le.PutUint32(buf[gdInodeTableLo:], uint32(d.inodeTable))
	le.PutUint16(buf[gdFreeBlocksLo:], d.freeBlocks)
	le.PutUint16(buf[gdFreeInodesLo:], d.freeInodes)
	le.PutUint16(buf[gdUsedDirsLo:], d.usedDirs)
}

// encodeBlockBitmap writes a group's block bitmap from its part of the
// used bitset.
func encodeBlockBitmap(buf []byte, used bitset) {
	for i := range used {
		le.PutUint64(buf[i*8:], used[i])
	}
}

// encodeInodeBitmap writes a group's inode bitmap, with its first used
// inodes in use and, as the kernel and e2fsck expect, the bits past the
// group's inodes set.
func encodeInodeBitmap(buf []byte, used, inodesPerGroup uint64) {
	clear(buf)
	for i := range used {
		buf[i/8] |= 1 << (i % 8)
	}
	for i := inodesPerGroup; i < uint64(len(buf))*8; i++ {
		buf[i/8] |= 1 << (i % 8)
	}
}

// encodeInode writes the on-disk inode into buf, which is inodeSize bytes.
func encodeInode(buf []byte, in *inode, crtime uint32) error {
	// i_blocks counts 512-byte sectors of everything the inode owns,
	// including its extent index blocks.
	sectors := (in.dataBlocks() + uint64(len(in.leaves))) * (blockSize / 512)
	size := in.sizeBytes()
	le.PutUint16(buf[inoMode:], uint16(in.mode))
	le.PutUint16(buf[inoUid:], uint16(in.uid))
	le.PutUint32(buf[inoSizeLo:], uint32(size))
	le.PutUint32(buf[inoAtime:], uint32(in.atime.Sec))
	le.PutUint32(buf[inoCtime:], uint32(in.ctime.Sec))
	le.PutUint32(buf[inoMtime:], uint32(in.mtime.Sec))
	le.PutUint16(buf[inoGid:], uint16(in.gid))
	le.PutUint16(buf[inoLinksCount:], uint16(in.links))
	le.PutUint32(buf[inoBlocksLo:], uint32(sectors))
	le.PutUint32(buf[inoSizeHigh:], uint32(size>>32))
	le.PutUint16(buf[inoBlocksHigh:], uint16(sectors>>32))
	le.PutUint16(buf[inoUidHigh:], uint16(in.uid>>16))
	le.PutUint16(buf[inoGidHigh:], uint16(in.gid>>16))
	le.PutUint16(buf[inoExtraIsize:], extraIsize)
	le.PutUint32(buf[inoCtimeExtra:], timeExtra(in.ctime))
	le.PutUint32(buf[inoMtimeExtra:], timeExtra(in.mtime))
	le.PutUint32(buf[inoAtimeExtra:], timeExtra(in.atime))
	le.PutUint32(buf[inoCrtime:], crtime)
	tree := buf[inoBlock : inoBlock+inodeBlockSize]
	if in.isFastSymlink() {
		copy(tree, in.target)
		return nil
	}
	le.PutUint32(buf[inoFlags:], inodeFlagExtents)
	if len(in.leaves) == 0 {
		if len(in.extents) > inodeExtents {
			return fmt.Errorf("inode %d: %d extents without index blocks", in.num, len(in.extents))
		}
		encodeExtentHeader(tree, len(in.extents), inodeExtents, 0)
		for i, e := range in.extents {
			encodeExtent(tree[extentHeaderSize+i*extentSize:], e)
		}
		return nil
	}
	encodeExtentHeader(tree, len(in.leaves), inodeExtents, 1)
	for i, leaf := range in.leaves {
		idx := tree[extentHeaderSize+i*extentSize:]
		le.PutUint32(idx[eiBlock:], uint32(in.extents[i*leafExtents].logical))
		le.PutUint32(idx[eiLeafLo:], uint32(leaf))
		le.PutUint16(idx[eiLeafHi:], uint16(leaf>>32))
	}
	return nil
}

// encodeExtentLeaf writes an extent index leaf block holding the extents.
func encodeExtentLeaf(buf []byte, extents []extent) {
	encodeExtentHeader(buf, len(extents), leafExtents, 0)
	for i, e := range extents {
		encodeExtent(buf[extentHeaderSize+i*extentSize:], e)
	}
}

func encodeExtentHeader(buf []byte, entries, max, depth int) {
	le.PutUint16(buf[ehMagic:], extentMagic)
	le.PutUint16(buf[ehEntries:], uint16(entries))
	le.PutUint16(buf[ehMax:], uint16(max))
	le.PutUint16(buf[ehDepth:], uint16(depth))
}

func encodeExtent(buf []byte, e extent) {
	le.PutUint32(buf[eeBlock:], uint32(e.logical))
	le.PutUint16(buf[eeLen:], uint16(e.length))
	le.PutUint16(buf[eeStartHi:], uint16(e.block>>32))
	le.PutUint32(buf[eeStartLo:], uint32(e.block))
}

// timeExtra encodes the nanoseconds and epoch bits of an ext4 timestamp.
func timeExtra(t syscall.Timespec) uint32 {
	return uint32(t.Nsec)<<2 | uint32(t.Sec>>32)&3
}

// encodeDirectory returns the directory's blocks. Entries never cross a
// block, and the last entry in each block is padded out to the block's
// end.
func encodeDirectory(in *inode) []byte {
	data := make([]byte, in.dataBlocks()*blockSize)
	off := 0
	last := -1
	put := func(num uint32, name string, fileType uint8) {
		size := dirEntrySize(len(name))
		if off%blockSize+size > blockSize {
			// Pad the previous entry to the end of its block.
			blockEnd := (off/blockSize + 1) * blockSize
			le.PutUint16(data[last+deRecLen:], uint16(blockEnd-last))
			off = blockEnd
		}
		le.PutUint32(data[off+deInode:], num)
		le.PutUint16(data[off+deRecLen:], uint16(size))
		data[off+deNameLen] = uint8(len(name))
		data[off+deFileType] = fileType
		copy(data[off+deName:], name)
		last = off
		off += size
	}
	put(in.num, ".", fileTypeDir)
	put(in.parentNum, "..", fileTypeDir)
	for _, c := range in.children {
		put(c.inode.num, c.name, c.inode.fileType())
	}
	blockEnd := (off/blockSize + 1) * blockSize
	if off%blockSize == 0 {
		blockEnd = off
	}
	le.PutUint16(data[last+deRecLen:], uint16(blockEnd-last))
	return data
}

// dirBlocksFor returns the number of blocks needed to hold the "." and ".."
// entries plus the given children.
func dirBlocksFor(children []dirEntry) uint64 {
	blocks := uint64(1)
	used := dirEntrySize(1) + dirEntrySize(2)
	for _, c := range children {
		size := dirEntrySize(len(c.name))
		if used+size > blockSize {
			blocks++
			used = 0
		}
		used += size
	}
	return blocks
}

func dirEntrySize(nameLen int) int {
	return (deName + nameLen + 3) &^ 3
}
