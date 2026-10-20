//go:build linux

// This file defines the block-level layout of an image. Blocks are 4 KiB,
// and the image is a whole number of 128 MiB groups of 32768 blocks. Every
// block is one of the following, listed in the order they are laid out.
// write.go writes all of them except file data and free blocks, which stay
// holes in the image file.
//
//   - A superblock copy. Block 0 holds the primary superblock at byte
//     1024, and the first block of groups 1, 3^n, 5^n and 7^n holds a
//     backup (sparse_super). It records the block and inode counts,
//     inodes per group, feature flags, UUID and free counts, and the guest
//     reads it at mount.
//   - A group descriptor table block. The gdtBlocks blocks after each
//     superblock copy hold a 32-byte descriptor per group, giving the
//     block numbers of the group's bitmaps and inode table and its free
//     block and inode counts. The guest reads them to find all other
//     metadata.
//   - A block bitmap, one block per group, with a bit per block of the
//     group. Set bits are blocks in use. The guest consults and updates it
//     when allocating or freeing blocks.
//   - An inode bitmap, one block per group, with a bit per inode of the
//     group and the bits past the last inode set. The guest consults and
//     updates it when creating or deleting inodes.
//   - An inode table block, holding 16 of a group's 256-byte inodes. A
//     group's table is ipg/16 contiguous blocks, of which only the used
//     prefix is written. The guest reads an inode by number and rewrites
//     it when the file changes.
//   - A file's data block. A file gets its size in blocks, rounded up, as
//     extents of at most 32768 blocks that do not span a superblock copy.
//     These blocks are never written at build time. The Store serves reads
//     of them from the file's source and keeps the guest's writes in the
//     image file, tracked by a per-block dirty bitmap.
//   - A directory block, holding ext4_dir_entry_2 records (inode number,
//     type and name) for ".", ".." and each child. An entry never crosses
//     a block boundary, so a directory takes as many blocks as its entries
//     fill. The guest reads them on lookups and rewrites them when entries
//     are added or removed.
//   - A symlink target block, for a target of 60 bytes or more. Shorter
//     targets are stored in the inode itself.
//   - An extent index block, for an inode with more than the 4 extents its
//     inode holds. It holds up to 340 extents, and an inode points at up to
//     4 of them. The guest follows it to find the inode's data blocks.
//   - A free block, allocated to nothing above. There are at least
//     freeBytes worth, plus whatever rounding to whole groups leaves. The
//     guest allocates them for the files it creates.

package vext4

import (
	"errors"
	"fmt"
	"math/bits"
	"sort"
	"sync/atomic"
)

const (
	// minInodesPerGroup matches the mke2fs default of one inode per 16 KiB
	// of disk.
	minInodesPerGroup = blocksPerGroup / 4
	// The guest gets spare inodes for the files it creates, a quarter of
	// the tree's and at least minSpareInodes.
	minSpareInodes = 1024
	// layoutAttempts bounds how often layout grows the image.
	layoutAttempts = 64
	wordsPerGroup  = blocksPerGroup / 64
)

// blockLayout is where everything in an image is. It holds the group count
// and inodes per group from the superblock, each group's bitmap and inode
// table locations from its group descriptor, and the block bitmap, which
// together are all that ext4 itself records about an image's layout. It
// also holds the one thing ext4 has no record of, which blocks stand for
// which source file.
// Each inode's own data blocks are recorded on the inode, in its extents, as
// ext4 records them in the inode's extent tree. layout fills all of it in
// from the tree.
type blockLayout struct {
	// groups is the number of block groups, ipg the inodes per group,
	// gdtBlocks the blocks the group descriptor table takes, and
	// totalBlocks the size of the image in blocks.
	groups      uint64
	ipg         uint64
	gdtBlocks   uint64
	totalBlocks uint64
	// blockBitmap, inodeBitmap and inodeTable hold the first block of each
	// group's bitmaps and inode table, indexed by group.
	blockBitmap []uint64
	inodeBitmap []uint64
	inodeTable  []uint64
	// used has a bit per block, set for every block the image uses.
	used bitset
	// runs are the virtual runs, image blocks that stand for the contents of
	// source files, files lists those sources, and virtualBlocks is how many
	// blocks the runs cover. The Store serves them.
	runs          []virtualRun
	files         []*sourceFile
	virtualBlocks uint64
}

// layout chooses the image's size and assigns every block, growing the
// image until the tree fits with at least freeBytes left over and spare
// inodes for the guest to use.
func layout(inodes inodeList, freeBytes int64) (*blockLayout, error) {
	usedInodes := uint64(len(inodes))
	neededInodes := usedInodes + max(usedInodes/4, minSpareInodes)
	freeBlocks := (uint64(freeBytes) + blockSize - 1) / blockSize
	dataBlocks := uint64(0)
	for _, in := range inodes {
		if in != nil {
			dataBlocks += in.dataBlocks()
		}
	}
	// Start from the groups the data alone needs. Each attempt adds the
	// metadata, and retries with more groups if the layout overflows or
	// leaves too little free.
	groups := max(uint64(1), (dataBlocks+freeBlocks+blocksPerGroup-1)/blocksPerGroup)
	for range layoutAttempts {
		ipg := max(minInodesPerGroup, (neededInodes+groups-1)/groups)
		// Inode tables are whole blocks, so ipg is a multiple of the inodes
		// per block.
		ipg = (ipg + blockSize/inodeSize - 1) / (blockSize / inodeSize) * (blockSize / inodeSize)
		if ipg > maxInodesPerGroup {
			groups = (neededInodes + maxInodesPerGroup - 1) / maxInodesPerGroup
			continue
		}
		l, err := layoutWith(inodes, groups, ipg)
		var oos *outOfSpaceError
		switch {
		case errors.As(err, &oos):
			groups += max(uint64(1), (oos.shortBlocks+blocksPerGroup-1)/blocksPerGroup)
		case err != nil:
			return nil, err
		case l.freeBlocks() < freeBlocks:
			groups += max(uint64(1), (freeBlocks-l.freeBlocks()+blocksPerGroup-1)/blocksPerGroup)
		default:
			return l, nil
		}
	}
	return nil, fmt.Errorf("image layout did not converge")
}

// layoutWith assigns every block of an image with the given number of
// groups and inodes per group. It fails with an outOfSpaceError if the tree
// does not fit.
func layoutWith(inodes inodeList, groups, ipg uint64) (*blockLayout, error) {
	l := &blockLayout{
		groups:      groups,
		ipg:         ipg,
		gdtBlocks:   (groups*descSize + blockSize - 1) / blockSize,
		totalBlocks: groups * blocksPerGroup,
		blockBitmap: make([]uint64, groups),
		inodeBitmap: make([]uint64, groups),
		inodeTable:  make([]uint64, groups),
	}
	l.used = newBitset(l.totalBlocks)
	a := &allocator{layout: l}
	// Reserve the superblock and descriptor table in group 0 and their
	// backups, which must not hold anything else.
	for g := range groups {
		if hasSuper(g) {
			start := g * blocksPerGroup
			a.reserved = append(a.reserved, [2]uint64{start, start + 1 + l.gdtBlocks})
			l.used.setRange(start, 1+l.gdtBlocks)
		}
	}
	var err error
	for g := range groups {
		if l.blockBitmap[g], err = a.contiguous(1); err != nil {
			return nil, err
		}
		if l.inodeBitmap[g], err = a.contiguous(1); err != nil {
			return nil, err
		}
	}
	for g := range groups {
		if l.inodeTable[g], err = a.contiguous(ipg * inodeSize / blockSize); err != nil {
			return nil, err
		}
	}
	// File data goes next, so that the virtual blocks form one contiguous
	// range, then directories and symlink targets, then extent index
	// blocks, whose number depends on how the data was split.
	sources := map[Source]*sourceFile{}
	for _, in := range inodes {
		if in == nil || in.kind != kindFile || in.dataBlocks() == 0 {
			continue
		}
		if in.extents, err = a.extents(in.dataBlocks()); err != nil {
			return nil, err
		}
		// Inodes with the same Source, such as files with the same
		// digest, share a source file so that it is opened once.
		file := sources[in.source]
		if file == nil {
			file = newSourceFile(in.source, in.size)
			sources[in.source] = file
			l.files = append(l.files, file)
		}
		for _, e := range in.extents {
			l.runs = append(l.runs, virtualRun{imageBlock: e.block, length: e.length, file: file, fileBlock: e.logical})
			l.virtualBlocks += e.length
		}
	}
	for _, in := range inodes {
		if in == nil || in.kind == kindFile || in.dataBlocks() == 0 {
			continue
		}
		if in.extents, err = a.extents(in.dataBlocks()); err != nil {
			return nil, err
		}
	}
	for _, in := range inodes {
		if in == nil {
			continue
		}
		// Drop the index blocks of an earlier, smaller attempt, since the
		// inode's data may now be in fewer extents and need none.
		in.leaves = nil
		if len(in.extents) <= inodeExtents {
			continue
		}
		leaves := (uint64(len(in.extents)) + leafExtents - 1) / leafExtents
		if leaves > inodeExtents {
			return nil, fmt.Errorf("inode %d: too many extents (%d)", in.num, len(in.extents))
		}
		for range leaves {
			blk, err := a.contiguous(1)
			if err != nil {
				return nil, err
			}
			in.leaves = append(in.leaves, blk)
		}
	}
	sort.Slice(l.runs, func(i, j int) bool { return l.runs[i].imageBlock < l.runs[j].imageBlock })
	return l, nil
}

func (l *blockLayout) sizeBytes() int64 {
	return int64(l.totalBlocks) * blockSize
}

func (l *blockLayout) freeBlocks() uint64 {
	return l.totalBlocks - l.used.count()
}

// groupUsed returns the group's part of the used bitset.
func (l *blockLayout) groupUsed(g uint64) bitset {
	return l.used[g*wordsPerGroup : (g+1)*wordsPerGroup]
}

// inodesUsedInGroup returns how many of the group's inodes are in use.
// Inodes are numbered in walk order, so each group fills before the next
// starts.
func (l *blockLayout) inodesUsedInGroup(usedInodes, g uint64) uint64 {
	if usedInodes <= g*l.ipg {
		return 0
	}
	return min(l.ipg, usedInodes-g*l.ipg)
}

// hasSuper returns whether the group holds a backup superblock and group
// descriptor table under the sparse_super feature.
func hasSuper(group uint64) bool {
	if group <= 1 {
		return true
	}
	for _, base := range []uint64{3, 5, 7} {
		n := base
		for n < group {
			n *= base
		}
		if n == group {
			return true
		}
	}
	return false
}

// allocator hands out image blocks in increasing order, skipping the
// ranges reserved for the superblock copies and group descriptor tables.
type allocator struct {
	layout *blockLayout
	next   uint64
	// reserved holds the reserved ranges as [start, end) block pairs in
	// increasing order, and nextReserved is the index of the first one that
	// next has not passed yet.
	reserved     [][2]uint64
	nextReserved int
}

// contiguous allocates n adjacent blocks, skipping past a reserved range that
// would split them.
func (a *allocator) contiguous(n uint64) (uint64, error) {
	for a.available() < n {
		if a.nextReserved >= len(a.reserved) || a.next >= a.layout.totalBlocks {
			return 0, &outOfSpaceError{shortBlocks: n}
		}
		a.next = a.reserved[a.nextReserved][0]
	}
	return a.take(n), nil
}

// extents allocates n blocks as a list of extents, each at most
// maxExtentLen blocks and none crossing a reserved range.
func (a *allocator) extents(n uint64) ([]extent, error) {
	var extents []extent
	logical := uint64(0)
	for n > 0 {
		avail := a.available()
		if avail == 0 {
			return nil, &outOfSpaceError{shortBlocks: n}
		}
		length := min(n, avail, maxExtentLen)
		extents = append(extents, extent{logical: logical, block: a.take(length), length: length})
		logical += length
		n -= length
	}
	return extents, nil
}

// available returns the number of blocks available from next before the
// next reserved range or the end of the image.
func (a *allocator) available() uint64 {
	a.skipReserved()
	end := a.layout.totalBlocks
	if a.nextReserved < len(a.reserved) {
		end = min(end, a.reserved[a.nextReserved][0])
	}
	if a.next >= end {
		return 0
	}
	return end - a.next
}

// skipReserved moves next past any reserved range it falls in.
func (a *allocator) skipReserved() {
	for a.nextReserved < len(a.reserved) && a.reserved[a.nextReserved][1] <= a.next {
		a.nextReserved++
	}
	if a.nextReserved < len(a.reserved) && a.reserved[a.nextReserved][0] <= a.next {
		a.next = a.reserved[a.nextReserved][1]
		a.nextReserved++
	}
}

func (a *allocator) take(n uint64) uint64 {
	start := a.next
	a.next += n
	a.layout.used.setRange(start, n)
	return start
}

// extent is a run of an inode's logical blocks and the image blocks that
// hold them.
type extent struct {
	logical uint64
	block   uint64
	length  uint64
}

type outOfSpaceError struct {
	shortBlocks uint64
}

func (e *outOfSpaceError) Error() string {
	return fmt.Sprintf("image is %d blocks too small", e.shortBlocks)
}

// bitset is a set of block numbers. Its methods are safe for concurrent
// use, so that a Store can mark blocks dirty while it serves reads.
type bitset []uint64

func newBitset(n uint64) bitset {
	return make(bitset, (n+63)/64)
}

func (b bitset) test(i uint64) bool {
	return atomic.LoadUint64(&b[i/64])&(1<<(i%64)) != 0
}

// set adds i and returns whether it was not already in the set.
func (b bitset) set(i uint64) bool {
	mask := uint64(1) << (i % 64)
	return atomic.OrUint64(&b[i/64], mask)&mask == 0
}

func (b bitset) setRange(start, n uint64) {
	for i := start; i < start+n; i++ {
		b.set(i)
	}
}

// count returns the number of members.
func (b bitset) count() uint64 {
	n := uint64(0)
	for i := range b {
		n += uint64(bits.OnesCount64(atomic.LoadUint64(&b[i])))
	}
	return n
}
