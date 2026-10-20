//go:build linux

// This file contains functions that take the metadata-only ext4 image
// layout (blockLayout) and write it to an image file, without writing the
// file data.

package vext4

import (
	"crypto/rand"
	"fmt"
	"math/bits"
	"os"
	"time"
)

// write writes the metadata of the image laid out by l. The metadata is
// the superblocks and group descriptors, the bitmaps, the inode tables, and
// the blocks of directories, long symlink targets and extent indexes.
// Everything else stays a hole.
func write(f *os.File, l *blockLayout, inodes inodeList) error {
	now := uint32(time.Now().Unix())
	if err := writeSuperblocks(f, l, inodes, now); err != nil {
		return err
	}
	if err := writeBitmaps(f, l, inodes); err != nil {
		return err
	}
	if err := writeInodeTables(f, l, inodes, now); err != nil {
		return err
	}
	return writeInodeData(f, inodes)
}

// writeSuperblocks writes the superblock and group descriptor table to
// group 0 and to every group that holds a backup.
func writeSuperblocks(f *os.File, l *blockLayout, inodes inodeList, now uint32) error {
	var uuid, hashSeed [16]byte
	if _, err := rand.Read(uuid[:]); err != nil {
		return err
	}
	if _, err := rand.Read(hashSeed[:]); err != nil {
		return err
	}
	usedInodes := uint64(len(inodes))
	totalInodes := l.groups * l.ipg
	sb := encodeSuperblock(superblock{
		inodes:         uint32(totalInodes),
		blocks:         uint32(l.totalBlocks),
		freeBlocks:     uint32(l.freeBlocks()),
		freeInodes:     uint32(totalInodes - usedInodes),
		inodesPerGroup: uint32(l.ipg),
		// All groups' metadata is packed together, so declare one flex
		// group spanning the whole image.
		logGroupsPerFlex: uint8(max(1, bits.Len64(l.groups-1))),
		uuid:             uuid,
		hashSeed:         hashSeed,
		now:              now,
	})
	usedDirs := make([]uint16, l.groups)
	for _, in := range inodes {
		if in != nil && in.kind == kindDir {
			usedDirs[(uint64(in.num)-1)/l.ipg]++
		}
	}
	gdt := make([]byte, l.gdtBlocks*blockSize)
	for g := range l.groups {
		encodeGroupDesc(gdt[g*descSize:], groupDesc{
			blockBitmap: l.blockBitmap[g],
			inodeBitmap: l.inodeBitmap[g],
			inodeTable:  l.inodeTable[g],
			freeBlocks:  uint16(blocksPerGroup - l.groupUsed(g).count()),
			freeInodes:  uint16(l.ipg - l.inodesUsedInGroup(usedInodes, g)),
			usedDirs:    usedDirs[g],
		})
	}
	for g := range l.groups {
		if !hasSuper(g) {
			continue
		}
		le.PutUint16(sb[sbBlockGroupNr:], uint16(g))
		off := int64(g*blocksPerGroup) * blockSize
		if g == 0 {
			off = superblockOffset
		}
		if _, err := f.WriteAt(sb, off); err != nil {
			return fmt.Errorf("write superblock: %w", err)
		}
		if _, err := f.WriteAt(gdt, int64(g*blocksPerGroup+1)*blockSize); err != nil {
			return fmt.Errorf("write group descriptors: %w", err)
		}
	}
	return nil
}

// writeBitmaps writes each group's block bitmap and inode bitmap. The block
// bitmap is the group's part of the layout's used set, and the inode bitmap
// marks the group's used inodes, which are a prefix of its inode table
// since inodes are numbered in walk order.
func writeBitmaps(f *os.File, l *blockLayout, inodes inodeList) error {
	usedInodes := uint64(len(inodes))
	buf := make([]byte, blockSize)
	for g := range l.groups {
		encodeBlockBitmap(buf, l.groupUsed(g))
		if _, err := f.WriteAt(buf, int64(l.blockBitmap[g])*blockSize); err != nil {
			return fmt.Errorf("write block bitmap: %w", err)
		}
		encodeInodeBitmap(buf, l.inodesUsedInGroup(usedInodes, g), l.ipg)
		if _, err := f.WriteAt(buf, int64(l.inodeBitmap[g])*blockSize); err != nil {
			return fmt.Errorf("write inode bitmap: %w", err)
		}
	}
	return nil
}

// writeInodeTables writes the used prefix of each group's inode table.
// Reserved inodes are left zero, which marks them unused in the table
// while the bitmap keeps them from being handed out.
func writeInodeTables(f *os.File, l *blockLayout, inodes inodeList, now uint32) error {
	usedInodes := uint64(len(inodes))
	for g := range l.groups {
		used := l.inodesUsedInGroup(usedInodes, g)
		if used == 0 {
			break
		}
		table := make([]byte, used*inodeSize)
		for i := range used {
			in := inodes[g*l.ipg+i]
			if in == nil {
				continue
			}
			if err := encodeInode(table[i*inodeSize:(i+1)*inodeSize], in, now); err != nil {
				return err
			}
		}
		if _, err := f.WriteAt(table, int64(l.inodeTable[g])*blockSize); err != nil {
			return fmt.Errorf("write inode table: %w", err)
		}
	}
	return nil
}

// writeInodeData writes the blocks that hold directory entries and long
// symlink targets, and the extent index leaves of inodes with many extents.
func writeInodeData(f *os.File, inodes inodeList) error {
	for _, in := range inodes {
		if in == nil {
			continue
		}
		for i, leaf := range in.leaves {
			buf := make([]byte, blockSize)
			extents := in.extents[i*leafExtents:]
			encodeExtentLeaf(buf, extents[:min(len(extents), leafExtents)])
			if _, err := f.WriteAt(buf, int64(leaf)*blockSize); err != nil {
				return fmt.Errorf("write extent leaf: %w", err)
			}
		}
		var data []byte
		switch {
		case in.kind == kindDir:
			data = encodeDirectory(in)
		case in.kind == kindSymlink && !in.isFastSymlink():
			data = make([]byte, blockSize)
			copy(data, in.target)
		default:
			continue
		}
		for _, e := range in.extents {
			chunk := data[e.logical*blockSize : (e.logical+e.length)*blockSize]
			if _, err := f.WriteAt(chunk, int64(e.block)*blockSize); err != nil {
				return fmt.Errorf("write inode %d data: %w", in.num, err)
			}
		}
	}
	return nil
}
