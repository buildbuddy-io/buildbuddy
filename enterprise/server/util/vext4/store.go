//go:build linux

// This file contains the Store that serves a built image as a block
// device, and the pool of source files it reads from.

package vext4

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
)

const (
	// maxOpenFiles bounds the source files a Store keeps open between
	// reads. Once over it, the pool closes the idle ones rather than
	// tracking recency, since a guest mostly reads each input once.
	maxOpenFiles = 1024
)

// Source is where a file's contents come from. A Store calls Open
// the first time the guest reads one of the file's blocks, so Open may be
// slow, as it is when the file has to be fetched from a remote cache first.
// The Store keeps the returned file open for later reads and closes it
// itself, when the Store is closed or when too many files are open and
// this one is idle. After an idle close, the next read calls Open again.
type Source interface {
	Open(ctx context.Context) (*os.File, error)
}

// Store serves an image built by Build or BuildTree as a block device.
// Reads of a file's data blocks come from its Source unless the guest has
// written the block, in which case they come from the image file like
// every other block.
type Store struct {
	image *os.File
	size  int64
	// runs are the virtual runs, the ranges of image blocks that stand for
	// source file contents, sorted by image block and non-overlapping.
	runs    []virtualRun
	sources *sourcePool
	// dirty has a bit per image block, set once the guest writes the block.
	// Only bits for virtual blocks are consulted.
	dirty bitset
	// cowMu serializes writes to virtual blocks, since a partial write first
	// copies the source block into the image.
	cowMu sync.Mutex

	files, dirs, symlinks int
	fileBytes             int64
	virtualBlocks         uint64
	dirtyBlocks           atomic.Uint64
}

func newStore(ctx context.Context, image *os.File, l *blockLayout, inodes inodeList) *Store {
	s := &Store{
		image:         image,
		size:          l.sizeBytes(),
		runs:          l.runs,
		sources:       newSourcePool(ctx, l.files),
		dirty:         newBitset(l.totalBlocks),
		virtualBlocks: l.virtualBlocks,
	}
	for _, in := range inodes {
		if in == nil {
			continue
		}
		switch in.kind {
		case kindFile:
			s.files++
			s.fileBytes += in.size
		case kindDir:
			s.dirs++
		case kindSymlink:
			s.symlinks++
		}
	}
	return s
}

// SizeBytes implements vbd.BlockDevice.
func (s *Store) SizeBytes() (int64, error) {
	return s.size, nil
}

// ReadAt implements io.ReaderAt. The request is served as a series of
// pieces, each read from one place. Blocks outside any virtual run, and
// virtual blocks the guest has written, come from the image file. Clean
// virtual blocks come from the source file they stand for.
func (s *Store) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off > s.size || int64(len(p)) > s.size-off {
		return 0, fmt.Errorf("read [%d, %d) outside image of size %d", off, off+int64(len(p)), s.size)
	}
	n := 0
	for n < len(p) {
		// cur is the image offset of p[n], and blk the block it falls in.
		cur := off + int64(n)
		blk := uint64(cur / blockSize)
		i, virtual := s.findRun(blk)
		var m int
		if !virtual {
			m = s.nonVirtualLen(i, cur, len(p)-n)
			if _, err := s.image.ReadAt(p[n:n+m], cur); err != nil {
				return n, fmt.Errorf("read image: %w", err)
			}
		} else {
			// Neighbouring blocks of the run that are all dirty, or all
			// clean, are read together.
			run := &s.runs[i]
			dirty := s.dirty.test(blk)
			m = s.sameStateLen(run, blk, dirty, cur, len(p)-n)
			if dirty {
				if _, err := s.image.ReadAt(p[n:n+m], cur); err != nil {
					return n, fmt.Errorf("read image: %w", err)
				}
			} else {
				// blk-run.imageBlock is how far into the run the block is, and
				// run.fileBlock is where the run starts in the file.
				// cur%blockSize keeps the position within the block.
				fileOff := int64(blk-run.imageBlock+run.fileBlock)*blockSize + cur%blockSize
				if err := s.sources.pread(run.file, p[n:n+m], fileOff); err != nil {
					return n, err
				}
			}
		}
		n += m
	}
	return n, nil
}

// WriteAt implements io.WriterAt. Writes always go to the image file, rather
// than to the source files, which are shared with the file cache. A partial
// write to a virtual block that the guest has not written before first
// copies the block from the source file, so that the rest of the block
// keeps its contents.
func (s *Store) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 || off > s.size || int64(len(p)) > s.size-off {
		return 0, fmt.Errorf("write [%d, %d) outside image of size %d", off, off+int64(len(p)), s.size)
	}
	n := 0
	for n < len(p) {
		cur := off + int64(n)
		blk := uint64(cur / blockSize)
		i, virtual := s.findRun(blk)
		var m int
		if !virtual {
			m = s.nonVirtualLen(i, cur, len(p)-n)
			if _, err := s.image.WriteAt(p[n:n+m], cur); err != nil {
				return n, fmt.Errorf("write image: %w", err)
			}
		} else {
			run := &s.runs[i]
			end := int64(run.imageBlock+run.length) * blockSize
			m = int(min(int64(len(p)-n), end-cur))
			if err := s.writeVirtual(run, p[n:n+m], cur); err != nil {
				return n, err
			}
		}
		n += m
	}
	return n, nil
}

// Stats counts the store's tree, the files the guest has read, the virtual
// blocks it has overwritten, and the bytes read from sources.
func (s *Store) Stats() Stats {
	return Stats{
		Files:           s.files,
		Directories:     s.dirs,
		Symlinks:        s.symlinks,
		FileBytes:       s.fileBytes,
		VirtualBytes:    int64(s.virtualBlocks) * blockSize,
		FilesRead:       int(s.sources.filesRead.Load()),
		ReadFileBytes:   s.sources.readFileBytes.Load(),
		DirtyBytes:      int64(s.dirtyBlocks.Load()) * blockSize,
		SourceReadBytes: int64(s.sources.readBytes.Load()),
	}
}

// Close closes the image and source files. It does not remove the image.
func (s *Store) Close() error {
	s.sources.closeAll()
	return s.image.Close()
}

// findRun returns the index of the run containing blk and true, or the
// index of the first run after blk and false.
func (s *Store) findRun(blk uint64) (int, bool) {
	i := sort.Search(len(s.runs), func(i int) bool {
		return s.runs[i].imageBlock+s.runs[i].length > blk
	})
	return i, i < len(s.runs) && s.runs[i].imageBlock <= blk
}

// nonVirtualLen returns how many bytes, at most limit, can be served from the
// image file starting at cur, which lies outside any virtual run. i is the
// index of the first run after cur, or len(runs) if there is none.
func (s *Store) nonVirtualLen(i int, cur int64, limit int) int {
	end := s.size
	if i < len(s.runs) {
		end = int64(s.runs[i].imageBlock) * blockSize
	}
	return int(min(int64(limit), end-cur))
}

// sameStateLen returns the number of bytes, at most limit, starting at off
// within run whose blocks all have the given dirty state.
func (s *Store) sameStateLen(run *virtualRun, blk uint64, dirty bool, off int64, limit int) int {
	end := int64(run.imageBlock+run.length) * blockSize
	for b := blk + 1; int64(b)*blockSize < end && int64(b)*blockSize < off+int64(limit); b++ {
		if s.dirty.test(b) != dirty {
			end = int64(b) * blockSize
			break
		}
	}
	return int(min(int64(limit), end-off))
}

// writeVirtual writes p, which lies within run, to the image and marks its
// blocks dirty.
func (s *Store) writeVirtual(run *virtualRun, p []byte, off int64) error {
	s.cowMu.Lock()
	defer s.cowMu.Unlock()
	first := uint64(off / blockSize)
	last := uint64((off + int64(len(p)) - 1) / blockSize)
	// Copy the blocks at either end of the write from the source file when
	// the write only covers part of them and they have not been copied yet.
	if off%blockSize != 0 || off+int64(len(p)) < int64(first+1)*blockSize {
		if !s.dirty.test(first) {
			if err := s.copyBlock(run, first); err != nil {
				return err
			}
		}
	}
	if last != first && (off+int64(len(p)))%blockSize != 0 {
		if !s.dirty.test(last) {
			if err := s.copyBlock(run, last); err != nil {
				return err
			}
		}
	}
	if _, err := s.image.WriteAt(p, off); err != nil {
		return fmt.Errorf("write image: %w", err)
	}
	for blk := first; blk <= last; blk++ {
		if s.dirty.set(blk) {
			s.dirtyBlocks.Add(1)
		}
	}
	return nil
}

// copyBlock copies a block of run from the source file into the image.
func (s *Store) copyBlock(run *virtualRun, blk uint64) error {
	buf := make([]byte, blockSize)
	if err := s.sources.pread(run.file, buf, int64(blk-run.imageBlock+run.fileBlock)*blockSize); err != nil {
		return err
	}
	if _, err := s.image.WriteAt(buf, int64(blk)*blockSize); err != nil {
		return fmt.Errorf("copy block to image: %w", err)
	}
	return nil
}

// virtualRun records that image blocks [imageBlock, imageBlock+length)
// stand for the contents of file, starting at the file's block fileBlock.
// layout makes one for each extent of each regular file.
type virtualRun struct {
	imageBlock uint64
	length     uint64
	file       *sourceFile
	fileBlock  uint64
}

// sourcePool reads the sources of the image's virtual blocks. It opens a file on first use
// and keeps it open, since a guest usually reads a file in several
// requests, and closes the idle ones once more than maxOpenFiles are open.
type sourcePool struct {
	// ctx is passed to each Source when it is opened.
	ctx   context.Context
	files []*sourceFile
	open  atomic.Int64
	// filesRead and readFileBytes count the files read at least once and
	// their total size.
	filesRead     atomic.Int64
	readFileBytes atomic.Int64
	readBytes     atomic.Uint64
}

func newSourcePool(ctx context.Context, files []*sourceFile) *sourcePool {
	return &sourcePool{ctx: ctx, files: files}
}

// pread fills p from the file starting at off, zero-filling past the end
// of the file. Image blocks past a file's last byte must read as zeros,
// since the guest kernel may expose them through mmap.
func (sp *sourcePool) pread(f *sourceFile, p []byte, off int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.file == nil {
		file, err := f.source.Open(sp.ctx)
		if err != nil {
			return err
		}
		f.file = file
		sp.open.Add(1)
		if !f.read {
			f.read = true
			sp.filesRead.Add(1)
			sp.readFileBytes.Add(f.size)
		}
	}
	n, err := f.file.ReadAt(p, off)
	if err != nil && err != io.EOF {
		return err
	}
	clear(p[n:])
	sp.readBytes.Add(uint64(len(p)))
	if sp.open.Load() > maxOpenFiles {
		sp.closeIdle()
	}
	return nil
}

// closeIdle closes every open file that is not being read.
func (sp *sourcePool) closeIdle() {
	for _, f := range sp.files {
		if !f.mu.TryLock() {
			continue
		}
		sp.closeLocked(f)
		f.mu.Unlock()
	}
}

// closeAll closes every open file, once reads in progress finish.
func (sp *sourcePool) closeAll() {
	for _, f := range sp.files {
		f.mu.Lock()
		sp.closeLocked(f)
		f.mu.Unlock()
	}
}

// closeLocked closes f, whose mu the caller holds, if it is open.
func (sp *sourcePool) closeLocked(f *sourceFile) {
	if f.file != nil {
		f.file.Close()
		f.file = nil
		sp.open.Add(-1)
	}
}

// sourceFile is a Source and its open descriptor, if any.
type sourceFile struct {
	source Source
	size   int64
	// mu is held while the file is opened and read, so that closeIdle does
	// not close a descriptor in use.
	mu   sync.Mutex
	file *os.File
	// read is whether the file has been opened at least once.
	read bool
}

func newSourceFile(source Source, size int64) *sourceFile {
	return &sourceFile{source: source, size: size}
}
