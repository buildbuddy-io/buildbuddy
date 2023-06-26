package blockio

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sys/unix"
)

// Store models a block-level storage system, which is useful as a backend for a
// Network Block Device (NBD) or userfaultfd(2).
type Store interface {
	io.ReaderAt
	io.WriterAt
	io.Closer

	// Sync flushes any buffered pages to the store, if applicable.
	Sync() error

	// Size returns the total addressable size of the store in bytes.
	SizeBytes() (int64, error)
}

// File implements the Store interface using standard file operations.
type File struct{ *os.File }

// NewFile returns a File store for an existing file.
func NewFile(path string) (*File, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	return &File{File: f}, nil
}

func (f *File) SizeBytes() (int64, error) {
	return fileSizeBytes(f.File)
}

// Mmap implements the Store interface using a memory-mapped file.
type Mmap struct {
	data []byte
}

// NewMmap returns an Mmap for an existing file.
func NewMmap(path string) (*Mmap, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	s, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return NewMmapFd(int(f.Fd()), int(s.Size()))
}

func NewMmapFd(fd, size int) (*Mmap, error) {
	data, err := syscall.Mmap(fd, 0, size, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap: %s", err)
	}
	return &Mmap{data: data}, nil
}

func (m *Mmap) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 || int(off)+len(p) > len(m.data) {
		return 0, status.InvalidArgumentErrorf("invalid read at offset 0x%x length 0x%x", off, len(p))
	}
	copy(p, m.data[int(off):int(off)+len(p)])
	return len(p), nil
}

func (m *Mmap) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 || int(off)+len(p) > len(m.data) {
		return 0, status.InvalidArgumentErrorf("invalid write at offset 0x%x length 0x%x", off, len(p))
	}
	copy(m.data[int(off):int(off)+len(p)], p)
	return len(p), nil
}

func (m *Mmap) Sync() error {
	return unix.Msync(m.data, unix.MS_SYNC)
}

func (m *Mmap) Close() error {
	// TODO: prevent double-unmap
	return syscall.Munmap(m.data)
}

func (m *Mmap) SizeBytes() (int64, error) {
	return int64(len(m.data)), nil
}

func fileSizeBytes(f *os.File) (int64, error) {
	s, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return s.Size(), nil
}

// Hole is a disk-efficient representation of a readonly store containing all
// zeroes.
type Hole struct{ Size int64 }

func (e *Hole) ReadAt(p []byte, off int64) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func (e *Hole) WriteAt(p []byte, off int64) (int, error) {
	return 0, status.PermissionDeniedError("Hole is read-only")
}

func (e *Hole) Sync() error {
	return nil
}

func (e *Hole) SizeBytes() (int64, error) {
	return e.Size, nil
}

func (e *Hole) Close() error {
	return nil
}

// COW implements copy-on-write for a Store that has been split into chunks of
// equal size. Just before a chunk is first written to, the chunk is first
// copied, and the write is then applied to the copy.
//
// A COW can be created either by splitting a file into chunks, or loading
// chunks from a directory containing artifacts exported by a COW instance.
type COW struct {
	Chunks []Store
	// Indexes of chunks which have been copied from the original chunks due to
	// writes.
	dirty map[int]bool
	// Dir where all chunk data is stored.
	dataDir string

	// Size of each chunk, except for possibly the last chunk.
	chunkSizeBytes int64
	// Total size in bytes. Note that this cannot be computed by simply
	// multiplying the number of chunks by chunkSizeBytes, because the last
	// chunk is allowed to be shorter than the rest of the chunks.
	totalSizeBytes int64
	// Number of bytes transferred during each IO operation. This is determined
	// by the filesystem on which the chunks are stored.
	ioBlockSize int64

	// Scratch buffer used for copying chunks.
	copyBuf []byte
}

// NewCOW creates a COW from the given chunks. The chunks should be open
// initially, and will be closed when calling Close on the returned COW.
func NewCOW(chunks []Store, chunkSizeBytes, totalSizeBytes int64, dataDir string) (*COW, error) {
	stat, err := os.Stat(dataDir)
	if err != nil {
		return nil, err
	}
	return &COW{
		Chunks:         chunks,
		dirty:          make(map[int]bool, 0),
		dataDir:        dataDir,
		copyBuf:        make([]byte, chunkSizeBytes),
		chunkSizeBytes: chunkSizeBytes,
		totalSizeBytes: totalSizeBytes,
		ioBlockSize:    int64(stat.Sys().(*syscall.Stat_t).Blksize),
	}, nil
}

func (c *COW) ReadAt(p []byte, off int64) (int, error) {
	chunkIdx := int(off / c.chunkSizeBytes)
	n := 0
	for len(p) > 0 {
		chunkRelativeOffset := off % c.chunkSizeBytes
		readSize := int(c.chunkSizeBytes - chunkRelativeOffset)
		if readSize > len(p) {
			readSize = len(p)
		}
		nr, err := readFullAt(c.Chunks[chunkIdx], p[:readSize], chunkRelativeOffset)
		n += nr
		if err != nil {
			return n, err
		}
		p = p[readSize:]
		off += int64(readSize)
		chunkIdx++
	}
	return n, nil
}

func (c *COW) WriteAt(p []byte, off int64) (int, error) {
	chunkIdx := int(off / c.chunkSizeBytes)
	n := 0
	for len(p) > 0 {
		// On each iteration, write to one chunk, first copying the readonly
		// chunk if needed.
		if !c.dirty[chunkIdx] {
			// If we're newly dirtying a chunk, copy.
			if err := c.copyChunk(chunkIdx); err != nil {
				return 0, status.WrapError(err, "failed to copy chunk")
			}
			c.dirty[chunkIdx] = true
		}
		chunkRelativeOffset := (off + int64(n)) % c.chunkSizeBytes
		writeSize := int(c.chunkSizeBytes - chunkRelativeOffset)
		if writeSize > len(p) {
			writeSize = len(p)
		}
		nw, err := c.Chunks[chunkIdx].WriteAt(p[:writeSize], chunkRelativeOffset)
		n += nw
		if err != nil {
			return n, err
		}
		if nw != writeSize {
			return n, io.ErrShortWrite
		}
		p = p[writeSize:]
		chunkIdx++
	}
	return n, nil
}

func (c *COW) Sync() error {
	var lastErr error
	// TODO: maybe parallelize
	for i := range c.Chunks {
		if !c.dirty[i] {
			continue
		}
		if err := c.Chunks[i].Sync(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (s *COW) Close() error {
	var lastErr error
	// TODO: maybe parallelize
	for _, c := range s.Chunks {
		if err := c.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (s *COW) SizeBytes() (int64, error) {
	return s.totalSizeBytes, nil
}

// Dirty returns the chunk indexes of chunks that have been written to (and
// therefore copied).
func (s *COW) Dirty() map[int]bool {
	return s.dirty
}

func (s *COW) copyChunk(index int) (err error) {
	src := s.Chunks[index]
	// Once we've created a copy, we no longer need the source chunk.
	defer src.Close()

	size, err := src.SizeBytes()
	if err != nil {
		return err
	}
	dst, err := s.initDirtyChunk(index, size)
	if err != nil {
		return status.WrapError(err, "initialize dirty chunk")
	}
	s.Chunks[index] = dst

	// Optimization: if copying a hole, the dirty chunk we just initialized
	// should already contain all 0s, so we're done.
	if _, ok := src.(*Hole); ok {
		return nil
	}

	b := s.copyBuf[:size]
	// TODO: avoid a full read here in the case where the chunk contains holes.
	// Can achieve this by having the Mmap keep around the underlying file
	// descriptor and use fseek (SEEK_DATA) on it.
	if _, err := readFullAt(src, b, 0); err != nil {
		return status.WrapError(err, "read chunk for copy")
	}
	// Copy to the mmap but skip holes to avoid materializing them as blocks.
	for off := int64(0); off < size; off += s.ioBlockSize {
		blockSize := s.ioBlockSize
		if remainder := size - off; blockSize > remainder {
			blockSize = remainder
		}
		dataBlock := b[off : off+blockSize]
		if IsEmptyOrAllZero(dataBlock) {
			continue
		}
		if _, err := dst.WriteAt(dataBlock, off); err != nil {
			return status.WrapError(err, "copy data to new chunk")
		}
	}
	return nil
}

// Writes a new dirty chunk containing all 0s for the given chunk index.
func (s *COW) initDirtyChunk(index int, size int64) (Store, error) {
	path := filepath.Join(s.dataDir, fmt.Sprintf("%d.dirty", index*int(s.chunkSizeBytes)))
	fd, err := syscall.Open(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	defer syscall.Close(fd)
	if err := syscall.Ftruncate(fd, size); err != nil {
		return nil, err
	}
	return NewMmapFd(fd, int(size))
}

// ConvertFileToCOW reads a file sequentially, splitting it into fixed size,
// read-only chunks. Any newly created chunks will be written to dataDir. The
// backing files are named according to their starting byte offset (in base 10).
// For example, the chunk at offset 4096 is named "4096".
//
// When chunks are written to, a new copy of the file is created, named as the
// chunk index with ".dirty" appended.
//
// Empty regions ("holes") in the file are respected. For example, if a chunk
// contains no data, then the written chunk file will contain no data blocks,
// and its on-disk representation will only contain metadata.
//
// If an error is returned from this function, the caller should decide what to
// do with any files written to dataDir. Typically the caller should provide an
// empty dataDir and remove the dir and contents if there is an error.
func ConvertFileToCOW(filePath string, chunkSizeBytes int64, dataDir string) (store *COW, err error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	totalSizeBytes := stat.Size()
	fd := int(f.Fd())
	chunks := make([]Store, 0, blockCount(totalSizeBytes, chunkSizeBytes))
	defer func() {
		// If there's an error, clean up any Store instances we created.
		if err == nil {
			return
		}
		for _, b := range chunks {
			b.Close()
		}
	}()

	// Copy buffer (large enough to copy one data block at a time).
	ioBlockSize := int64(stat.Sys().(*syscall.Stat_t).Blksize)
	copyBuf := make([]byte, ioBlockSize)

	// dataOffset points to the start of the current non-empty data block in the
	// file. It is updated every time we consume a data block.
	dataOffset, err := syscall.Seek(fd, 0, unix.SEEK_DATA)
	if err == syscall.ENXIO {
		// No more data
		dataOffset = totalSizeBytes
	} else if err != nil {
		return nil, status.InternalErrorf("initial data seek failed: %s", err)
	}

	createChunkFile := func(chunkStartOffset int64) (store Store, err error) {
		chunkFileSize := chunkSizeBytes
		if remainder := totalSizeBytes - chunkStartOffset; chunkFileSize > remainder {
			chunkFileSize = remainder
		}
		if dataOffset >= chunkStartOffset+chunkFileSize {
			// Chunk contains no data; avoid writing a file.
			return &Hole{Size: chunkFileSize}, nil
		}
		chunkFile, err := os.Create(filepath.Join(dataDir, fmt.Sprint(chunkStartOffset)))
		if err != nil {
			return nil, err
		}
		defer chunkFile.Close()
		if err := chunkFile.Truncate(chunkFileSize); err != nil {
			return nil, err
		}
		endOffset := chunkStartOffset + chunkSizeBytes
		if endOffset > totalSizeBytes {
			endOffset = totalSizeBytes
		}
		for dataOffset < endOffset {
			// Copy the current data block to the output chunk file.
			chunkFileDataOffset := dataOffset - chunkStartOffset
			dataBuf := copyBuf
			if remainder := chunkFileSize - chunkFileDataOffset; remainder < int64(len(dataBuf)) {
				dataBuf = copyBuf[:remainder]
			}
			// TODO: see whether it's faster to mmap the file we're copying
			// from, rather than issuing read() syscalls for each data block
			if _, err := io.ReadFull(f, dataBuf); err != nil {
				return nil, err
			}
			if _, err := chunkFile.WriteAt(dataBuf, chunkFileDataOffset); err != nil {
				return nil, err
			}
			// Seek to the next data block, starting from the end of the data
			// block we just copied.
			dataOffset += int64(len(dataBuf))
			dataOffset, err = syscall.Seek(fd, dataOffset, unix.SEEK_DATA)
			if err == syscall.ENXIO {
				// No more data
				dataOffset = totalSizeBytes
			} else if err != nil {
				return nil, err
			}
		}
		return NewMmapFd(int(chunkFile.Fd()), int(chunkFileSize))
	}

	// TODO: iterate through the file with multiple goroutines
	for chunkStartOffset := int64(0); chunkStartOffset < totalSizeBytes; chunkStartOffset += chunkSizeBytes {
		chunk, err := createChunkFile(chunkStartOffset)
		if err != nil {
			return nil, status.WrapError(err, "failed to create block file")
		}
		chunks = append(chunks, chunk)
	}

	return NewCOW(chunks, chunkSizeBytes, totalSizeBytes, dataDir)
}

func IsEmptyOrAllZero(data []byte) bool {
	n := len(data)
	nRound8 := n & ^0b111
	i := 0
	for ; i < nRound8; i += 8 {
		b := *(*uint64)(unsafe.Pointer(&data[i]))
		if b != 0 {
			return false
		}
	}
	for ; i < n; i++ {
		if data[i] != 0 {
			return false
		}
	}
	return true
}

func blockCount(totalSizeBytes, blockSizeBytes int64) int64 {
	b := totalSizeBytes / blockSizeBytes
	if totalSizeBytes%blockSizeBytes > 0 {
		b++
	}
	return b
}

func readFullAt(r io.ReaderAt, p []byte, off int64) (n int, err error) {
	return io.ReadFull(io.NewSectionReader(r, off, int64(len(p))), p)
}
