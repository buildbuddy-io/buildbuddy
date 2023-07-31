package blockio

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"unsafe"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/exp/maps"
	"golang.org/x/sys/unix"
)

const (
	// Suffix used for dirtied chunks.
	dirtySuffix = ".dirty"
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

// Mmap implements the Store interface using a memory-mapped file. This allows processes to read/write to the file as if it
// was memory, as opposed to having to interact with it via I/O file operations.
type Mmap struct {
	data   []byte
	mapped bool
	closed bool
	path   string
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

// NewLazyMmap returns an mmap that is set up only when the file is read or
// written to.
func NewLazyMmap(path string) (*Mmap, error) {
	if path == "" {
		return nil, status.FailedPreconditionError("missing path")
	}
	return &Mmap{data: nil, mapped: false, path: path}, nil
}

func NewMmapFd(fd, size int) (*Mmap, error) {
	data, err := syscall.Mmap(fd, 0, size, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap: %s", err)
	}
	return &Mmap{data: data, mapped: true}, nil
}

func (m *Mmap) initMap() error {
	if m.closed {
		return status.InternalError("store is closed")
	}
	if m.mapped {
		return status.InternalError("already mapped")
	}
	if m.path == "" {
		return status.InternalError("missing file path")
	}
	f, err := os.OpenFile(m.path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	s, err := f.Stat()
	if err != nil {
		return err
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(s.Size()), syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return status.InternalErrorf("mmap: %s", err)
	}
	m.data = data
	m.mapped = true
	m.path = ""
	return nil
}

func (m *Mmap) ReadAt(p []byte, off int64) (n int, err error) {
	if !m.mapped {
		if err := m.initMap(); err != nil {
			return 0, err
		}
	}
	if off < 0 || int(off)+len(p) > len(m.data) {
		return 0, status.InvalidArgumentErrorf("invalid read at offset 0x%x length 0x%x", off, len(p))
	}
	copy(p, m.data[int(off):int(off)+len(p)])
	return len(p), nil
}

func (m *Mmap) WriteAt(p []byte, off int64) (n int, err error) {
	if !m.mapped {
		if err := m.initMap(); err != nil {
			return 0, err
		}
	}
	if off < 0 || int(off)+len(p) > len(m.data) {
		return 0, status.InvalidArgumentErrorf("invalid write at offset 0x%x length 0x%x", off, len(p))
	}
	copy(m.data[int(off):int(off)+len(p)], p)
	return len(p), nil
}

func (m *Mmap) Sync() error {
	if !m.mapped {
		return nil
	}
	return unix.Msync(m.data, unix.MS_SYNC)
}

func (m *Mmap) Close() error {
	m.closed = true
	if !m.mapped {
		return nil
	}
	m.mapped = false
	return syscall.Munmap(m.data)
}

func (m *Mmap) SizeBytes() (int64, error) {
	if !m.mapped {
		if err := m.initMap(); err != nil {
			return 0, err
		}
	}
	return int64(len(m.data)), nil
}

// StartAddress returns the address of the first mapped byte. If this is a lazy
// mmap, calling this func will force an mmap if not already mapped.
func (m *Mmap) StartAddress() (uintptr, error) {
	if !m.mapped {
		if err := m.initMap(); err != nil {
			return 0, err
		}
	}
	return memoryAddress(m.data), nil
}

func fileSizeBytes(f *os.File) (int64, error) {
	s, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return s.Size(), nil
}

// Chunk represents a section of a larger composite store at a given offset.
type Chunk struct {
	Store
	Offset int64
}

// COWStore implements copy-on-write for a Store that has been split into chunks
// of equal size. Just before a chunk is first written to, the chunk is first
// copied, and the write is then applied to the copy.
//
// A COWStore can be created either by splitting a file into chunks, or loading
// chunks from a directory containing artifacts exported by a COWStore instance.
type COWStore struct {
	mu sync.RWMutex

	// Chunks is a mapping of chunk offset to Store implementation.
	chunks map[int64]*Chunk
	// Indexes of chunks which have been copied from the original chunks due to
	// writes.
	dirty map[int64]bool
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

	// Buffer that is `chunkSize` bytes in length and contains all zeroes.
	// This is used to serve memory page requests for holes.
	zeroBuf []byte

	// Scratch buffer used for copying chunks.
	copyBuf []byte
}

// NewCOWStore creates a COWStore from the given chunks. The chunks should be
// open initially, and will be closed when calling Close on the returned
// COWStore.
func NewCOWStore(chunks []*Chunk, chunkSizeBytes, totalSizeBytes int64, dataDir string) (*COWStore, error) {
	stat, err := os.Stat(dataDir)
	if err != nil {
		return nil, err
	}
	chunkMap := make(map[int64]*Chunk, len(chunks))
	for _, c := range chunks {
		chunkMap[c.Offset] = c
	}
	return &COWStore{
		chunks:         chunkMap,
		dirty:          make(map[int64]bool, 0),
		dataDir:        dataDir,
		copyBuf:        make([]byte, chunkSizeBytes),
		zeroBuf:        make([]byte, chunkSizeBytes),
		chunkSizeBytes: chunkSizeBytes,
		totalSizeBytes: totalSizeBytes,
		ioBlockSize:    int64(stat.Sys().(*syscall.Stat_t).Blksize),
	}, nil
}

// GetPageAddress returns the memory address for the given byte offset into the store.
//
// This memory address can be used to handle a page fault with userfaultfd.
//
// If reading a lazily mmapped chunk, this will cause the chunk to be mmapped
// so that the returned address is valid.
//
// If write is set to true, and the page is not dirty, then a copy is first
// performed so that the returned chunk can be written to without modifying
// readonly chunks.
func (c *COWStore) GetPageAddress(offset uintptr, write bool) (uintptr, error) {
	chunkStartOffset := c.chunkStartOffset(int64(offset))
	chunkRelativeAddress := offset - uintptr(chunkStartOffset)
	if write {
		if err := c.copyChunkIfNotDirty(chunkStartOffset); err != nil {
			return 0, status.WrapError(err, "copy chunk")
		}
	}
	chunk := c.chunks[chunkStartOffset]
	if chunk == nil {
		// No data (yet); map into our static zero-filled buf. Note that this
		// can only happen for reads, since for writes we call copyChunkIfNotDirty above.
		return memoryAddress(c.zeroBuf) + chunkRelativeAddress, nil
	}
	// Non-empty chunk; initialize the lazy mmap (if applicable) and return
	// the offset relative to the memory start address.
	mm, ok := chunk.Store.(*Mmap)
	if !ok {
		return 0, status.InternalErrorf("failed to map page address: only supported for Mmap chunk store type (chunk is of type %T)", chunk)
	}
	start, err := mm.StartAddress()
	if err != nil {
		return 0, status.WrapError(err, "mmap start adddress")
	}
	return start + chunkRelativeAddress, nil
}

// Chunks returns all chunks sorted by offset.
func (c *COWStore) Chunks() []*Chunk {
	chunks := maps.Values(c.chunks)
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].Offset < chunks[j].Offset
	})
	return chunks
}

// chunkStartOffset returns the chunk start offset for an offset within the
// store.
func (c *COWStore) chunkStartOffset(off int64) int64 {
	return (off / c.chunkSizeBytes) * c.chunkSizeBytes
}

func (c *COWStore) ReadAt(p []byte, off int64) (int, error) {
	chunkOffset := c.chunkStartOffset(off)
	n := 0
	for len(p) > 0 {
		chunkRelativeOffset := off % c.chunkSizeBytes
		readSize := int(c.chunkSizeBytes - chunkRelativeOffset)
		if readSize > len(p) {
			readSize = len(p)
		}
		chunk := c.chunks[chunkOffset]
		if chunk == nil {
			// No chunk at this index yet; write all 0s.
			for i := range p[:readSize] {
				p[i] = 0
			}
		} else {
			if _, err := readFullAt(chunk, p[:readSize], chunkRelativeOffset); err != nil {
				return n, err
			}
		}
		n += readSize
		p = p[readSize:]
		off += int64(readSize)
		chunkOffset += c.chunkSizeBytes
	}
	return n, nil
}

func (c *COWStore) WriteAt(p []byte, off int64) (int, error) {
	chunkOffset := c.chunkStartOffset(off)
	n := 0
	for len(p) > 0 {
		// On each iteration, write to one chunk, first copying the readonly
		// chunk if needed.
		if err := c.copyChunkIfNotDirty(chunkOffset); err != nil {
			return 0, status.WrapError(err, "failed to copy chunk")
		}

		chunkRelativeOffset := (off + int64(n)) % c.chunkSizeBytes
		writeSize := int(c.chunkSizeBytes - chunkRelativeOffset)
		if writeSize > len(p) {
			writeSize = len(p)
		}
		c.mu.RLock()
		chunk := c.chunks[chunkOffset]
		c.mu.RUnlock()
		nw, err := chunk.WriteAt(p[:writeSize], chunkRelativeOffset)
		n += nw
		if err != nil {
			return n, err
		}
		if nw != writeSize {
			return n, io.ErrShortWrite
		}
		p = p[writeSize:]
		chunkOffset += c.chunkSizeBytes
	}
	return n, nil
}

func (c *COWStore) Sync() error {
	var lastErr error
	// TODO: maybe parallelize
	for offset, chunk := range c.chunks {
		if !c.Dirty(offset) {
			continue
		}
		if err := chunk.Sync(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (s *COWStore) Close() error {
	var lastErr error
	// TODO: maybe parallelize
	for _, c := range s.chunks {
		if err := c.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (s *COWStore) SizeBytes() (int64, error) {
	return s.totalSizeBytes, nil
}

// Dirty returns whether the chunk at the given offset is dirty.
func (s *COWStore) Dirty(chunkOffset int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dirty[chunkOffset]
}

// ChunkName returns the data file name for the given chunk offset.
func (s *COWStore) ChunkName(chunkOffset int64) string {
	suffix := ""
	if s.Dirty(chunkOffset) {
		suffix = dirtySuffix
	}
	return fmt.Sprintf("%d%s", chunkOffset, suffix)
}

func (s *COWStore) DataDir() string {
	return s.dataDir
}

func (s *COWStore) ChunkSizeBytes() int64 {
	return s.chunkSizeBytes
}

// WriteFile creates a new file at the given path and writes all contents to the
// file.
func (s *COWStore) WriteFile(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return status.WrapError(err, "create")
	}
	if err := f.Truncate(s.totalSizeBytes); err != nil {
		return status.WrapError(err, "truncate")
	}
	for off, c := range s.chunks {
		size := s.calculateChunkSize(off)
		b := s.copyBuf[:size]
		// TODO: skip sparse regions in the chunk?
		if _, err := readFullAt(c, b, 0); err != nil {
			return status.WrapError(err, "read chunk")
		}
		if _, err := f.WriteAt(b, off); err != nil {
			return status.WrapError(err, "write chunk")
		}
	}
	return nil
}

func (s *COWStore) calculateChunkSize(startOffset int64) int64 {
	size := s.chunkSizeBytes
	if remainder := s.totalSizeBytes - startOffset; size > remainder {
		return remainder
	}
	return size
}

func (s *COWStore) copyChunkIfNotDirty(chunkStartOffset int64) (err error) {
	if s.Dirty(chunkStartOffset) {
		// Chunk is already dirty - no need to copy
		return nil
	}

	size := s.calculateChunkSize(chunkStartOffset)
	dst, err := s.initDirtyChunk(chunkStartOffset, size)
	if err != nil {
		return status.WrapError(err, "initialize dirty chunk")
	}

	s.mu.Lock()
	src := s.chunks[chunkStartOffset]
	s.chunks[chunkStartOffset] = dst
	s.dirty[chunkStartOffset] = true
	s.mu.Unlock()

	if src == nil {
		// We had no data at this offset; nothing to copy.
		return nil
	}
	// Once we've created a copy, we no longer need the source chunk.
	defer func() {
		if src != nil {
			src.Close()
		}
	}()

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
func (s *COWStore) initDirtyChunk(offset int64, size int64) (*Chunk, error) {
	path := filepath.Join(s.dataDir, fmt.Sprintf("%d%s", offset, dirtySuffix))
	fd, err := syscall.Open(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	defer syscall.Close(fd)
	if err := syscall.Ftruncate(fd, size); err != nil {
		return nil, err
	}
	store, err := NewMmapFd(fd, int(size))
	if err != nil {
		return nil, err
	}
	return &Chunk{
		Offset: offset,
		Store:  store,
	}, nil
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
func ConvertFileToCOW(filePath string, chunkSizeBytes int64, dataDir string) (store *COWStore, err error) {
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
	var chunks []*Chunk
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

	createStoreForChunk := func(chunkStartOffset int64) (Store, error) {
		chunkFileSize := chunkSizeBytes
		if remainder := totalSizeBytes - chunkStartOffset; chunkFileSize > remainder {
			chunkFileSize = remainder
		}
		if dataOffset >= chunkStartOffset+chunkFileSize {
			// Chunk contains no data; avoid writing a file.
			return nil, nil
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
		store, err := createStoreForChunk(chunkStartOffset)
		if err != nil {
			return nil, status.WrapError(err, "failed to create chunk")
		}
		if store != nil {
			chunk := &Chunk{Offset: chunkStartOffset, Store: store}
			chunks = append(chunks, chunk)
		}
	}

	return NewCOWStore(chunks, chunkSizeBytes, totalSizeBytes, dataDir)
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

type storeReader struct {
	store Store
	sr    *io.SectionReader
}

// Reader returns an io.Reader that reads all bytes from the given store,
// starting at offset 0 and ending at SizeBytes.
func Reader(store Store) io.Reader {
	return &storeReader{store: store}
}

func (r *storeReader) Read(p []byte) (n int, err error) {
	if r.sr == nil {
		size, err := r.store.SizeBytes()
		if err != nil {
			return 0, err
		}
		r.sr = io.NewSectionReader(r.store, 0, size)
		r.store = nil
	}
	return r.sr.Read(p)
}

// memoryAddress returns the memory address of the first byte of a slice
func memoryAddress(s []byte) uintptr {
	return uintptr(unsafe.Pointer(&s[0]))
}
