package copy_on_write

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaputil"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
	"golang.org/x/time/rate"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// Suffix used for dirtied chunks.
	dirtySuffix = ".dirty"

	// We don't want this to be too big, because we want to prioritize fetching
	// more recently accessed data. If the chan is too big, we may waste time
	// churning through stale data
	eagerFetchChanSize = 100

	// If we access a chunk, we will queue this number of contiguous chunks
	// to be eagerly fetched in the background, anticipating that they will
	// also be accessed
	numChunksToEagerFetch = 4
)

var maxEagerFetchesPerSec = flag.Int("executor.snaploader_max_eager_fetches_per_sec", 1000, "Max number of chunks snaploader can eagerly fetch in the background per second.")
var eagerFetchConcurrency = flag.Int("executor.snaploader_eager_fetch_concurrency", 8, "Max number of goroutines allowed to run concurrently when eagerly fetching chunks.")

// Total number of mmapped bytes. This is atomically updated and backs the
// mapped bytes gauge metric.
var mmappedBytes int64

// COWStore To enable copy-on-write support for a file, it can be split into
// chunks of equal size. Just before a chunk is first written to, the chunk is first
// copied, and the write is then applied to the copy.
//
// A COWStore can be created either by splitting a file into chunks, or loading
// chunks from a directory containing artifacts exported by a COWStore instance.
//
// A COWStore supports concurrent reads/writes
type COWStore struct {
	ctx                context.Context
	env                environment.Env
	remoteInstanceName string

	// Whether the store supports remote fetching/caching of artifacts
	// in addition to local caching
	remoteEnabled bool

	mu sync.RWMutex // Protects chunks and dirty

	// chunks is a mapping of chunk offset to the backing data store
	chunks map[int64]*Mmap
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

	// Concurrency-safe pool of buffers that can be used for copying chunks
	copyBufPool sync.Pool

	eagerFetchChan chan *eagerFetchData
	eagerFetchEg   *errgroup.Group
	quitChan       chan struct{}
}

// NewCOWStore creates a COWStore from the given chunks. The chunks should be
// open initially, and will be closed when calling Close on the returned
// COWStore.
func NewCOWStore(ctx context.Context, env environment.Env, chunks []*Mmap, chunkSizeBytes, totalSizeBytes int64, dataDir string, remoteInstanceName string, remoteEnabled bool) (*COWStore, error) {
	stat, err := os.Stat(dataDir)
	if err != nil {
		return nil, err
	}
	chunkMap := make(map[int64]*Mmap, len(chunks))
	for _, c := range chunks {
		chunkMap[c.Offset] = c
	}

	s := &COWStore{
		ctx:                ctx,
		env:                env,
		remoteInstanceName: remoteInstanceName,
		remoteEnabled:      remoteEnabled,
		chunks:             chunkMap,
		dirty:              make(map[int64]bool, 0),
		dataDir:            dataDir,
		copyBufPool: sync.Pool{
			New: func() any {
				b := make([]byte, chunkSizeBytes)
				return &b
			},
		},
		zeroBuf:        make([]byte, chunkSizeBytes),
		chunkSizeBytes: chunkSizeBytes,
		totalSizeBytes: totalSizeBytes,
		ioBlockSize:    int64(stat.Sys().(*syscall.Stat_t).Blksize),
		eagerFetchChan: make(chan *eagerFetchData, eagerFetchChanSize),
		eagerFetchEg:   &errgroup.Group{},
		quitChan:       make(chan struct{}),
	}

	s.eagerFetchEg.Go(func() error {
		s.eagerFetchChunksInBackground()
		return nil
	})

	return s, nil
}

// GetRelativeOffsetFromChunkStart returns the relative offset from the
// beginning of the chunk
//
// Ex. Let's say there are 100B chunks. For input offset 205, chunkStartOffset
// would be 200, and this would return 5
func (c *COWStore) GetRelativeOffsetFromChunkStart(offset uintptr) uintptr {
	chunkStartOffset := c.chunkStartOffset(int64(offset))
	chunkRelativeAddress := offset - uintptr(chunkStartOffset)
	return chunkRelativeAddress
}

// GetChunkStartAddressAndSize returns the start address of the chunk containing
// the input offset, and the size of the chunk. Note that the returned chunk
// size may not be equal to ChunkSizeBytes() if it's the last chunk in the file.
func (c *COWStore) GetChunkStartAddressAndSize(offset uintptr, write bool) (uintptr, int64, error) {
	chunkStartOffset := c.chunkStartOffset(int64(offset))
	chunkStartAddress, err := c.GetPageAddress(uintptr(chunkStartOffset), write)
	if err != nil {
		return 0, 0, err
	}
	return chunkStartAddress, c.calculateChunkSize(chunkStartOffset), nil
}

// GetPageAddress returns the memory address for the given byte offset into
// the store.
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

	c.eagerFetchNextChunks(chunkStartOffset)

	c.mu.Lock()
	defer c.mu.Unlock()
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
	start, err := chunk.StartAddress()
	if err != nil {
		return 0, status.WrapError(err, "mmap start adddress")
	}
	return start + chunkRelativeAddress, nil
}

// SortedChunks returns all chunks sorted by offset.
func (c *COWStore) SortedChunks() []*Mmap {
	c.mu.RLock()
	chunks := maps.Values(c.chunks)
	c.mu.RUnlock()

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
	if err := checkBounds("read", c.totalSizeBytes, p, off); err != nil {
		return 0, err
	}

	chunkOffset := c.chunkStartOffset(off)
	n := 0

	c.eagerFetchNextChunks(chunkOffset)

	c.mu.RLock()
	defer c.mu.RUnlock()

	for len(p) > 0 {
		chunkRelativeOffset := off % c.chunkSizeBytes
		chunkCalculatedSize := c.calculateChunkSize(chunkOffset)
		readSize := int(chunkCalculatedSize - chunkRelativeOffset)
		if readSize > len(p) {
			readSize = len(p)
		}
		chunk := c.chunks[chunkOffset]
		if chunk == nil {
			// No chunk at this index yet; write all 0s.
			copy(p[:readSize], c.zeroBuf)
		} else {
			// chunkActualSize will be different than chunkCalculatedSize only
			// if this chunk was the last chunk when we called Resize(). This is
			// because Resize() does not actually resize the chunk itself - it
			// is a lazy operation that "virtually" right-pads the COWStore with
			// 0s. So in this case, we need to limit the amount of data
			// requested from the chunk (readSize) to the amount of data that's
			// actually remaining in the chunk (remainingDataSize), then fill
			// the remainder with zeroes.
			//
			// For example, let's say we have the following chunks, before
			// resizing:
			//     [1111]  [1]
			//     c1      c2
			// Now let's say we Resize, increasing the COW's total size by 4
			// bytes. (Bytes between "[]"" are physically present in the chunk,
			// while other bytes are "virtual")
			//     [1111]  [1]000  0
			//     c1      c2      c3
			// Now, the chunkActualSize of c2 will still be 1, while the
			// chunkCalculatedSize will be 4. So when reading from c2, we need
			// to make sure that we zero-pad when reading offset >= 1.

			chunkActualSize, err := chunk.SizeBytes()
			if err != nil {
				return n, err
			}
			// Chunk might have less data available than the calculated size
			// if this was the last chunk when we resized. If so then fill
			// the range from [dataSize, readSize) with 0s.
			dataSize := int64(readSize)
			if remainder := chunkActualSize - chunkRelativeOffset; readSize > int(remainder) {
				dataSize = max(0, remainder)
			}
			if dataSize > 0 {
				if _, err := readFullAt(chunk, p[:dataSize], chunkRelativeOffset); err != nil {
					return n, err
				}
			}
			copy(p[dataSize:readSize], c.zeroBuf)
		}
		n += readSize
		p = p[readSize:]
		off += int64(readSize)
		chunkOffset += c.chunkSizeBytes
	}
	return n, nil
}

func (c *COWStore) WriteAt(p []byte, off int64) (int, error) {
	if err := checkBounds("write", c.totalSizeBytes, p, off); err != nil {
		return 0, err
	}

	chunkOffset := c.chunkStartOffset(off)
	n := 0

	c.eagerFetchNextChunks(chunkOffset)

	c.mu.Lock()
	defer c.mu.Unlock()

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
		chunk := c.chunks[chunkOffset]
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
	// Close background goroutine eagerly fetching chunks
	close(s.quitChan)
	s.eagerFetchEg.Wait()

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

// ChunkName returns the file name containing the data for the given chunk offset.
func ChunkName(chunkOffset int64, dirty bool) string {
	suffix := ""
	if dirty {
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

// Resize resizes the COWStore to the given size, effectively right-padding the
// current store with 0-bytes.
func (s *COWStore) Resize(newSize int64) (oldSize int64, err error) {
	oldSize = s.totalSizeBytes
	if newSize < oldSize {
		return 0, status.InvalidArgumentErrorf("cannot decrease COWStore size (requested %d, currently %d)", newSize, s.totalSizeBytes)
	}
	s.totalSizeBytes = newSize
	return oldSize, nil
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

	b := s.copyBufPool.Get().(*[]byte)
	defer s.copyBufPool.Put(b)

	for off, c := range s.chunks {
		size := s.calculateChunkSize(off)
		copyBuf := (*b)[:size]
		// TODO: skip sparse regions in the chunk?
		if _, err := readFullAt(c, copyBuf, 0); err != nil {
			return status.WrapError(err, "read chunk")
		}
		if _, err := f.WriteAt(copyBuf, off); err != nil {
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

// NOTE: This function should be executed atomically. Callers should manage locking
func (s *COWStore) copyChunkIfNotDirty(chunkStartOffset int64) (err error) {
	if s.dirty[chunkStartOffset] {
		// Chunk is already dirty - no need to copy
		return nil
	}

	dstChunkSize := s.calculateChunkSize(chunkStartOffset)
	src, dst, err := s.initDirtyChunk(chunkStartOffset, dstChunkSize)
	if err != nil {
		return status.WrapError(err, "initialize dirty chunk")
	}

	if src == nil {
		// We had no data at this offset; nothing to copy.
		return nil
	}
	// Once we've created a copy, we no longer need the source chunk.
	defer func() {
		src.Close()
	}()

	// note: the src chunk might be smaller than the dst chunk if we resized
	// and now we're copying the last chunk, since resizing is done lazily.
	srcChunkSize, err := src.SizeBytes()
	if err != nil {
		return err
	}
	if srcChunkSize > dstChunkSize {
		return status.InternalErrorf("chunk source size %d is greater than dest size %d; this is a bug", srcChunkSize, dstChunkSize)
	}

	b := s.copyBufPool.Get().(*[]byte)
	defer s.copyBufPool.Put(b)
	copyBuf := (*b)[:srcChunkSize]

	// TODO: avoid a full read here in the case where the chunk contains holes.
	// Can achieve this by having the Mmap keep around the underlying file
	// descriptor and use fseek (SEEK_DATA) on it.
	if _, err := readFullAt(src, copyBuf, 0); err != nil {
		return status.WrapError(err, "read chunk for copy")
	}
	// Copy to the mmap but skip holes to avoid materializing them as blocks.
	for off := int64(0); off < srcChunkSize; off += s.ioBlockSize {
		blockSize := s.ioBlockSize
		if remainder := srcChunkSize - off; blockSize > remainder {
			blockSize = remainder
		}
		dataBlock := copyBuf[off : off+blockSize]
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
// NOTE: This function should be executed atomically. Callers should manage locking
func (s *COWStore) initDirtyChunk(offset int64, size int64) (ogChunk *Mmap, newChunk *Mmap, err error) {
	path := filepath.Join(s.dataDir, fmt.Sprintf("%d%s", offset, dirtySuffix))
	fd, err := syscall.Open(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, err
	}
	defer syscall.Close(fd)
	if err := syscall.Ftruncate(fd, size); err != nil {
		return nil, nil, err
	}

	ogChunk = s.chunks[offset]
	chunkSource := snaputil.ChunkSourceHole
	if ogChunk != nil {
		if !ogChunk.safeReadMapped() {
			if err := ogChunk.initMap(); err != nil {
				return nil, nil, err
			}
		}
		chunkSource = ogChunk.source
	}
	newChunk, err = NewMmapFd(s.ctx, s.env, s.DataDir(), fd, int(size), offset, chunkSource, s.remoteInstanceName, s.remoteEnabled)
	if err != nil {
		return nil, nil, err
	}

	s.chunks[offset] = newChunk
	s.dirty[offset] = true

	return ogChunk, newChunk, nil
}

type eagerFetchData struct {
	// Chunk offset to eagerly fetch
	offset int64
}

func (s *COWStore) eagerFetchNextChunks(offset int64) {
	currentOffset := offset + s.chunkSizeBytes
	for i := 0; i < numChunksToEagerFetch; i++ {
		s.sendNonBlockingEagerFetch(currentOffset)
		currentOffset += s.chunkSizeBytes
	}
}

func (s *COWStore) sendNonBlockingEagerFetch(offset int64) {
	select {
	case s.eagerFetchChan <- &eagerFetchData{offset: offset}:
	default:
	}
}

func (s *COWStore) eagerFetchChunksInBackground() {
	rateLimiter := rate.NewLimiter(rate.Limit(*maxEagerFetchesPerSec), 1)
	eg := &errgroup.Group{}
	eg.SetLimit(*eagerFetchConcurrency)

	for {
		select {
		case <-s.quitChan:
			_ = eg.Wait()
			return
		case d := <-s.eagerFetchChan:
			if err := rateLimiter.Wait(s.ctx); err != nil {
				if err != nil {
					log.Errorf("COWStore eager fetch rate limiter failed, stopping eager fetches: %s", err)
				}
				_ = eg.Wait()
				return
			}
			eg.Go(func() error {
				err := s.fetchChunk(d.offset)
				if err != nil {
					log.Warningf("COWStore eager fetch chunk failed with: %s", err)
				}
				return nil
			})
		}
	}
}

func (s *COWStore) fetchChunk(offset int64) error {
	s.mu.RLock()
	c := s.chunks[offset]
	s.mu.RUnlock()

	// Skip holes
	if c == nil {
		return nil
	}

	var err error
	if !c.safeReadMapped() {
		err = c.initMap()
	}
	return err
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
func ConvertFileToCOW(ctx context.Context, env environment.Env, filePath string, chunkSizeBytes int64, dataDir string, remoteInstanceName string, remoteEnabled bool) (store *COWStore, err error) {
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
	var chunks []*Mmap
	defer func() {
		// If there's an error, clean up any Store instances we created.
		if err == nil {
			return
		}
		for _, c := range chunks {
			c.Close()
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

	createChunk := func(chunkStartOffset int64) (*Mmap, error) {
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
		return NewMmapFd(ctx, env, dataDir, int(chunkFile.Fd()), int(chunkFileSize), chunkStartOffset, snaputil.ChunkSourceLocalFile, remoteInstanceName, remoteEnabled)
	}

	// TODO: iterate through the file with multiple goroutines
	for chunkStartOffset := int64(0); chunkStartOffset < totalSizeBytes; chunkStartOffset += chunkSizeBytes {
		c, err := createChunk(chunkStartOffset)
		if err != nil {
			return nil, status.WrapError(err, "failed to create chunk")
		}
		if c != nil {
			chunks = append(chunks, c)
		}
	}

	return NewCOWStore(ctx, env, chunks, chunkSizeBytes, totalSizeBytes, dataDir, remoteInstanceName, remoteEnabled)
}

// Mmap uses a memory-mapped file to represent a section of a larger composite
// COW store at a specific offset.
//
// It allows processes to read/write to the file as if it
// was memory, as opposed to having to interact with it via I/O file operations.
//
// Mmap is concurrency safe
type Mmap struct {
	ctx                context.Context
	env                environment.Env
	remoteInstanceName string

	// Whether the store supports remote fetching/caching of artifacts
	// in addition to local caching
	remoteEnabled bool

	Offset  int64
	dataDir string

	// Mutex protects the subsequent fields
	mu         sync.RWMutex
	data       []byte
	source     snaputil.ChunkSource
	closed     bool
	lazyDigest *repb.Digest
}

// NewLazyMmap returns an mmap that is set up only when the file is read or
// written to.
func NewLazyMmap(ctx context.Context, env environment.Env, dataDir string, offset int64, digest *repb.Digest, remoteInstanceName string, remoteEnabled bool) (*Mmap, error) {
	if dataDir == "" {
		return nil, status.FailedPreconditionError("missing dataDir")
	}
	if digest == nil {
		return nil, status.FailedPreconditionError("missing digest")
	}
	return &Mmap{
		ctx:                ctx,
		env:                env,
		remoteInstanceName: remoteInstanceName,
		remoteEnabled:      remoteEnabled,
		Offset:             offset,
		data:               nil,
		source:             snaputil.ChunkSourceUnmapped,
		dataDir:            dataDir,
		lazyDigest:         digest,
	}, nil
}

func NewMmapFd(ctx context.Context, env environment.Env, dataDir string, fd, size int, offset int64, source snaputil.ChunkSource, remoteInstanceName string, remoteEnabled bool) (*Mmap, error) {
	if source == snaputil.ChunkSourceUnmapped {
		return nil, status.InvalidArgumentError("ChunkSourceUnmapped is not a valid source when initializing a chunk from a fd")
	}
	data, err := mmapDataFromFd(fd, size)
	if err != nil {
		return nil, err
	}
	return &Mmap{
		remoteInstanceName: remoteInstanceName,
		remoteEnabled:      remoteEnabled,
		ctx:                ctx,
		env:                env,
		Offset:             offset,
		data:               data,
		source:             source,
		dataDir:            dataDir,
	}, nil
}

// mmapDataFromPath memory maps a file and returns the data
func mmapDataFromPath(path string) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	s, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return mmapDataFromFd(int(f.Fd()), int(s.Size()))
}

// mmapDataFromFd memory maps a file descriptor and returns the data
func mmapDataFromFd(fd, size int) ([]byte, error) {
	data, err := syscall.Mmap(fd, 0, size, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap: %s", err)
	}
	metrics.COWSnapshotMemoryMappedBytes.Set(float64(
		atomic.AddInt64(&mmappedBytes, int64(len(data))),
	))
	return data, nil
}

func (m *Mmap) initMap() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return status.InternalError("store is closed")
	}
	if m.source != snaputil.ChunkSourceUnmapped {
		return nil
	}
	if m.lazyDigest == nil {
		return status.InternalError("cannot initialize chunk without a digest")
	}

	outputPath := filepath.Join(m.dataDir, ChunkName(m.Offset, false /*dirty*/))
	artifactSrc, err := snaputil.GetArtifact(m.ctx, m.env.GetFileCache(), m.env.GetByteStreamClient(), m.remoteEnabled, m.lazyDigest, m.remoteInstanceName, outputPath)
	if err != nil {
		return status.WrapErrorf(err, "fetch snapshot chunk for offset %d digest %s", m.Offset, m.lazyDigest.Hash)
	}

	data, err := mmapDataFromPath(outputPath)
	if err != nil {
		return status.WrapErrorf(err, "create mmap for path %s", outputPath)
	}

	m.data = data
	m.source = artifactSrc
	return nil
}

func (m *Mmap) ReadAt(p []byte, off int64) (n int, err error) {
	if !m.safeReadMapped() {
		if err := m.initMap(); err != nil {
			return 0, err
		}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := checkBounds("read", int64(len(m.data)), p, off); err != nil {
		return 0, err
	}
	copy(p, m.data[int(off):int(off)+len(p)])
	return len(p), nil
}

func (m *Mmap) WriteAt(p []byte, off int64) (n int, err error) {
	if !m.safeReadMapped() {
		if err := m.initMap(); err != nil {
			return 0, err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := checkBounds("write", int64(len(m.data)), p, off); err != nil {
		return 0, err
	}
	m.lazyDigest = nil
	copy(m.data[int(off):int(off)+len(p)], p)
	return len(p), nil
}

func (m *Mmap) Sync() error {
	if !m.safeReadMapped() {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return unix.Msync(m.data, unix.MS_SYNC)
}

func (m *Mmap) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	if m.source == snaputil.ChunkSourceUnmapped {
		return nil
	}
	m.source = snaputil.ChunkSourceUnmapped
	n := len(m.data)
	if err := syscall.Munmap(m.data); err != nil {
		return err
	}
	metrics.COWSnapshotMemoryMappedBytes.Set(float64(
		atomic.AddInt64(&mmappedBytes, int64(-n)),
	))
	return nil
}

func (m *Mmap) SizeBytes() (int64, error) {
	if !m.safeReadMapped() {
		if err := m.initMap(); err != nil {
			return 0, err
		}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return int64(len(m.data)), nil
}

// StartAddress returns the address of the first mapped byte. If this is a lazy
// mmap, calling this func will force an mmap if not already mapped.
func (m *Mmap) StartAddress() (uintptr, error) {
	if !m.safeReadMapped() {
		if err := m.initMap(); err != nil {
			return 0, err
		}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return memoryAddress(m.data), nil
}

func (m *Mmap) safeReadMapped() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.source != snaputil.ChunkSourceUnmapped
}

func (m *Mmap) Mapped() bool {
	return m.safeReadMapped()
}

func (m *Mmap) Source() snaputil.ChunkSource {
	return m.safeReadSource()
}

func (m *Mmap) safeReadSource() snaputil.ChunkSource {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.source
}

func (m *Mmap) safeReadClosed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.closed
}

func (m *Mmap) safeReadLazyDigest() *repb.Digest {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lazyDigest
}

func (m *Mmap) SetDigest(d *repb.Digest) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lazyDigest = d
}

func (m *Mmap) Digest() (*repb.Digest, error) {
	if d := m.safeReadLazyDigest(); d != nil {
		return d, nil
	}

	// Otherwise compute the digest.
	chunkReader, err := interfaces.StoreReader(m)
	if err != nil {
		return nil, err
	}
	d, err := digest.Compute(chunkReader, repb.DigestFunction_BLAKE3)
	if err != nil {
		return nil, err
	}
	m.SetDigest(d)
	return d, nil
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

func readFullAt(r io.ReaderAt, p []byte, off int64) (n int, err error) {
	return io.ReadFull(io.NewSectionReader(r, off, int64(len(p))), p)
}

// memoryAddress returns the memory address of the first byte of a slice
func memoryAddress(s []byte) uintptr {
	return uintptr(unsafe.Pointer(&s[0]))
}

// checkBounds checks whether a read or write operation is safe.
func checkBounds(opName string, storeSize int64, p []byte, off int64) error {
	if off < 0 || off+int64(len(p)) > storeSize {
		return status.InvalidArgumentErrorf("invalid %s at offset 0x%x, length 0x%x", opName, off, len(p))
	}
	return nil
}
