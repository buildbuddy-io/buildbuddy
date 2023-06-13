package blockio

import (
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/boljen/go-bitmap"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sys/unix"
)

const (
	// The default block size to use in Overlay when copying up from the base
	// layer to the top layer.
	defaultOverlayBlockSizeBytes = 4096
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
	file *os.File
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
	data, err := syscall.Mmap(int(f.Fd()), 0, int(s.Size()), syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap: %s", err)
	}
	return &Mmap{file: f, data: data}, nil
}

func (m *Mmap) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 || int(off)+len(p) > len(m.data) {
		return 0, status.InvalidArgumentErrorf("invalid offset %x", off)
	}
	copy(p, m.data[int(off):int(off)+len(p)])
	return len(p), nil
}

func (m *Mmap) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 || int(off)+len(p) > len(m.data) {
		return 0, status.InvalidArgumentErrorf("invalid offset %x", off)
	}
	copy(m.data[int(off):int(off)+len(p)], p)
	return len(p), nil
}

func (m *Mmap) Sync() error {
	return unix.Msync(m.data, unix.MS_SYNC)
}

func (m *Mmap) Close() error {
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

// Overlay implements copy-on-write for stores, similar to overlayfs but for
// block-level IO instead of file IO.
//
// It currently supports one immutable base layer and one mutable copy-up layer.
type Overlay struct {
	// Immutable base layer.
	base Store
	// Mutable copy-up layer.
	top Store
	// Total size of the store in bytes. Both layers should have the same size.
	sizeBytes int64
	// Block size used when copying up.
	blockSizeBytes int64
	// Dirty blocks.
	dirty bitmap.Bitmap
	// Scratch buffer used for copying blocks from the base layer to the top.
	copyUpBuf []byte
}

type OverlayOpts struct {
	// BlockSizeBytes is the block size to use when copying up from the base
	// layer to the top layer. Smaller sizes are more space efficient but
	// require more memory to keep track of dirty pages. If unspecified, a
	// reasonable default is used.
	BlockSizeBytes int64
}

// NewOverlay returns a new Overlay for the given layers. The two layers must be
// the same size, or an error is returned.
//
// If the top store is non-empty (i.e. restored from a previous Overlay
// instance), then dirty must be non-nil and contain the data exported from the
// DirtyBitmap func of the overlay being restored.
func NewOverlay(base, top Store, dirty []byte, opts *OverlayOpts) (*Overlay, error) {
	baseSizeBytes, err := base.SizeBytes()
	if err != nil {
		return nil, err
	}
	topSizeBytes, err := top.SizeBytes()
	if err != nil {
		return nil, err
	}
	if baseSizeBytes != topSizeBytes {
		return nil, status.InvalidArgumentErrorf("base layer size %d != top layer size %d", baseSizeBytes, topSizeBytes)
	}
	if opts == nil {
		opts = &OverlayOpts{}
	}
	blockSizeBytes := opts.BlockSizeBytes
	if blockSizeBytes == 0 {
		blockSizeBytes = defaultOverlayBlockSizeBytes
	}
	if len(dirty) == 0 {
		// Not restoring from snapshot; initialize a new dirty bitmap.
		// TODO: maybe mmap the dirty bitmap to simplify the logic around
		// syncing this with a file on disk.
		nChunks := blockCount(baseSizeBytes, blockSizeBytes)
		dirty = []byte(bitmap.New(int(nChunks)))
	}
	return &Overlay{
		base:           base,
		top:            top,
		sizeBytes:      baseSizeBytes,
		blockSizeBytes: blockSizeBytes,
		dirty:          bitmap.Bitmap(dirty),
		copyUpBuf:      make([]byte, blockSizeBytes),
	}, nil
}

func (o *Overlay) ReadAt(p []byte, off int64) (int, error) {
	blockIndex := int(off / o.blockSizeBytes)
	n := 0
	for len(p) > 0 {
		// On each iteration, read one block from the appropriate layer. On the
		// initial iteration, read a partial block if the offset is not
		// block-aligned. On the last iteration, if the number of remaining
		// bytes to read is less than a block, also read a partial block.
		// Otherwise, read a full block.
		readSize := int(o.blockSizeBytes - off%o.blockSizeBytes)
		if readSize > len(p) {
			readSize = len(p)
		}
		nr, err := readFullAt(o.getReadLayer(blockIndex), p[:readSize], off)
		n += nr
		if err != nil {
			return n, err
		}
		p = p[readSize:]
		off += int64(readSize)
		blockIndex++
	}
	return n, nil
}

func (o *Overlay) WriteAt(p []byte, off int64) (int, error) {
	blockIndex := int(off / o.blockSizeBytes)
	n := 0
	for len(p) > 0 {
		// On each iteration, write one block to the top layer, first copying up
		// from the base layer if needed. On the initial iteration, write a
		// partial block if the offset is not block-aligned. On the last
		// iteration, if the number of remaining bytes to be written is less
		// than a block, also write a partial block. Otherwise, write a full
		// block.
		writeSize := int(o.blockSizeBytes - off%o.blockSizeBytes)
		if writeSize > len(p) {
			writeSize = len(p)
		}
		var nw int
		var err error
		if !o.dirty.Get(blockIndex) && int64(writeSize) < o.blockSizeBytes {
			// If we're dirtying a block and not overwriting the whole block,
			// then we need to copy up from the base layer so that if we later
			// read from an unmodified section of the now-dirty block, it
			// matches its original contents from the base layer.
			nw, err = o.writeWithCopyUp(p[:writeSize], off%o.blockSizeBytes, blockIndex)
		} else {
			nw, err = o.top.WriteAt(p[:writeSize], off)
		}
		o.dirty.Set(blockIndex, true)
		n += nw
		if err != nil {
			return n, err
		}
		if nw != writeSize {
			return n, io.ErrShortWrite
		}
		p = p[writeSize:]
		off += int64(writeSize)
		blockIndex++
	}
	return n, nil
}

func (o *Overlay) getReadLayer(blockIndex int) Store {
	if o.dirty.Get(blockIndex) {
		return o.top
	}
	return o.base
}

// writeWithCopyUp reads the block at the given blockIndex from the bottom
// layer, writes p to the block (starting at blockOffset, which is relative to
// the start of the block), then writes the modified block to the top layer.
func (o *Overlay) writeWithCopyUp(p []byte, blockOffset int64, blockIndex int) (int, error) {
	blockStart := int64(blockIndex) * o.blockSizeBytes
	blockLen := o.blockSizeBytes
	if blockStart+blockLen > o.sizeBytes {
		blockLen = o.sizeBytes - blockStart
	}
	block := o.copyUpBuf[:blockLen]
	_, err := readFullAt(o.base, block, blockStart)
	if err != nil {
		return 0, status.WrapError(err, "copy up: failed to read from base layer")
	}
	n := copy(block[blockOffset:], p)
	if n != len(p) {
		return 0, io.ErrShortWrite
	}
	nw, err := o.top.WriteAt(block, blockStart)
	if err != nil {
		return 0, err
	}
	if int64(nw) < blockLen {
		return 0, io.ErrShortWrite
	}
	return n, nil
}

func (o *Overlay) Sync() error {
	if err := o.base.Sync(); err != nil {
		return err
	}
	if err := o.top.Sync(); err != nil {
		return err
	}
	return nil
}

func (o *Overlay) Close() error {
	var lastErr error
	if err := o.base.Close(); err != nil {
		lastErr = err
	}
	if err := o.top.Close(); err != nil {
		lastErr = err
	}
	return lastErr
}

func (o *Overlay) SizeBytes() (int64, error) {
	return o.sizeBytes, nil
}

// BlockSizeBytes returns the block size.
func (o *Overlay) BlockSizeBytes() int64 {
	return o.blockSizeBytes
}

// DirtyBitmap returns a bitmap representing which block indexes are dirty.
func (o *Overlay) DirtyBitmap() []byte {
	return o.dirty
}

func (o *Overlay) BaseLayer() Store {
	return o.base
}

func (o *Overlay) TopLayer() Store {
	return o.top
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
