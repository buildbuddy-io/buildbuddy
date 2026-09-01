package ext4writer

// A virtual ext4 image: the same layout the writer produces, but served
// through a block-device interface instead of materialized into a file.
// Metadata blocks live in memory, file data blocks are read on demand from
// the source files (host workspace files or filecache entries), and guest
// writes go to a sparse overlay file. Preparation cost is O(metadata) and the
// input bytes are never copied per action; the host page cache of the source
// files is shared across all actions that use them.

import (
	"context"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sys/unix"
)

// blockMap collects metadata writes as 4 KiB blocks.
type blockMap map[uint32][]byte

func (m blockMap) WriteAt(p []byte, off int64) (int, error) {
	n := 0
	for len(p) > 0 {
		blk := uint32(off / blockSize)
		inBlk := int(off % blockSize)
		buf, ok := m[blk]
		if !ok {
			buf = make([]byte, blockSize)
			m[blk] = buf
		}
		c := copy(buf[inBlk:], p)
		p = p[c:]
		off += int64(c)
		n += c
	}
	return n, nil
}

// dataExtent maps a physical block range of the image to a source file range.
type dataExtent struct {
	start   uint32 // physical block
	n       uint32
	node    *node
	logical uint32 // logical block within the file
}

// VirtualOptions configures a VirtualImage.
type VirtualOptions struct {
	// PinSources opens every source file when the image is created and keeps
	// it open for the image's lifetime, so that a filecache eviction can't
	// pull an input out from under a running action. Costs one fd per unique
	// file. Off by default (host workspace hardlinks already pin the inode).
	PinSources bool
}

// VirtualImage implements vbd.BlockDevice (io.ReaderAt, io.WriterAt,
// SizeBytes) for a workspace image that is never written to disk.
type VirtualImage struct {
	stats   *Stats
	size    int64
	nblocks uint32
	meta    blockMap
	extents []dataExtent // sorted by start
	open    func(n *node) (*os.File, error)

	mu      sync.Mutex
	dirty   []uint64 // bitset of dirty blocks, backed by overlay
	overlay *os.File // sparse file holding dirty blocks at block*blockSize
	fds     *fdCache
}

const virtualImageMaxOpenFiles = 512

// NewVirtualImage lays out an image for inputDir. overlayDir is where the
// sparse overlay file for guest writes is created.
func NewVirtualImage(ctx context.Context, inputDir, overlayDir string, opts *Options) (*VirtualImage, error) {
	w := newWriter(opts)
	w.open = func(n *node) (*os.File, error) { return os.Open(n.path) }
	start := time.Now()
	if _, err := w.walk(inputDir); err != nil {
		return nil, status.WrapError(err, "walk input directory")
	}
	w.stats.WalkDuration = time.Since(start)
	return w.finishVirtual(ctx, overlayDir, &VirtualOptions{})
}

// NewVirtualImageFromTree is NewVirtualImage with an input Tree overlaid on
// inputDir (see DirectoryAndTreeToImage).
func NewVirtualImageFromTree(ctx context.Context, inputDir, overlayDir string, opts *TreeOptions, vopts *VirtualOptions) (*VirtualImage, error) {
	if vopts == nil {
		vopts = &VirtualOptions{PinSources: true}
	}
	if opts == nil || opts.Tree == nil || opts.Open == nil {
		return nil, status.InvalidArgumentError("tree and opener are required")
	}
	w := newWriter(&opts.Options)
	w.open = func(n *node) (*os.File, error) {
		if n.fileNode != nil {
			return opts.Open(ctx, n.fileNode)
		}
		return os.Open(n.path)
	}
	start := time.Now()
	root, err := w.walk(inputDir)
	if err != nil {
		return nil, status.WrapError(err, "walk input directory")
	}
	if err := w.addTree(root, opts.Tree, opts.DigestFunction); err != nil {
		return nil, status.WrapError(err, "add input tree")
	}
	w.stats.WalkDuration = time.Since(start)
	return w.finishVirtual(ctx, overlayDir, vopts)
}

func newWriter(opts *Options) *writer {
	if opts == nil {
		opts = &Options{}
	}
	w := &writer{opts: *opts, stats: &Stats{}}
	if w.opts.Concurrency <= 0 {
		w.opts.Concurrency = min(8, defaultConcurrency())
	}
	if w.opts.Now.IsZero() {
		w.opts.Now = time.Now()
	}
	return w
}

func (w *writer) finishVirtual(ctx context.Context, overlayDir string, vopts *VirtualOptions) (*VirtualImage, error) {
	w.assignInodes()
	start := time.Now()
	if err := w.layout(w.root); err != nil {
		return nil, status.WrapError(err, "compute layout")
	}
	w.stats.LayoutDuration = time.Since(start)

	start = time.Now()
	meta := blockMap{}
	if err := w.writeMetadata(meta); err != nil {
		return nil, status.WrapError(err, "write metadata")
	}
	w.stats.MetadataDuration = time.Since(start)

	var exts []dataExtent
	for _, n := range w.nodes {
		if !n.mode.IsRegular() || n.size == 0 {
			continue
		}
		for _, e := range n.extents {
			exts = append(exts, dataExtent{start: e.start, n: e.len, node: n, logical: e.logical})
		}
	}
	sort.Slice(exts, func(i, j int) bool { return exts[i].start < exts[j].start })

	overlay, err := os.CreateTemp(overlayDir, "workspace-overlay-*")
	if err != nil {
		return nil, status.WrapError(err, "create overlay file")
	}
	// Unlink immediately: the overlay lives only as long as the fd.
	os.Remove(overlay.Name())
	v := &VirtualImage{
		stats:   w.stats,
		size:    w.stats.ImageBytes,
		nblocks: w.blocksCount,
		meta:    meta,
		extents: exts,
		open:    w.open,
		dirty:   make([]uint64, (w.blocksCount+63)/64),
		overlay: overlay,
		fds:     newFDCache(virtualImageMaxOpenFiles),
	}
	if vopts.PinSources {
		// Open everything now (in parallel) and keep it open.
		start := time.Now()
		if err := v.pinAll(ctx, w.opts.Concurrency); err != nil {
			v.Close()
			return nil, status.WrapError(err, "open input files")
		}
		w.stats.DataDuration = time.Since(start)
	}
	return v, nil
}

// pinAll opens every source file and holds a reference so it is never
// evicted from the fd cache (and can't be deleted from under us).
func (v *VirtualImage) pinAll(ctx context.Context, concurrency int) error {
	seen := map[*node]bool{}
	var nodes []*node
	for i := range v.extents {
		n := v.extents[i].node
		if !seen[n] {
			seen[n] = true
			nodes = append(nodes, n)
		}
	}
	if rl, err := getNoFileLimit(); err == nil && uint64(len(nodes)+1024) > rl {
		return status.ResourceExhaustedErrorf("need %d open files but RLIMIT_NOFILE is %d", len(nodes), rl)
	}
	v.fds.max = len(nodes) + virtualImageMaxOpenFiles
	ch := make(chan *node)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for i := 0; i < max(1, concurrency); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range ch {
				_, _, err := v.fds.get(n, v.open) // reference intentionally never released
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
				}
			}
		}()
	}
	for _, n := range nodes {
		if ctx.Err() != nil {
			break
		}
		ch <- n
	}
	close(ch)
	wg.Wait()
	return firstErr
}

// Stats returns layout statistics.
func (v *VirtualImage) Stats() *Stats { return v.stats }

// SizeBytes implements vbd.BlockDevice.
func (v *VirtualImage) SizeBytes() (int64, error) { return v.size, nil }

func (v *VirtualImage) isDirty(blk uint32) bool {
	return v.dirty[blk/64]&(1<<(blk%64)) != 0
}

func (v *VirtualImage) setDirty(blk uint32) {
	v.dirty[blk/64] |= 1 << (blk % 64)
}

// findExtent returns the data extent containing blk, or nil.
func (v *VirtualImage) findExtent(blk uint32) *dataExtent {
	i := sort.Search(len(v.extents), func(i int) bool { return v.extents[i].start+v.extents[i].n > blk })
	if i < len(v.extents) && v.extents[i].start <= blk {
		return &v.extents[i]
	}
	return nil
}

// ReadAt implements io.ReaderAt. Reads are served block by block from the
// dirty overlay, the metadata map, the source files, or zeros.
func (v *VirtualImage) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= v.size {
		return 0, io.EOF
	}
	short := false
	if int64(len(p)) > v.size-off {
		p = p[:v.size-off]
		short = true
	}
	n, err := v.readAll(p, off)
	if err == nil && short {
		return n, io.EOF
	}
	return n, err
}

func (v *VirtualImage) readAll(p []byte, off int64) (int, error) {
	done := 0
	for done < len(p) {
		blk := uint32((off + int64(done)) / blockSize)
		inBlk := int((off + int64(done)) % blockSize)
		n := min(blockSize-inBlk, len(p)-done)
		dst := p[done : done+n]
		v.mu.Lock()
		dirty := v.isDirty(blk)
		v.mu.Unlock()
		switch {
		case dirty:
			if _, err := v.overlay.ReadAt(dst, int64(blk)*blockSize+int64(inBlk)); err != nil && err != io.EOF {
				return done, err
			}
		default:
			if buf, ok := v.meta[blk]; ok {
				copy(dst, buf[inBlk:inBlk+n])
			} else if e := v.findExtent(blk); e != nil {
				if err := v.readFileBlock(e, blk, inBlk, dst); err != nil {
					return done, err
				}
			} else {
				clear(dst)
			}
		}
		done += n
	}
	return done, nil
}

// readFileBlock fills dst with bytes [inBlk, inBlk+len(dst)) of physical block
// blk, which belongs to data extent e. Bytes past the file's end are zero.
func (v *VirtualImage) readFileBlock(e *dataExtent, blk uint32, inBlk int, dst []byte) error {
	fileOff := int64(e.logical+(blk-e.start))*blockSize + int64(inBlk)
	n := e.node
	if fileOff >= n.size {
		clear(dst)
		return nil
	}
	want := dst
	if fileOff+int64(len(dst)) > n.size {
		want = dst[:n.size-fileOff]
		clear(dst[len(want):])
	}
	f, release, err := v.fds.get(n, v.open)
	if err != nil {
		return err
	}
	defer release()
	nr, err := f.ReadAt(want, fileOff)
	if err != nil && !(err == io.EOF && nr == len(want)) {
		if err == io.EOF {
			// The source is shorter than it was when the layout was computed:
			// don't hand the guest zeros in place of input data.
			return status.DataLossErrorf("input %q truncated: expected %d bytes at %d, got %d", n.name, len(want), fileOff, nr)
		}
		return err
	}
	return nil
}

// WriteAt implements io.WriterAt: writes go to the overlay. Partial-block
// writes first pull the current block contents into the overlay.
func (v *VirtualImage) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 || off+int64(len(p)) > v.size {
		return 0, status.InvalidArgumentErrorf("write [%d,+%d) outside image of %d bytes", off, len(p), v.size)
	}
	done := 0
	for done < len(p) {
		blk := uint32((off + int64(done)) / blockSize)
		inBlk := int((off + int64(done)) % blockSize)
		n := min(blockSize-inBlk, len(p)-done)
		v.mu.Lock()
		if !v.isDirty(blk) && n < blockSize {
			// Copy-up the rest of the block first.
			var cur [blockSize]byte
			v.mu.Unlock()
			if _, err := v.ReadAt(cur[:], int64(blk)*blockSize); err != nil && err != io.EOF {
				return done, err
			}
			v.mu.Lock()
			if !v.isDirty(blk) {
				if _, err := v.overlay.WriteAt(cur[:], int64(blk)*blockSize); err != nil {
					v.mu.Unlock()
					return done, err
				}
			}
		}
		if _, err := v.overlay.WriteAt(p[done:done+n], int64(blk)*blockSize+int64(inBlk)); err != nil {
			v.mu.Unlock()
			return done, err
		}
		v.setDirty(blk)
		v.mu.Unlock()
		done += n
	}
	return done, nil
}

// DirtyBlocks returns how many blocks the guest has written.
func (v *VirtualImage) DirtyBlocks() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	c := 0
	for _, w := range v.dirty {
		for ; w != 0; w &= w - 1 {
			c++
		}
	}
	return c
}

// Close releases open source files and the overlay.
func (v *VirtualImage) Close() error {
	v.fds.closeAll()
	return v.overlay.Close()
}

// fdCache keeps a bounded number of source files open, LRU-evicting the rest.
// Entries in use are never evicted.
type fdCache struct {
	mu    sync.Mutex
	max   int
	items map[*node]*fdEntry
	lru   []*node // most recently used at the end
}

type fdEntry struct {
	f    *os.File
	refs int
}

func newFDCache(max int) *fdCache { return &fdCache{max: max, items: map[*node]*fdEntry{}} }

func (c *fdCache) get(n *node, open func(n *node) (*os.File, error)) (*os.File, func(), error) {
	c.mu.Lock()
	if e, ok := c.items[n]; ok {
		e.refs++
		c.touch(n)
		c.mu.Unlock()
		return e.f, func() { c.put(n) }, nil
	}
	c.mu.Unlock()
	f, err := open(n)
	if err != nil {
		return nil, nil, err
	}
	c.mu.Lock()
	if e, ok := c.items[n]; ok {
		// Raced with another opener; use theirs.
		f.Close()
		e.refs++
		c.touch(n)
		c.mu.Unlock()
		return e.f, func() { c.put(n) }, nil
	}
	c.items[n] = &fdEntry{f: f, refs: 1}
	c.lru = append(c.lru, n)
	c.evictLocked()
	c.mu.Unlock()
	return f, func() { c.put(n) }, nil
}

func (c *fdCache) put(n *node) {
	c.mu.Lock()
	if e, ok := c.items[n]; ok {
		e.refs--
	}
	c.evictLocked()
	c.mu.Unlock()
}

func getNoFileLimit() (uint64, error) {
	var rl unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rl); err != nil {
		return 0, err
	}
	return rl.Cur, nil
}

func (c *fdCache) touch(n *node) {
	for i, x := range c.lru {
		if x == n {
			c.lru = append(c.lru[:i], c.lru[i+1:]...)
			break
		}
	}
	c.lru = append(c.lru, n)
}

func (c *fdCache) evictLocked() {
	for len(c.items) > c.max {
		evicted := false
		for i, n := range c.lru {
			if e := c.items[n]; e != nil && e.refs == 0 {
				e.f.Close()
				delete(c.items, n)
				c.lru = append(c.lru[:i], c.lru[i+1:]...)
				evicted = true
				break
			}
		}
		if !evicted {
			return
		}
	}
}

func (c *fdCache) closeAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for n, e := range c.items {
		e.f.Close()
		delete(c.items, n)
	}
	c.lru = nil
}
