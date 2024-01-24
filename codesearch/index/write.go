package index

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/cockroachdb/pebble"
	"github.com/buildbuddy-io/buildbuddy/codesearch/sparse"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

// An mmapData is mmap'ed read-only data from a file.
type mmapData struct {
	f *os.File
	d []byte
}

type IndexWriter struct {
	db *pebble.DB

	LogSkip bool // log information about skipped files
	Verbose bool // log status using package log

	inbuf      []byte // input buffer
	totalBytes int64

	trigram        *sparse.Set // trigrams for the current file
	post           []postEntry // list of (trigram, file#) pairs
	postFile       []*os.File  // flushed post entries
	filesProcessed int

	repoID    []byte // TODO(tylerw): set this via API instead of hacky
	segmentID string
}

// Tuning constants for detecting text files.
// A file is assumed not to be text files (and thus not indexed)
// if it contains an invalid UTF-8 sequences, if it is longer than maxFileLength
// bytes, if it contains a line longer than maxLineLen bytes,
// or if it contains more than maxTextTrigrams distinct trigrams.
const (
	maxFileLen      = 1 << 30
	maxLineLen      = 2000
	maxTextTrigrams = 20000

	npost = 64 << 20 / 8 // 64 MB worth of post entries
)

// A postEntry is an in-memory (trigram, file#) pair.
type postEntry uint64

func (p postEntry) trigram() uint32 {
	return uint32(p >> 32)
}

func (p postEntry) fileid() uint32 {
	return uint32(p)
}

func makePostEntry(trigram, fileid uint32) postEntry {
	return postEntry(trigram)<<32 | postEntry(fileid)
}

// Create returns a new IndexWriter that will write the index to file.
func Create(db *pebble.DB) (*IndexWriter, error) {
	sID, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	return &IndexWriter{
		db:        db,
		trigram:   sparse.NewSet(1 << 24),
		post:      make([]postEntry, 0, npost),
		inbuf:     make([]byte, 16384),
		segmentID: sID.String(),
	}, nil
}

// AddFile adds the file with the given name (opened using os.Open)
// to the index. It logs errors using package log.
func (iw *IndexWriter) AddFile(name string) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	return iw.Add(name, f)
}

func (iw *IndexWriter) fileExists(fileDigest string) bool {
	_, closer, err := iw.db.Get(filenameKey(fileDigest))
	if err != pebble.ErrNotFound {
		//log.Printf("File %q already indexed!!!", fileDigest)
		closer.Close()
		return true
	}
	return false
}

// Add adds the file f to the index under the given name.
// It logs errors using package log.
func (iw *IndexWriter) Add(name string, f io.ReadSeeker) error {
	hashSum, err := hashFile(f)
	if err != nil {
		return err
	}
	digest := fmt.Sprintf("%x", hashSum)
	fileid := bytesToUint32(hashSum[:4])

	if iw.fileExists(digest) {
		return nil
	}

	f.Seek(0, 0)
	iw.trigram.Reset()
	var (
		c         = byte(0)
		i         = 0
		buf       = iw.inbuf[:0]
		readCount = 0
		tv        = uint32(0)
		n         = int64(0)
		linelen   = 0
	)
	for {
		tv = (tv << 8) & (1<<24 - 1)
		if i >= len(buf) {
			n, err := f.Read(buf[:cap(buf)])
			if n == 0 {
				if err != nil {
					if err == io.EOF {
						break
					}
					log.Printf("%s: %v\n", name, err)
					return nil
				}
				log.Printf("%s: 0-length read\n", name)
				return nil
			}
			buf = buf[:n]
			i = 0
			readCount += 1
		}
		c = buf[i]
		i++
		tv |= uint32(c)
		if n++; n >= 3 {
			iw.trigram.Add(tv)
		}
		if !validUTF8((tv>>8)&0xFF, tv&0xFF) {
			if iw.LogSkip {
				log.Printf("%s: invalid UTF-8, ignoring\n", name)
			}
			return nil
		}
		if n > maxFileLen {
			if iw.LogSkip {
				log.Printf("%s: too long, ignoring\n", name)
			}
			return nil
		}
		if linelen++; linelen > maxLineLen {
			if iw.LogSkip {
				log.Printf("%s: very long lines, ignoring\n", name)
			}
			return nil
		}
		if c == '\n' {
			linelen = 0
		}
	}
	if iw.trigram.Len() > maxTextTrigrams {
		if iw.LogSkip {
			log.Printf("%s: too many trigrams, probably not text, ignoring\n", name)
		}
		return nil
	}
	iw.totalBytes += n

	if iw.Verbose {
		log.Printf("%d %d %s id %d (%q)\n", n, iw.trigram.Len(), name, fileid, digest)
	}

	for _, trigram := range iw.trigram.Dense() {
		if len(iw.post) >= cap(iw.post) {
			if err := iw.flushPost(); err != nil {
				return err
			}
		}
		iw.post = append(iw.post, makePostEntry(trigram, fileid))
	}

	if err := iw.db.Set(filenameKey(digest), []byte(name), pebble.NoSync); err != nil {
		return err
	}
	var fileBuf []byte
	if readCount == 1 {
		fileBuf = buf
	} else {
		f.Seek(0, 0)
		fileBuf, err = io.ReadAll(f)
		if err != nil {
			return err
		}
	}
	if err := iw.db.Set(dataKey(digest), fileBuf, pebble.NoSync); err != nil {
		return err
	}
	if err := iw.db.Set(namehashKey(hashString(name)), []byte(digest), pebble.NoSync); err != nil {
		return err
	}

	iw.filesProcessed += 1
	log.Printf("iw.filesProcessed: %d", iw.filesProcessed)
	return nil
}

func (iw *IndexWriter) Flush() error {
	iw.mergePost()
	if err := iw.db.Flush(); err != nil {
		return err
	}
	return nil
}

// flushPost writes iw.post to a new temporary file and
// clears the slice.
func (iw *IndexWriter) flushPost() error {
	w, err := os.CreateTemp("", "csearch-index")
	if err != nil {
		return err
	}
	if iw.Verbose {
		log.Printf("flush %d entries to %s", len(iw.post), w.Name())
	}
	sortPost(iw.post)

	// Write the raw iw.post array to disk as is.
	// This process is the one reading it back in, so byte order is not a concern.
	data := (*[npost * 8]byte)(unsafe.Pointer(&iw.post[0]))[:len(iw.post)*8]
	if n, err := w.Write(data); err != nil || n < len(data) {
		if err != nil {
			return err
		}
		return fmt.Errorf("short write writing %s", w.Name())
	}

	iw.post = iw.post[:0]
	w.Seek(0, 0)
	iw.postFile = append(iw.postFile, w)
	return nil
}

// mergePost reads the flushed index entries and merges them
// into posting lists, writing the resulting lists to out.
func (iw *IndexWriter) mergePost() error {
	var h postHeap

	log.Printf("merge %d files + mem", len(iw.postFile))
	for _, f := range iw.postFile {
		h.addFile(f)
	}
	sortPost(iw.post)
	h.addMem(iw.post)

	e := h.next()

	mu := sync.Mutex{}
	batch := iw.db.NewBatch()

	flushBatch := func() error {
		if batch.Empty() {
			return nil
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			return err
		}
		log.Printf("flushed batch")
		batch = iw.db.NewBatch()
		return nil
	}

	eg := new(errgroup.Group)
	eg.SetLimit(runtime.GOMAXPROCS(0))
	writeDocIDs := func(key []byte, ids []uint32) error {
		buf := new(bytes.Buffer)
		pl := roaring.BitmapOf(ids...)
		if _, err := pl.WriteTo(buf); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()

		if err := batch.Set(key, buf.Bytes(), nil); err != nil {
			return err
		}
		if batch.Len() >= 64<<20 {
			if err := flushBatch(); err != nil {
				return err
			}
		}
		return nil
	}

	npost := 0

	for {
		trigram := e.trigram()

		// posting list
		npost++
		nfile := uint32(0)

		docIDs := make([]uint32, 0, 3)
		for ; e.trigram() == trigram && trigram != 1<<24-1; e = h.next() {
			docIDs = append(docIDs, e.fileid())
			nfile++
		}
		eg.Go(func() error {
			triString := trigramToString(trigram)
			triKey := append(trigramKey(triString), []byte(":"+iw.segmentID)...)
			return writeDocIDs(triKey, docIDs)
		})

		if trigram == 1<<24-1 {
			break
		}
	}
	eg.Wait()
	err := flushBatch()
	log.Printf("Wrote %d posting lists", npost)
	return err
}

// A postChunk represents a chunk of post entries flushed to disk or
// still in memory.
type postChunk struct {
	e postEntry   // next entry
	m []postEntry // remaining entries after e
}

const postBuf = 4096

// A postHeap is a heap (priority queue) of postChunks.
type postHeap struct {
	ch []*postChunk
}

func (h *postHeap) addFile(f *os.File) {
	data := mmapFile(f).d
	m := (*[npost]postEntry)(unsafe.Pointer(&data[0]))[:len(data)/8]
	h.addMem(m)
}

func (h *postHeap) addMem(x []postEntry) {
	h.add(&postChunk{m: x})
}

// step reads the next entry from ch and saves it in ch.e.
// It returns false if ch is over.
func (h *postHeap) step(ch *postChunk) bool {
	old := ch.e
	m := ch.m
	if len(m) == 0 {
		return false
	}
	ch.e = postEntry(m[0])
	m = m[1:]
	ch.m = m
	if old >= ch.e {
		panic("bad sort")
	}
	return true
}

// add adds the chunk to the postHeap.
// All adds must be called before the first call to next.
func (h *postHeap) add(ch *postChunk) {
	if len(ch.m) > 0 {
		ch.e = ch.m[0]
		ch.m = ch.m[1:]
		h.push(ch)
	}
}

// empty reports whether the postHeap is empty.
func (h *postHeap) empty() bool {
	return len(h.ch) == 0
}

// next returns the next entry from the postHeap.
// It returns a postEntry with trigram == 1<<24 - 1 if h is empty.
func (h *postHeap) next() postEntry {
	if len(h.ch) == 0 {
		return makePostEntry(1<<24-1, 0)
	}
	ch := h.ch[0]
	e := ch.e
	m := ch.m
	if len(m) == 0 {
		h.pop()
	} else {
		ch.e = m[0]
		ch.m = m[1:]
		h.siftDown(0)
	}
	return e
}

func (h *postHeap) pop() *postChunk {
	ch := h.ch[0]
	n := len(h.ch) - 1
	h.ch[0] = h.ch[n]
	h.ch = h.ch[:n]
	if n > 1 {
		h.siftDown(0)
	}
	return ch
}

func (h *postHeap) push(ch *postChunk) {
	n := len(h.ch)
	h.ch = append(h.ch, ch)
	if len(h.ch) >= 2 {
		h.siftUp(n)
	}
}

func (h *postHeap) siftDown(i int) {
	ch := h.ch
	for {
		j1 := 2*i + 1
		if j1 >= len(ch) {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < len(ch) && ch[j1].e >= ch[j2].e {
			j = j2
		}
		if ch[i].e < ch[j].e {
			break
		}
		ch[i], ch[j] = ch[j], ch[i]
		i = j
	}
}

func (h *postHeap) siftUp(j int) {
	ch := h.ch
	for {
		i := (j - 1) / 2
		if i == j || ch[i].e < ch[j].e {
			break
		}
		ch[i], ch[j] = ch[j], ch[i]
		j = i
	}
}

// sortPost sorts the postentry list.
// The list is already sorted by fileid (bottom 32 bits)
// and the top 8 bits are always zero, so there are only
// 24 bits to sort.  Run two rounds of 12-bit radix sort.
const sortK = 12

var sortTmp []postEntry
var sortN [1 << sortK]int

func sortPost(post []postEntry) {
	if len(post) > len(sortTmp) {
		sortTmp = make([]postEntry, len(post))
	}
	tmp := sortTmp[:len(post)]

	const k = sortK
	for i := range sortN {
		sortN[i] = 0
	}
	for _, p := range post {
		r := uintptr(p>>32) & (1<<k - 1)
		sortN[r]++
	}
	tot := 0
	for i, count := range sortN {
		sortN[i] = tot
		tot += count
	}
	for _, p := range post {
		r := uintptr(p>>32) & (1<<k - 1)
		o := sortN[r]
		sortN[r]++
		tmp[o] = p
	}
	tmp, post = post, tmp

	for i := range sortN {
		sortN[i] = 0
	}
	for _, p := range post {
		r := uintptr(p>>(32+k)) & (1<<k - 1)
		sortN[r]++
	}
	tot = 0
	for i, count := range sortN {
		sortN[i] = tot
		tot += count
	}
	for _, p := range post {
		r := uintptr(p>>(32+k)) & (1<<k - 1)
		o := sortN[r]
		sortN[r]++
		tmp[o] = p
	}
}
