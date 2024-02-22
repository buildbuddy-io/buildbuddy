package index

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/RoaringBitmap/roaring"
	"github.com/buildbuddy-io/buildbuddy/codesearch/query"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/cockroachdb/pebble"
)

// An Index implements read-only access to a trigram index.
type Index struct {
	db      *pebble.DB
	Verbose bool
}

// Open returns a new Index for reading.
func Open(db *pebble.DB) *Index {
	//printDB(db)
	return &Index{
		db: db,
	}
}

func (i *Index) Close() error {
	if err := i.db.Close(); err != nil {
		return err
	}
	return nil
}

// Name returns the name corresponding to the given fileid.
func (ix *Index) Name(fileid uint32) (string, error) {
	buf, err := ix.NameBytes(fileid)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

// NameBytes returns the name corresponding to the given fileid.
func (ix *Index) NameBytes(fileid uint32) ([]byte, error) {
	iter := ix.db.NewIter(&pebble.IterOptions{
		LowerBound: filenameKey(""),
		UpperBound: filenameKey(string('\xff')),
	})
	defer iter.Close()

	filePrefix := filenameKey(fmt.Sprintf("%x", string(uint32ToBytes(fileid))))
	if !iter.SeekGE(filePrefix) || !bytes.HasPrefix(iter.Key(), filePrefix) {
		return nil, fmt.Errorf("File (name) %d not found in index (prefix: %q)", fileid, filePrefix)
	}
	buf := make([]byte, len(iter.Value()))
	copy(buf, iter.Value())
	return buf, nil
}

func (ix *Index) Contents(fileid uint32) ([]byte, error) {
	iter := ix.db.NewIter(&pebble.IterOptions{
		LowerBound: dataKey(""),
		UpperBound: dataKey(string('\xff')),
	})
	defer iter.Close()

	filePrefix := dataKey(fmt.Sprintf("%x", string(uint32ToBytes(fileid))))
	if !iter.SeekGE(filePrefix) || !bytes.HasPrefix(iter.Key(), filePrefix) {
		return nil, fmt.Errorf("File (data) %d not found in index (prefix: %q)", fileid, filePrefix)
	}
	buf := make([]byte, len(iter.Value()))
	copy(buf, iter.Value())
	return buf, nil
}

// Paths returns the list of indexed paths.
func (ix *Index) Paths() ([]string, error) {
	fileIDs, err := ix.allIndexedFiles()
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(fileIDs))

	for _, fileID := range fileIDs {
		name, err := ix.Name(fileID)
		if err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
}

func (ix *Index) allIndexedFiles() ([]uint32, error) {
	iter := ix.db.NewIter(&pebble.IterOptions{
		LowerBound: filenameKey(""),
		UpperBound: filenameKey(string('\xff')),
	})
	defer iter.Close()

	found := make([]uint32, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		digest := bytes.TrimPrefix(iter.Key(), filenameKey(""))
		hashSum, err := hex.DecodeString(string(digest))
		if err != nil {
			return nil, err
		}
		fileid := bytesToUint32(hashSum[:4])
		found = append(found, fileid)
	}
	return found, nil
}

func (ix *Index) PostingList(trigram uint32) ([]uint32, error) {
	return ix.postingList(trigram, nil)
}

func (ix *Index) postingListBM(trigram uint32, restrict *roaring.Bitmap) (*roaring.Bitmap, error) {
	triString := trigramToString(trigram)
	iter := ix.db.NewIter(&pebble.IterOptions{
		LowerBound: ngramKey(triString, 2),
		UpperBound: ngramKey(triString+string('\xff'), 2),
	})
	defer iter.Close()

	resultSet := roaring.New()
	postingList := roaring.New()
	for iter.First(); iter.Valid(); iter.Next() {
		log.Printf("query %q matched key %q", triString, iter.Key())
		if _, err := postingList.ReadFrom(bytes.NewReader(iter.Value())); err != nil {
			return nil, err
		}
		resultSet = roaring.Or(resultSet, postingList)
		postingList.Clear()
	}
	if !restrict.IsEmpty() {
		resultSet.And(restrict)
	}
	return resultSet, nil
}

func (ix *Index) postingList(trigram uint32, restrict []uint32) ([]uint32, error) {
	bm, err := ix.postingListBM(trigram, roaring.BitmapOf(restrict...))
	if err != nil {
		return nil, err
	}
	return bm.ToArray(), nil
}

func (ix *Index) PostingAnd(list []uint32, trigram uint32) ([]uint32, error) {
	return ix.postingAnd(list, trigram, nil)
}

func (ix *Index) postingAnd(list []uint32, trigram uint32, restrict []uint32) ([]uint32, error) {
	bm, err := ix.postingListBM(trigram, roaring.BitmapOf(restrict...))
	if err != nil {
		return nil, err
	}
	bm.And(roaring.BitmapOf(list...))
	return bm.ToArray(), nil
}

func (ix *Index) PostingOr(list []uint32, trigram uint32) ([]uint32, error) {
	return ix.postingOr(list, trigram, nil)
}

func (ix *Index) postingOr(list []uint32, trigram uint32, restrict []uint32) ([]uint32, error) {
	bm, err := ix.postingListBM(trigram, roaring.BitmapOf(restrict...))
	if err != nil {
		return nil, err
	}
	bm.Or(roaring.BitmapOf(list...))
	return bm.ToArray(), nil
}

func (ix *Index) merge(fileids []uint32) ([]uint32, error) {
	filenames := make(map[uint32][]byte)

	fnameIter := ix.db.NewIter(&pebble.IterOptions{
		LowerBound: filenameKey(""),
		UpperBound: filenameKey(string('\xff')),
	})
	defer fnameIter.Close()
	sort.Slice(fileids, func(i, j int) bool { return fileids[i] < fileids[j] })
	for _, fileid := range fileids {
		filePrefix := filenameKey(fmt.Sprintf("%x", string(uint32ToBytes(fileid))))
		if !fnameIter.SeekGE(filePrefix) || !bytes.HasPrefix(fnameIter.Key(), filePrefix) {
			return nil, fmt.Errorf("File %d not found in index (prefix: %q)", fileid, filePrefix)
		}
		buf := make([]byte, len(fnameIter.Value()))
		copy(buf, fnameIter.Value())
		filenames[fileid] = buf
	}

	namehashIter := ix.db.NewIter(&pebble.IterOptions{
		LowerBound: namehashKey(""),
		UpperBound: namehashKey(string('\xff')),
	})
	defer namehashIter.Close()
	for fileid, name := range filenames {
		nameHash := namehashKey(hashString(string(name)))
		if !namehashIter.SeekGE(nameHash) || !bytes.HasPrefix(namehashIter.Key(), nameHash) {
			// log.Printf("File %d (%q) not found (deleted?)", fileid, name)
			delete(filenames, fileid)
		}
		filePrefix := []byte(fmt.Sprintf("%x", uint32ToBytes(fileid)))
		if !bytes.HasPrefix(namehashIter.Value(), filePrefix) {
			// log.Printf("File %d (%q) hash updated (%q != %q)", fileid, name, string(namehashIter.Value()), string(uint32ToBytes(fileid)))
			delete(filenames, fileid)
		}
	}
	x := fileids
	fileids = fileids[:0]
	for _, fileid := range x {
		if _, ok := filenames[fileid]; ok {
			fileids = append(fileids, fileid)
		}
	}
	return fileids, nil
}

func (ix *Index) PostingQuery(q *query.Query) ([]uint32, error) {
	pl, err := ix.postingQuery(q, nil)
	if err != nil {
		return nil, err
	}
	return ix.merge(pl)
}

func (ix *Index) postingQuery(q *query.Query, restrict []uint32) (ret []uint32, err error) {
	var list []uint32
	switch q.Op {
	case query.QNone:
		// nothing
	case query.QAll:
		if restrict != nil {
			return restrict, err
		}
		list, err = ix.allIndexedFiles()
	case query.QAnd:
		for _, t := range q.Trigram {
			tri := uint32(t[0])<<16 | uint32(t[1])<<8 | uint32(t[2])
			if list == nil {
				list, err = ix.postingList(tri, restrict)
			} else {
				list, err = ix.postingAnd(list, tri, restrict)
			}
			if len(list) == 0 {
				return nil, err
			}
		}
		for _, sub := range q.Sub {
			if list == nil {
				list = restrict
			}
			list, err = ix.postingQuery(sub, list)
			if len(list) == 0 {
				return nil, err
			}
		}
	case query.QOr:
		for _, t := range q.Trigram {
			tri := uint32(t[0])<<16 | uint32(t[1])<<8 | uint32(t[2])
			if list == nil {
				list, err = ix.postingList(tri, restrict)
			} else {
				list, err = ix.postingOr(list, tri, restrict)
			}
		}
		for _, sub := range q.Sub {
			l, err := ix.postingQuery(sub, restrict)
			if err != nil {
				return nil, err
			}
			list = mergeOr(list, l)
		}
	}
	return list, err
}

func mergeOr(l1, l2 []uint32) []uint32 {
	var l []uint32
	i := 0
	j := 0
	for i < len(l1) || j < len(l2) {
		switch {
		case j == len(l2) || (i < len(l1) && l1[i] < l2[j]):
			l = append(l, l1[i])
			i++
		case i == len(l1) || (j < len(l2) && l1[i] > l2[j]):
			l = append(l, l2[j])
			j++
		case l1[i] == l2[j]:
			l = append(l, l1[i])
			i++
			j++
		}
	}
	return l
}
