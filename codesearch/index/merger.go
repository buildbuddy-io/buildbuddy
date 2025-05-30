package index

import (
	"io"

	"github.com/cockroachdb/pebble"

	"github.com/buildbuddy-io/buildbuddy/codesearch/posting"
)

// The name of the merger is stored in the Pebble database. Opening a database with
// a different merger is an error. So if this name changes, the entire codesearch index
// would need to be rebuilt.
// Changing the behavior of this merger without changing the name / regenerating the index
// must be done in a backward compatible way.
const roaringMergerName = "roaring_merger"

// OpenPebbleDB opens a Pebble database at the specified path with the merger used
// for the codesearch index.
func OpenPebbleDB(path string) (*pebble.DB, error) {
	return pebble.Open(path, &pebble.Options{
		Merger: &pebble.Merger{
			Merge: initRoaringMerger,
			Name:  roaringMergerName,
		},
	})
}

func initRoaringMerger(key, value []byte) (pebble.ValueMerger, error) {
	// If we ever need to support merging values that are not posting lists,
	// we can peek at the first few bytes here to see if they match the roaring
	// format: https://github.com/RoaringBitmap/RoaringFormatSpec?tab=readme-ov-file#1-cookie-header
	// For now, we assume all values are posting lists.
	if len(value) == 0 {
		return &roaringValueMerger{
			pl: posting.NewList(),
		}, nil
	}

	pl, err := posting.Unmarshal(value)
	if err != nil {
		return nil, err
	}

	return &roaringValueMerger{
		pl: pl,
	}, nil
}

type roaringValueMerger struct {
	pl posting.List
}

func mergeInto(pl posting.List, docIdBytes []byte) error {
	newPl, err := posting.Unmarshal(docIdBytes)
	if err != nil {
		return err
	}
	pl.Or(newPl)
	return nil
}

func (rm *roaringValueMerger) MergeNewer(value []byte) error {
	return mergeInto(rm.pl, value)
}

func (rm *roaringValueMerger) MergeOlder(value []byte) error {
	return mergeInto(rm.pl, value)
}

func (rm *roaringValueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	r, err := posting.Marshal(rm.pl)
	return r, nil, err
}
