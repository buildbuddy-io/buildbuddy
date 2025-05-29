package index

import (
	"io"

	"github.com/cockroachdb/pebble"

	"github.com/buildbuddy-io/buildbuddy/codesearch/posting"
)

const roaringMergerName = "roaring_merger"

// OpenPebbleDB opens a Pebble database at the specified path with the merger used
// for the codesearch index.
func OpenPebbleDB(path string) (*pebble.DB, error) {
	return pebble.Open(path, &pebble.Options{
		Merger: &pebble.Merger{
			Merge: InitRoaringMerger,
			Name:  roaringMergerName,
		},
	})
}

func InitRoaringMerger(key, value []byte) (pebble.ValueMerger, error) {
	// TODO(jdelfino): Sanity check key to make sure it is a posting list key? this will only work
	// posting lists...
	// TODO(jdelfino): do we need to handle empty/nil value?
	pl, err := posting.Unmarshal(value)
	if err != nil {
		return nil, err
	}

	return &RoaringValueMerger{
		pl: pl,
	}, nil
}

type RoaringValueMerger struct {
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

func (rm *RoaringValueMerger) MergeNewer(value []byte) error {
	return mergeInto(rm.pl, value)
}

func (rm *RoaringValueMerger) MergeOlder(value []byte) error {
	return mergeInto(rm.pl, value)
}

func (rm *RoaringValueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	r, err := posting.Marshal(rm.pl)
	return r, nil, err
}
