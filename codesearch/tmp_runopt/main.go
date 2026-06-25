// Scratch analysis: how much would roaring64.RunOptimize() shrink the
// serialized posting lists in an existing codesearch index?
//
// Usage: go run ./codesearch/tmp_runopt <path-to-index>
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/buildbuddy-io/buildbuddy/codesearch/posting"
	"github.com/cockroachdb/pebble"
)

var countedListMagic = []byte{'C', 'S', 'T', 'F', 4}

// Same merger as index.OpenPebbleDB (name must match what's stored in the DB);
// merges can still run at read time over un-compacted MERGE operands.
func openReadOnly(path string) (*pebble.DB, error) {
	return pebble.Open(path, &pebble.Options{
		ReadOnly: true,
		Merger: &pebble.Merger{
			Name: "roaring_merger",
			Merge: func(key, value []byte) (pebble.ValueMerger, error) {
				pl := posting.NewMergeList()
				if len(value) > 0 {
					existing, err := posting.Unmarshal(value)
					if err != nil {
						return nil, err
					}
					pl = existing
				}
				return &valueMerger{pl: pl}, nil
			},
		},
	})
}

type valueMerger struct{ pl *posting.MergeList }

func (m *valueMerger) merge(value []byte) error {
	other, err := posting.UnmarshalReadOnly(value)
	if err != nil {
		return err
	}
	m.pl.Or(other)
	return nil
}
func (m *valueMerger) MergeNewer(value []byte) error { return m.merge(value) }
func (m *valueMerger) MergeOlder(value []byte) error { return m.merge(value) }
func (m *valueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	b, err := m.pl.Marshal()
	return b, nil, err
}

// roaringPayload extracts the roaring-bitmap portion of a serialized posting
// list (stripping the CSTF magic + freq tail if present). Mirrors
// posting.decodePostings.
func roaringPayload(buf []byte) ([]byte, error) {
	if !bytes.HasPrefix(buf, countedListMagic) {
		return buf, nil
	}
	rest := buf[len(countedListMagic):]
	n, sz := binary.Uvarint(rest)
	if sz <= 0 || n > uint64(len(rest[sz:])) {
		return nil, fmt.Errorf("bad counted list framing")
	}
	return rest[sz : sz+int(n)], nil
}

type bucket struct {
	name              string
	maxCard           uint64
	lists             int
	before, after     uint64
	improved          int
	improvedBytes     uint64 // savings only from lists that shrank
}

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("usage: %s <index-path>", os.Args[0])
	}
	db, err := openReadOnly(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	buckets := []*bucket{
		{name: "card=1", maxCard: 1},
		{name: "card 2-16", maxCard: 16},
		{name: "card 17-256", maxCard: 256},
		{name: "card 257-4Ki", maxCard: 4096},
		{name: "card 4Ki-64Ki", maxCard: 65536},
		{name: "card >64Ki", maxCard: ^uint64(0)},
	}

	iter, err := db.NewIter(&pebble.IterOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer iter.Close()

	var totalKeys, plKeys, decodeErrs int
	var totalValueBytes, totalRoaringBytes, totalAfterBytes uint64
	var optTime time.Duration
	start := time.Now()

	for iter.First(); iter.Valid(); iter.Next() {
		totalKeys++
		if !bytes.Contains(iter.Key(), []byte(":gra:")) {
			continue
		}
		plKeys++
		val := iter.Value()
		totalValueBytes += uint64(len(val))

		payload, err := roaringPayload(val)
		if err != nil {
			decodeErrs++
			continue
		}
		bm := roaring64.New()
		if _, err := bm.ReadFrom(bytes.NewReader(payload)); err != nil {
			decodeErrs++
			continue
		}
		before := uint64(len(payload))
		t0 := time.Now()
		bm.RunOptimize()
		optTime += time.Since(t0)
		after := bm.GetSerializedSizeInBytes()

		totalRoaringBytes += before
		totalAfterBytes += after

		card := bm.GetCardinality()
		for _, b := range buckets {
			if card <= b.maxCard {
				b.lists++
				b.before += before
				b.after += after
				if after < before {
					b.improved++
					b.improvedBytes += before - after
				}
				break
			}
		}
		if plKeys%500000 == 0 {
			fmt.Fprintf(os.Stderr, "...%d posting lists scanned (%s elapsed)\n", plKeys, time.Since(start).Round(time.Second))
		}
	}
	if err := iter.Error(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("total keys: %d, posting-list keys: %d, decode errors: %d\n", totalKeys, plKeys, decodeErrs)
	fmt.Printf("total posting-list value bytes:  %d (%.2f GiB)\n", totalValueBytes, float64(totalValueBytes)/(1<<30))
	fmt.Printf("roaring payload bytes (before):  %d (%.2f GiB)\n", totalRoaringBytes, float64(totalRoaringBytes)/(1<<30))
	fmt.Printf("roaring payload bytes (after):   %d (%.2f GiB)\n", totalAfterBytes, float64(totalAfterBytes)/(1<<30))
	fmt.Printf("savings: %d bytes (%.2f%% of roaring payload, %.2f%% of all posting-list value bytes)\n",
		totalRoaringBytes-totalAfterBytes,
		100*float64(totalRoaringBytes-totalAfterBytes)/float64(totalRoaringBytes),
		100*float64(totalRoaringBytes-totalAfterBytes)/float64(totalValueBytes))
	fmt.Printf("RunOptimize CPU time: %s total (%.2f µs/list avg)\n\n", optTime.Round(time.Millisecond), float64(optTime.Microseconds())/float64(plKeys))

	fmt.Printf("%-14s %10s %14s %14s %9s %10s\n", "bucket", "lists", "before", "after", "improved", "saved")
	for _, b := range buckets {
		if b.lists == 0 {
			continue
		}
		pct := 0.0
		if b.before > 0 {
			pct = 100 * float64(b.before-b.after) / float64(b.before)
		}
		fmt.Printf("%-14s %10d %14d %14d %9d %9.2f%%\n", b.name, b.lists, b.before, b.after, b.improved, pct)
	}
}
