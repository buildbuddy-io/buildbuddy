package index

import (
	"bytes"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/cockroachdb/pebble"
)

var postFiles = map[string]string{
	"file0": "",
	"file1": "Google Code Search",
	"file2": "Google Code Project Hosting",
	"file3": "Google Web Search",
}

var postFilesIndex = map[string]string{
	"dat:2d633c7d522078e4934efa3086ccea374a5feffd97c03eb5d4bd461701281245": "Google Web Search",
	"dat:5617f20f29913a5b34852df9f2a1ffac3f89412715c1b236d5e6e7e49eadbc90": "Google Code Search",
	"dat:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855": "",
	"dat:f35f09cc7c2dae701866c9ba66b7123a28e39d2636abb52ea1b068d67d3e334f": "Google Code Project Hosting",
	"fil:2d633c7d522078e4934efa3086ccea374a5feffd97c03eb5d4bd461701281245": "file3",
	"fil:5617f20f29913a5b34852df9f2a1ffac3f89412715c1b236d5e6e7e49eadbc90": "file1",
	"fil:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855": "file0",
	"fil:f35f09cc7c2dae701866c9ba66b7123a28e39d2636abb52ea1b068d67d3e334f": "file2",
	"nam:3377870dfeaaa7adf79a374d2702a3fdb13e5e5ea0dd8aa95a802ad39044a92f": "f35f09cc7c2dae701866c9ba66b7123a28e39d2636abb52ea1b068d67d3e334f",
	"nam:56f3fd843f7ae959a8409e0ae7c067a0e862a6faa7a22bad147ee90ee5992bd7": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
	"nam:6f3fef6dc51c7996a74992b70d0c35f328ed909a5e07646cf0bab3383c95bb02": "2d633c7d522078e4934efa3086ccea374a5feffd97c03eb5d4bd461701281245",
	"nam:c147efcfc2d7ea666a9e4f5187b115c90903f0fc896a56df9a6ef5d8f3fc9f31": "5617f20f29913a5b34852df9f2a1ffac3f89412715c1b236d5e6e7e49eadbc90",
	"tri: Co:1":          "[267523926 3423166451]",
	"tri: Ho:1":          "[3423166451]",
	"tri: Pr:1":          "[3423166451]",
	"tri: Se:1":          "[267523926 2101109549]",
	"tri: We:1":          "[2101109549]",
	"tri:Cod:1":          "[267523926 3423166451]",
	"tri:Goo:1":          "[267523926 2101109549 3423166451]",
	"tri:Hos:1":          "[3423166451]",
	"tri:Pro:1":          "[3423166451]",
	"tri:Sea:1":          "[267523926 2101109549]",
	"tri:Web:1":          "[2101109549]",
	"tri:arc:1":          "[267523926 2101109549]",
	"tri:b S:1":          "[2101109549]",
	"tri:ct :1":          "[3423166451]",
	"tri:de :1":          "[267523926 3423166451]",
	"tri:e C:1":          "[267523926 3423166451]",
	"tri:e P:1":          "[3423166451]",
	"tri:e S:1":          "[267523926]",
	"tri:e W:1":          "[2101109549]",
	"tri:ear:1":          "[267523926 2101109549]",
	"tri:eb :1":          "[2101109549]",
	"tri:ect:1":          "[3423166451]",
	"tri:gle:1":          "[267523926 2101109549 3423166451]",
	"tri:ing:1":          "[3423166451]",
	"tri:jec:1":          "[3423166451]",
	"tri:le :1":          "[267523926 2101109549 3423166451]",
	"tri:ode:1":          "[267523926 3423166451]",
	"tri:ogl:1":          "[267523926 2101109549 3423166451]",
	"tri:oje:1":          "[3423166451]",
	"tri:oog:1":          "[267523926 2101109549 3423166451]",
	"tri:ost:1":          "[3423166451]",
	"tri:rch:1":          "[267523926 2101109549]",
	"tri:roj:1":          "[3423166451]",
	"tri:sti:1":          "[3423166451]",
	"tri:t H:1":          "[3423166451]",
	"tri:tin:1":          "[3423166451]",
	"tri:\xff\xff\xff:1": "[]",
}

func writeTestingIndex(t *testing.T, dir string) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range postFilesIndex {
		// convert posting lists back to []uint32
		if strings.HasPrefix(v, "[") && strings.HasSuffix(v, "]") {
			v = strings.TrimSuffix(strings.TrimPrefix(v, "["), "]")
			bm := roaring.New()
			for _, s := range strings.Split(v, " ") {
				if len(s) == 0 {
					continue
				}
				d, err := strconv.ParseUint(s, 10, 32)
				if err != nil {
					t.Fatal(err)
				}
				bm.Add(uint32(d))
			}
			buf := new(bytes.Buffer)
			if _, err := bm.WriteTo(buf); err != nil {
				log.Fatal(err)
			}
			db.Set([]byte(k), buf.Bytes(), pebble.Sync)
		} else {
			db.Set([]byte(k), []byte(v), pebble.Sync)
		}
	}
	db.Flush()
	db.Close()
}

func tri(x, y, z byte) uint32 {
	return uint32(x)<<16 | uint32(y)<<8 | uint32(z)
}

func TestTrivialPosting(t *testing.T) {
	d, _ := os.MkdirTemp("", "test")
	defer os.RemoveAll(d)

	writeTestingIndex(t, d)

	catchErr := func(pl []uint32, err error) []uint32 {
		if err != nil {
			t.Fatal(err)
		}
		return pl
	}

	db, err := pebble.Open(d, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ix := Open(db)
	if l := catchErr(ix.PostingList(tri('S', 'e', 'a'))); !equalList(l, []uint32{267523926, 2101109549}) {
		t.Errorf("PostingList(Sea) = %v, want [267523926 2101109549]", l)
	}
	if l := catchErr(ix.PostingList(tri('G', 'o', 'o'))); !equalList(l, []uint32{267523926, 2101109549, 3423166451}) {
		t.Errorf("PostingList(Goo) = %v, want [267523926 2101109549 3423166451]", l)
	}

	sea := catchErr(ix.PostingList(tri('S', 'e', 'a')))
	if l := catchErr(ix.PostingAnd(sea, tri('G', 'o', 'o'))); !equalList(l, []uint32{267523926, 2101109549}) {
		t.Errorf("PostingList(Sea&Goo) = %v, want [267523926 2101109549]", l)
	}

	goo := catchErr(ix.PostingList(tri('G', 'o', 'o')))
	if l := catchErr(ix.PostingAnd(goo, tri('S', 'e', 'a'))); !equalList(l, []uint32{267523926, 2101109549}) {
		t.Errorf("PostingList(Goo&Sea) = %v, want [267523926 2101109549]", l)
	}

	sea = catchErr(ix.PostingList(tri('S', 'e', 'a')))
	if l := catchErr(ix.PostingOr(sea, tri('G', 'o', 'o'))); !equalList(l, []uint32{267523926, 2101109549, 3423166451}) {
		t.Errorf("PostingList(Sea|Goo) = %v, want [267523926 2101109549 3423166451]", l)
	}
	goo = catchErr(ix.PostingList(tri('G', 'o', 'o')))
	if l := catchErr(ix.PostingOr(goo, tri('S', 'e', 'a'))); !equalList(l, []uint32{267523926, 2101109549, 3423166451}) {
		t.Errorf("PostingList(Goo|Sea) = %v, want [267523926 2101109549 3423166451]", l)
	}
}

func equalList(x, y []uint32) bool {
	if len(x) != len(y) {
		return false
	}
	for i, xi := range x {
		if xi != y[i] {
			return false
		}
	}
	return true
}
