package index

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/cockroachdb/pebble"
)

var trivialFiles = map[string]string{
	"f0":       "\n\n",
	"file1":    "\na\n",
	"thefile2": "\nab\n",
	"file3":    "\nabc\n",
	"afile4":   "\ndabc\n",
	"file5":    "\nxyzw\n",
}

var trivialIndex = map[string]string{
	"dat:426e0799711d0ae24f9cf63761e97f8e2d0a5cf4695d6c95721645a352fd8d98": "\nabc\n",
	"dat:5209d84ebb2c8deaf291693a3b3e41963640f8ee4f4ccc855fde6bfd920db809": "\ndabc\n",
	"dat:6dba9d80d5c3ac293f1947c1457ea897869ebb556045095ffb3f06b14da2f7f0": "\na\n",
	"dat:75a11da44c802486bc6f65640aa48a730f0f684c5c07a42ba3cd1735eb3fb070": "\n\n",
	"dat:d68f4f99347a5c4b1f844a7432f02e375d8704dac222bb1403e343988a19e122": "\nab\n",
	"dat:f09bab9e688e84d242a75c95e13c6a3855f0ebbeae1231bd63232b926bee8cc2": "\nxyzw\n",
	"fil:426e0799711d0ae24f9cf63761e97f8e2d0a5cf4695d6c95721645a352fd8d98": "file3",
	"fil:5209d84ebb2c8deaf291693a3b3e41963640f8ee4f4ccc855fde6bfd920db809": "afile4",
	"fil:6dba9d80d5c3ac293f1947c1457ea897869ebb556045095ffb3f06b14da2f7f0": "file1",
	"fil:75a11da44c802486bc6f65640aa48a730f0f684c5c07a42ba3cd1735eb3fb070": "f0",
	"fil:d68f4f99347a5c4b1f844a7432f02e375d8704dac222bb1403e343988a19e122": "thefile2",
	"fil:f09bab9e688e84d242a75c95e13c6a3855f0ebbeae1231bd63232b926bee8cc2": "file5",
	"nam:122e449c9b64ae8807051d6bd6f6dfbe3dd4c1ea496a4a603da4c9710c6bfd8f": "5209d84ebb2c8deaf291693a3b3e41963640f8ee4f4ccc855fde6bfd920db809",
	"nam:3196620cd4c3ad773bbbb0f1435a59d0b4a5b54d7f8f03604a8d34d629631c85": "d68f4f99347a5c4b1f844a7432f02e375d8704dac222bb1403e343988a19e122",
	"nam:6f3fef6dc51c7996a74992b70d0c35f328ed909a5e07646cf0bab3383c95bb02": "426e0799711d0ae24f9cf63761e97f8e2d0a5cf4695d6c95721645a352fd8d98",
	"nam:865ab0d317f36965e43d20d275b545a6773137adad19db1d61ecb8032f473e0b": "75a11da44c802486bc6f65640aa48a730f0f684c5c07a42ba3cd1735eb3fb070",
	"nam:9a8363aff25b5ffb5120eeb66d735bfd225d6e27d0a1ce6afc2a6b177bb94336": "f09bab9e688e84d242a75c95e13c6a3855f0ebbeae1231bd63232b926bee8cc2",
	"nam:c147efcfc2d7ea666a9e4f5187b115c90903f0fc896a56df9a6ef5d8f3fc9f31": "6dba9d80d5c3ac293f1947c1457ea897869ebb556045095ffb3f06b14da2f7f0",
	"tri:\na\n:1":        "[2157820525]",
	"tri:\nab:1":         "[2567401026 2572128214]",
	"tri:\nda:1":         "[1322781010]",
	"tri:\nxy:1":         "[2662046704]",
	"tri:ab\n:1":         "[2572128214]",
	"tri:abc:1":          "[1322781010 2567401026]",
	"tri:bc\n:1":         "[1322781010 2567401026]",
	"tri:dab:1":          "[1322781010]",
	"tri:xyz:1":          "[2662046704]",
	"tri:yzw:1":          "[2662046704]",
	"tri:zw\n:1":         "[2662046704]",
	"tri:\xff\xff\xff:1": "[]",
}

func readIndex(t *testing.T, dir string) map[string]string {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{0},
		UpperBound: []byte{math.MaxUint8},
	})
	defer iter.Close()
	x := make(map[string]string)
	for iter.First(); iter.Valid(); iter.Next() {
		if bytes.HasPrefix(iter.Key(), []byte(trigramPrefix)) {
			bm := roaring.New()
			if _, err := bm.ReadFrom(bytes.NewReader(iter.Value())); err != nil {
				t.Fatal(err)
			}
			x[string(iter.Key())] = fmt.Sprintf("%+v", bm.ToArray())
		} else {
			x[string(iter.Key())] = string(iter.Value())
		}
	}
	db.Close()
	return x
}

func TestTrivialWrite(t *testing.T) {
	d, _ := os.MkdirTemp("", "test")
	defer os.RemoveAll(d)

	db, err := pebble.Open(d, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}

	iw, err := Create(db)
	if err != nil {
		t.Fatal(err)
	}

	iw.segmentID = "1"
	for name, contents := range trivialFiles {
		err := iw.Add(name, strings.NewReader(contents))
		if err != nil {
			t.Fatal(err)
		}
	}
	iw.flushPost()
	iw.Flush()
	db.Close()

	want := trivialIndex
	got := readIndex(t, d)

	for k, v := range want {
		if got[k] != v {
			t.Fatalf("Got %s = %s, wanted %s", k, got[k], v)
		}
	}
}
