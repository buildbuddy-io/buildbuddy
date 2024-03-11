// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"

	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/codesearch/searcher"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/cockroachdb/pebble"
)


var (
	verboseFlag = flag.Bool("verbose", false, "print extra information")
	cpuProfile  = flag.String("cpuprofile", "", "write cpu profile to this file")
)

type IndexReader interface {
	Name(fileid uint64) (string, error)
	Contents(fileid uint64) ([]byte, error)
}

type indexAdapter struct {
	types.IndexReader
}

func (a *indexAdapter) Name(docid uint64) (string, error) {
	buf, err := a.GetStoredFieldValue(docid, "filename")
	if err != nil {
		return "", err
	}
	return string(buf), nil
}
func (a *indexAdapter) Contents(docid uint64) ([]byte, error) {
	return a.GetStoredFieldValue(docid, "body")
}

func indexDir() string {
	f := os.Getenv("CSEARCHINDEX2")
	if f != "" {
		return f
	}
	var home string
	home = os.Getenv("HOME")
	if runtime.GOOS == "windows" && home == "" {
		home = os.Getenv("USERPROFILE")
	}
	return filepath.Clean(home + "/.csindex")
}

func Main() {
	flag.Parse()
	args := flag.Args()
	if len(args) != 1 {
		log.Fatalf("one arg is accepted")
	}

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err.Error())
		}
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	db, err := pebble.Open(indexDir(), &pebble.Options{})
	if err != nil {
		log.Fatal(err.Error())
	}

	ix := &indexAdapter{index.NewReader(db, "repo")}
	s := searcher.NewSexp(ix)
	results, err := s.Search(args[0])
	if err != nil {
		log.Fatalf("search err: %s", err)
	}
	if len(results) == 0 {
		log.Fatal("no results found")
	}
	for i, result := range results {
		log.Infof("result[%d]: %+v", i, result)
	}
}

func main() {
	Main()
	os.Exit(0)
}
