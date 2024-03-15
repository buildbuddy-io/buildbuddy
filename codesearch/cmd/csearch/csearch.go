// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"

	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/codesearch/query"
	"github.com/buildbuddy-io/buildbuddy/codesearch/regexp"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/cockroachdb/pebble"
)

var usageMessage = `usage: csearch [-c] [-f fileregexp] [-h] [-i] [-l] [-n] regexp

Csearch behaves like grep over all indexed files, searching for regexp,
an RE2 (nearly PCRE) regular expression.

The -c, -h, -i, -l, and -n flags are as in grep, although note that as per Go's
flag parsing convention, they cannot be combined: the option pair -i -n 
cannot be abbreviated to -in.

The -f flag restricts the search to files whose names match the RE2 regular
expression fileregexp.

Csearch relies on the existence of an up-to-date index created ahead of time.
To build or rebuild the index that csearch uses, run:

	cindex path...

where path... is a list of directories or individual files to be included in the index.
If no index exists, this command creates one.  If an index already exists, cindex
overwrites it.  Run cindex -help for more.

Csearch uses the index stored in $CSEARCHINDEX or, if that variable is unset or
empty, $HOME/.csearchindex.
`

func usage() {
	fmt.Fprint(os.Stderr, usageMessage)
	os.Exit(2)
}

var (
	fFlag       = flag.String("f", "", "search only files with names matching this regexp")
	iFlag       = flag.Bool("i", false, "case-insensitive search")
	verboseFlag = flag.Bool("verbose", false, "print extra information")
	bruteFlag   = flag.Bool("brute", false, "brute force - search all files in index")
	squery      = flag.Bool("squery", false, "use squery instead of posting query")
	noResults   = flag.Bool("noresults", false, "don't print results")
	cpuProfile  = flag.String("cpuprofile", "", "write cpu profile to this file")

	listMatchesOnly = flag.Bool("l", false, "list matching files only")
	matchCountsOnly = flag.Bool("c", false, "print match counts only")
	showLineNumbers = flag.Bool("n", false, "show line numbers")
	omitFileNames   = flag.Bool("h", false, "omit file names")

	matches bool
)

type IndexReader interface {
	PostingQuery(q *query.Query) ([]uint64, error)
	PostingQuerySX(sExpression []byte) ([]uint64, error)
	Name(fileid uint64) (string, error)
	Contents(fileid uint64) ([]byte, error)
}

type indexAdapter struct {
	*index.Reader
}

func (a *indexAdapter) Name(docid uint64) (string, error) {
	buf, err := a.Reader.GetStoredFieldValue(docid, "filename")
	if err != nil {
		return "", err
	}
	return string(buf), nil
}
func (a *indexAdapter) Contents(docid uint64) ([]byte, error) {
	return a.Reader.GetStoredFieldValue(docid, "content")
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

func runQuery(ix IndexReader, q *query.Query, fre *regexp.Regexp) []uint64 {
	var post []uint64
	var err error

	if *squery {
		post, err = ix.PostingQuerySX([]byte(q.SQuery()))
	} else if *bruteFlag {
		post, err = ix.PostingQuery(&query.Query{Op: query.QAll})
	} else {
		post, err = ix.PostingQuery(q)
	}
	if err != nil {
		log.Fatal(err.Error())
	}
	if *verboseFlag {
		log.Printf("post query identified %d possible files\n", len(post))
	}

	if *noResults {
		return nil
	}
	if fre != nil {
		fnames := make([]uint64, 0, len(post))
		for _, fileid := range post {
			name, err := ix.Name(fileid)
			if err != nil {
				log.Fatal(err.Error())
			}
			if fre.MatchString(name, true, true) < 0 {
				continue
			}
			fnames = append(fnames, fileid)
		}

		if *verboseFlag {
			log.Printf("filename regexp matched %d files\n", len(fnames))
		}
		post = fnames
	}
	return post
}

func Main() {
	g := regexp.Grep{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}

	g.AddFlags(*listMatchesOnly, *matchCountsOnly, *showLineNumbers, *omitFileNames)

	flag.Usage = usage
	flag.Parse()
	args := flag.Args()

	if len(args) != 1 {
		usage()
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

	pat := "(?m)" + args[0]
	if *iFlag {
		pat = "(?i)" + pat
	}
	re, err := regexp.Compile(pat)
	if err != nil {
		log.Fatal(err.Error())
	}
	g.Regexp = re

	var fre *regexp.Regexp
	if *fFlag != "" {
		fre, err = regexp.Compile(*fFlag)
		if err != nil {
			log.Fatal(err.Error())
		}
	}
	q := query.RegexpQuery(re.Syntax)
	if *verboseFlag {
		log.Printf("query: %s\n", q)
		log.Printf("squery: %s\n", q.SQuery())
	}

	db, err := pebble.Open(indexDir(), &pebble.Options{})
	if err != nil {
		log.Fatal(err.Error())
	}

	ix := &indexAdapter{index.NewReader(db, "repo")}

	post2 := runQuery(ix, q, fre)

	for _, fileid := range post2 {
		name, err := ix.Name(fileid)
		if err != nil {
			log.Fatal(err.Error())
		}
		buf, err := ix.Contents(fileid)
		if err != nil {
			log.Fatal(err.Error())
		}
		res, err := g.MakeResult(bytes.NewReader(buf), name)
		if err != nil {
			log.Fatal(err.Error())
		}
		fmt.Printf("%+v", res)
	}

	matches = g.Match
}

func main() {
	Main()
	if !matches {
		os.Exit(1)
	}
	os.Exit(0)
}
