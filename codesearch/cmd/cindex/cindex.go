// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"

	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/cockroachdb/pebble"
)

var usageMessage = `usage: cindex [-list] [-reset] [path...]

Cindex prepares the trigram index for use by csearch.  The index is the
file named by $CSEARCHINDEX, or else $HOME/.csearchindex.

The simplest invocation is

	cindex path...

which adds the file or directory tree named by each path to the index.
For example:

	cindex $HOME/src /usr/include

or, equivalently:

	cindex $HOME/src
	cindex /usr/include

If cindex is invoked with no paths, it reindexes the paths that have
already been added, in case the files have changed.  Thus, 'cindex' by
itself is a useful command to run in a nightly cron job.

The -list flag causes cindex to list the paths it has indexed and exit.

By default cindex adds the named paths to the index but preserves 
information about other paths that might already be indexed
(the ones printed by cindex -list).  The -reset flag causes cindex to
delete the existing index before indexing the new paths.
With no path arguments, cindex -reset removes the index.
`

func usage() {
	fmt.Fprint(os.Stderr, usageMessage)
	os.Exit(2)
}

var (
	resetFlag   = flag.Bool("reset", false, "discard existing index")
	verboseFlag = flag.Bool("verbose", false, "print extra information")
	cpuProfile  = flag.String("cpuprofile", "", "write cpu profile to this file")
)

type indexReader interface {
	Paths() []string
	Close()
}

type indexWriter interface {
	AddFile(string) error
	Flush() error
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

func main() {
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()

	log.Printf("Using pebble!")

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if *resetFlag {
		os.RemoveAll(indexDir())
	}

	// Translate paths to absolute paths so that we can
	// generate the file list in sorted order.
	for i, arg := range args {
		a, err := filepath.Abs(arg)
		if err != nil {
			log.Printf("%s: %s", arg, err)
			args[i] = ""
			continue
		}
		args[i] = a
	}
	sort.Strings(args)

	for len(args) > 0 && args[0] == "" {
		args = args[1:]
	}

	db, err := pebble.Open(indexDir(), &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}

	var ix indexWriter
	i, err := index.CreateSimple(db)
	if err != nil {
		log.Fatal(err)
	}
	ix = i

	for _, arg := range args {
		log.Printf("index %s", arg)
		filepath.Walk(arg, func(path string, info os.FileInfo, err error) error {
			if _, elem := filepath.Split(path); elem != "" {
				// Skip various temporary or "hidden" files or directories.
				if elem[0] == '.' || elem[0] == '#' || elem[0] == '~' || elem[len(elem)-1] == '~' {
					if info.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
			}
			if err != nil {
				log.Printf("%s: %s", path, err)
				return nil
			}
			if info != nil && info.Mode()&os.ModeType == 0 {
				if err := ix.AddFile(path); err != nil {
					log.Fatal(err)
				}
			}
			return nil
		})
	}
	log.Printf("flush index")
	if err := ix.Flush(); err != nil {
		log.Fatal(err)
	}

	log.Printf("done")
}
