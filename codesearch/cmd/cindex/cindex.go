// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/pprof"
	"sort"

	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/cockroachdb/pebble"
	"github.com/gabriel-vasile/mimetype"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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

var skipMime = regexp.MustCompile(`^audio/.*|video/.*|image/.*$`)

// Tuning constants for detecting text files.
// A file is assumed not to be a text file (and thus is not indexed)
// if it contains an invalid UTF-8 sequences, if it is longer than maxFileLength
// bytes, or if it contains more than maxTextTrigrams distinct trigrams.
const (
	maxFileLen      = 1 << 30
	maxLineLen      = 2000
	maxTextTrigrams = 20000
)

type namedField struct {
	ftype  index.FieldType
	name   string
	buf    []byte
	stored bool
}

func (f namedField) Type() index.FieldType { return f.ftype }
func (f namedField) Name() string          { return f.name }
func (f namedField) Contents() []byte      { return f.buf }
func (f namedField) Stored() bool          { return f.stored }
func (f namedField) String() string {
	var snippet string
	if len(f.buf) < 10 {
		snippet = string(f.buf)
	} else {
		snippet = string(f.buf[:10])
	}
	return fmt.Sprintf("field<type: %v, name: %q, buf: %q>", f.ftype, f.name, snippet)
}

type mapDocument struct {
	id     uint64
	fields map[string]namedField
}

func (d mapDocument) ID() uint64                    { return d.id }
func (d mapDocument) Field(name string) index.Field { return d.fields[name] }
func (d mapDocument) Fields() []string {
	fieldNames := make([]string, 0, len(d.fields))
	for name := range d.fields {
		fieldNames = append(fieldNames, name)
	}
	return fieldNames
}

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

type indexWriterAdaptor struct {
	*index.Writer
}

func (a *indexWriterAdaptor) AddFile(name string) error {
	// Open the file and read all contents to memory.
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	buf, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	// Compute the digest of the file.
	h := sha256.New()
	n, err := h.Write(buf)
	if err != nil {
		return err
	}
	d := &repb.Digest{
		Hash:      fmt.Sprintf("%x", h.Sum(nil)),
		SizeBytes: int64(n),
	}
	return a.AddFileByDigest(name, d, buf)
}

func (a *indexWriterAdaptor) AddFileByDigest(name string, digest *repb.Digest, contents []byte) error {
	if len(contents) > maxFileLen {
		log.Infof("%s: too long, ignoring\n", name)
		return nil
	}
	r := bytes.NewReader(contents)
	mtype, err := mimetype.DetectReader(r)
	if err == nil && skipMime.MatchString(mtype.String()) {
		log.Infof("%q: skipping (invalid mime type: %q)", name, mtype.String())
		return nil
	}
	r.Seek(0, 0)

	// Compute docid (first bytes from digest)
	hexBytes, err := hex.DecodeString(digest.GetHash())
	if err != nil {
		return err
	}

	doc := mapDocument{
		id: index.BytesToUint64(hexBytes[:8]),
		fields: map[string]namedField{
			"filename": namedField{index.TextField, "filename", []byte(name), true /*=stored*/},
			"body":     namedField{index.TextField, "body", contents, true /*=stored*/},
		},
	}
	return a.AddDocument(doc)
}

func main() {
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()

	log.Printf("Using pebble!")

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err.Error())
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
		log.Fatal(err.Error())
	}

	var ix indexWriter
	i, err := index.NewWriter(db, "repo")
	if err != nil {
		log.Fatal(err.Error())
	}
	ix = &indexWriterAdaptor{i}

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
					log.Fatal(err.Error())
				}
			}
			return nil
		})
	}
	log.Printf("flush index")
	if err := ix.Flush(); err != nil {
		log.Fatal(err.Error())
	}

	log.Printf("done")
}
