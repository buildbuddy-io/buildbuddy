package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime/pprof"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/codesearch/query"
	"github.com/buildbuddy-io/buildbuddy/codesearch/searcher"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/cockroachdb/pebble"
	"github.com/gabriel-vasile/mimetype"
	"github.com/go-enry/go-enry/v2"

	xxhash "github.com/cespare/xxhash/v2"
)

var (
	indexCmd  = flag.NewFlagSet("index", flag.ExitOnError)
	searchCmd = flag.NewFlagSet("search", flag.ExitOnError)
	squeryCmd = flag.NewFlagSet("squery", flag.ExitOnError)

	subcommands = map[string]*flag.FlagSet{
		indexCmd.Name():  indexCmd,
		searchCmd.Name(): searchCmd,
		squeryCmd.Name(): squeryCmd,
	}

	indexDir   string
	cpuProfile string
	namespace  string

	reset   = indexCmd.Bool("reset", false, "Delete the index and start fresh")
	results = searchCmd.Int("results", 100, "Print this many results")

	skipMime = regexp.MustCompile(`^audio/.*|video/.*|image/.*$`)
)

const (
	maxFileLen = 10_000_000

	// The maximum amount of bytes from a file to use for language and
	// mimetype detection.
	detectionBufferSize = 1000
)

func printMainHelpAndDie() {
	fmt.Println("usage: cli <command> [<args>]")
	fmt.Println("Please specify a subcommand:")
	for subcommand := range subcommands {
		fmt.Printf("\t%s\n", subcommand)
	}
	os.Exit(1)
}

func setupCommonFlags() {
	for cmdName, flagset := range subcommands {
		switch cmdName {
		case indexCmd.Name(), searchCmd.Name(), squeryCmd.Name():
			flagset.StringVar(&indexDir, "index_dir", "", "Path to Index Directory")
			flagset.StringVar(&cpuProfile, "cpu_profile", "", "Path to dump a CPU profile")
			flagset.StringVar(&namespace, "namespace", "", "Namespace to index/search/squery in")
		}
	}
}

func main() {
	setupCommonFlags()
	if len(os.Args) < 2 {
		printMainHelpAndDie()
	}
	cmd := subcommands[os.Args[1]]
	if cmd == nil {
		log.Fatalf("unknown subcommand '%s', see help for more details.", os.Args[1])
	}

	if err := cmd.Parse(os.Args[2:]); err != nil {
		log.Fatalf("error parsing command: %s", err)
	}

	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			log.Fatal(err.Error())
		}
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	switch cmd.Name() {
	case indexCmd.Name():
		handleIndex(cmd.Args())
	case searchCmd.Name():
		handleSearch(cmd.Args())
	case squeryCmd.Name():
		handleSquery(cmd.Args())
	default:
		log.Fatalf("no handler for command %q", cmd.Name())
	}
}

func getNamespace() string {
	if namespace != "" {
		return namespace
	}
	return os.Getenv("USER") + "-ns"
}

func makeDoc(name string) (types.Document, error) {
	// Open the file and read all contents to memory.
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	// Skip long files.
	if len(buf) > maxFileLen {
		return nil, fmt.Errorf("%s: too long, ignoring\n", name)
	}

	// Compute a hash of the file.
	docID := xxhash.Sum64(buf)

	var shortBuf []byte
	if len(buf) > detectionBufferSize {
		shortBuf = buf[:detectionBufferSize]
	} else {
		shortBuf = buf
	}

	// Check the mimetype and skip if bad.
	mtype, err := mimetype.DetectReader(bytes.NewReader(shortBuf))
	if err == nil && skipMime.MatchString(mtype.String()) {
		return nil, fmt.Errorf("%q: skipping (invalid mime type: %q)", name, mtype.String())
	}

	// Compute filetype
	lang := strings.ToLower(enry.GetLanguage(filepath.Base(name), shortBuf))
	doc := types.NewMapDocument(
		docID,
		map[string]types.NamedField{
			"filename": types.NewNamedField(types.TrigramField, "filename", []byte(name), true /*=stored*/),
			"content":  types.NewNamedField(types.TrigramField, "content", buf, true /*=stored*/),
			"language": types.NewNamedField(types.StringTokenField, "language", []byte(lang), true /*=stored*/),
		},
	)
	return doc, nil
}

func handleIndex(args []string) {
	if *reset {
		os.RemoveAll(indexDir)
	}
	db, err := pebble.Open(indexDir, &pebble.Options{})
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()

	iw, err := index.NewWriter(db, getNamespace())
	if err != nil {
		log.Fatal(err.Error())
	}
	defer iw.Flush()

	for _, arg := range args {
		log.Printf("indexing dir: %q", arg)
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
				doc, err := makeDoc(path)
				if err != nil {
					log.Info(err.Error())
					return nil
				}

				if err := iw.AddDocument(doc); err != nil {
					log.Fatal(err.Error())
				}
			}
			return nil
		})
	}
}

func handleSearch(args []string) {
	pat := args[0]

	db, err := pebble.Open(indexDir, &pebble.Options{})
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()

	codesearcher := searcher.New(index.NewReader(db, getNamespace()))
	q, err := query.NewReQuery(pat, *results)
	if err != nil {
		log.Fatal(err.Error())
	}
	docs, err := codesearcher.Search(q)
	if err != nil {
		log.Fatal(err.Error())
	}
	highlighter := q.GetHighlighter()
	for _, doc := range docs {
		regions := highlighter.Highlight(doc)
		if len(regions) == 0 {
			continue
		}
		fmt.Printf("%q [%d matches]\n", doc.Field("filename").Contents(), len(regions))
		for _, region := range regions {
			scanner := bufio.NewScanner(strings.NewReader(region.String()))
			for scanner.Scan() {
				fmt.Print("  " + scanner.Text() + "\n")
			}
		}
	}
}

func handleSquery(args []string) {
	pat := []byte(args[0])

	db, err := pebble.Open(indexDir, &pebble.Options{})
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()

	ir := index.NewReader(db, getNamespace())
	fieldMapPostings, err := ir.RawQuery(pat)
	if err != nil {
		log.Fatal(err.Error())
	}

	for field, pl := range fieldMapPostings {
		fmt.Printf("%s => %d\n", field, len(pl))
	}
}
