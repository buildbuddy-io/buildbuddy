package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime/pprof"
	"slices"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/codesearch/performance"
	"github.com/buildbuddy-io/buildbuddy/codesearch/query"
	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/codesearch/searcher"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/cockroachdb/pebble"
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

	indexDir    string
	cpuProfile  string
	heapProfile string
	namespace   string

	reset    = indexCmd.Bool("reset", false, "Delete the index and start fresh")
	results  = searchCmd.Int("results", 100, "Print this many results")
	offset   = searchCmd.Int("offset", 0, "Start printing results this far in")
	snippets = searchCmd.Int("snippets", 5, "Print this many snippets per result")

	skipMime = regexp.MustCompile(`^audio/.*|video/.*|image/.*|application/gzip$`)
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
			flagset.StringVar(&heapProfile, "heap_profile", "", "Path to dump a heap profile")
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

	if heapProfile != "" {
		f, err := os.Create(heapProfile)
		if err != nil {
			log.Fatal(err.Error())
		}
		defer f.Close()
		defer pprof.WriteHeapProfile(f)
	}

	ctx := performance.WrapContext(context.Background())

	switch cmd.Name() {
	case indexCmd.Name():
		handleIndex(cmd.Args())
	case searchCmd.Name():
		handleSearch(ctx, cmd.Args())
	case squeryCmd.Name():
		handleSquery(ctx, cmd.Args())
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

func extractRepoURL(dir string) *git.RepoURL {
	cmd := exec.Command("git", "config", "--get", "remote.origin.url")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err == nil {
		if repoURL, err := git.ParseGitHubRepoURL(strings.TrimSpace(string(out))); err == nil {
			return repoURL
		}
	}
	return &git.RepoURL{}
}

func extractGitSHA(dir string) string {
	cmd := exec.Command("git", "rev-parse", "--verify", "HEAD")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err == nil {
		return strings.TrimSpace(string(out))
	}
	return ""
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

	for _, dir := range args {
		repoURL := extractRepoURL(dir)
		commitSHA := extractGitSHA(dir)
		log.Printf("indexing dir: %q", dir)
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
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
				buf, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				doc, err := schema.MakeDocument(path, commitSHA, repoURL, buf)
				if err != nil {
					log.Info(err.Error())
					return nil
				}

				if err := iw.UpdateDocument(doc.Field(schema.IDField), doc); err != nil {
					log.Fatal(err.Error())
				}
			}
			return nil
		})
	}

	if err := iw.Flush(); err != nil {
		log.Fatal(err.Error())
	}
}

func handleSearch(ctx context.Context, args []string) {
	pat := args[0]

	db, err := pebble.Open(indexDir, &pebble.Options{})
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()

	codesearcher := searcher.New(ctx, index.NewReader(ctx, db, getNamespace()))
	q, err := query.NewReQuery(ctx, pat)
	if err != nil {
		log.Fatal(err.Error())
	}
	docs, err := codesearcher.Search(q, *results, *offset)
	if err != nil {
		log.Fatal(err.Error())
	}

	if t := performance.TrackerFromContext(ctx); t != nil {
		t.PrettyPrint()
	}

	highlighter := q.Highlighter()
	for _, doc := range docs {
		regions := highlighter.Highlight(doc)
		if len(regions) == 0 {
			log.Warningf("Skipping %q, no regions!!!", doc.Field(schema.FilenameField).Contents())
			continue
		}

		dedupedRegions := make([]types.HighlightedRegion, 0, len(regions))
		lastLine := -1
		lastField := ""
		for _, region := range regions {
			if region.Line() == lastLine && region.FieldName() == lastField {
				continue
			}
			if region.FieldName() != "content" {
				continue
			}
			dedupedRegions = append(dedupedRegions, region)
			lastLine = region.Line()
			lastField = region.FieldName()
		}

		fmt.Printf("%q [%d matches]\n", doc.Field(schema.FilenameField).Contents(), len(dedupedRegions))
		if len(dedupedRegions) > *snippets {
			dedupedRegions = dedupedRegions[:*snippets]
		}
		for i, region := range dedupedRegions {
			// if the prev region abuts this one, don't print leading lines.
			precedingLines := 1
			if i-1 >= 0 && dedupedRegions[i-1].Line() == region.Line()-1 {
				precedingLines = 0
			}
			// if next region abuts this one, don't print trailing lines.
			trailingLines := 1
			if i+1 < len(dedupedRegions) && dedupedRegions[i+1].Line() == region.Line()+1 {
				trailingLines = 0
			}
			scanner := bufio.NewScanner(strings.NewReader(region.CustomSnippet(precedingLines, trailingLines)))
			for scanner.Scan() {
				fmt.Print("  " + scanner.Text() + "\n")
			}
		}
	}
}

func handleSquery(ctx context.Context, args []string) {
	pat := args[0]

	db, err := pebble.Open(indexDir, &pebble.Options{})
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()

	ir := index.NewReader(ctx, db, getNamespace())
	matches, err := ir.RawQuery(pat)
	if err != nil {
		log.Fatal(err.Error())
	}
	if t := performance.TrackerFromContext(ctx); t != nil {
		t.PrettyPrint()
	}
	allDocIDs := make([]uint64, 0)
	for _, match := range matches {
		allDocIDs = append(allDocIDs, match.Docid())
	}
	slices.Sort(allDocIDs)
	docIDs := slices.Compact(allDocIDs)
	numDocs := len(docIDs)

	docFields := make(map[uint64][]string, numDocs)
	for _, match := range matches {
		for _, fieldName := range match.FieldNames() {
			if len(fieldName) == 0 {
				continue
			}
			docFields[match.Docid()] = append(docFields[match.Docid()], fieldName)
		}
	}
	for _, docID := range docIDs {
		doc, err := ir.GetStoredDocument(docID)
		if err != nil {
			log.Fatal(err.Error())
		}
		filename := doc.Field(schema.FilenameField).Contents()
		fmt.Printf("%d (%q) matched fields: %s\n", docID, filename, strings.Join(docFields[docID], ", "))
	}
}
