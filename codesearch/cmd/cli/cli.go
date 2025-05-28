package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/pprof"
	"slices"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/codesearch/github"
	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/codesearch/performance"
	"github.com/buildbuddy-io/buildbuddy/codesearch/posting"
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
	db, err := pebble.Open(indexDir, &pebble.Options{
		Merger: &pebble.Merger{
			Merge: posting.InitRoaringMerger,
			Name:  "roaringMerger",
		},
	})
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

				if err := github.AddFileToIndex(iw, repoURL, commitSHA, path, buf); err != nil {
					log.Infof("Skipping file %s: %s", path, err)
				}
			}
			return nil
		})
		github.SetLastIndexedCommitSha(iw, repoURL, commitSHA)
	}

	if err := iw.Flush(); err != nil {
		log.Fatal(err.Error())
	}

	err = iw.CompactDeletes()
	if err != nil {
		log.Fatalf("error compacting deletes: %s", err)
	}
	if err := iw.Flush(); err != nil {
		log.Fatal(err.Error())
	}
}

func handleSearch(ctx context.Context, args []string) {
	pat := args[0]

	db, err := pebble.Open(indexDir, &pebble.Options{
		Merger: &pebble.Merger{
			Merge: posting.InitRoaringMerger,
			Name:  "roaringMerger",
		},
	})
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()

	codesearcher := searcher.New(ctx, index.NewReader(ctx, db, getNamespace(), schema.GitHubFileSchema()))
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

	db, err := pebble.Open(indexDir, &pebble.Options{
		Merger: &pebble.Merger{
			Merge: posting.InitRoaringMerger,
			Name:  "roaringMerger",
		},
	})
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()

	ir := index.NewReader(ctx, db, getNamespace(), schema.GitHubFileSchema())

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
		doc := ir.GetStoredDocument(docID)
		filename := doc.Field(schema.FilenameField).Contents()
		fmt.Printf("%d (%q) matched fields: %s\n", docID, filename, strings.Join(docFields[docID], ", "))
	}
}

//[106 100 101 108 102 105 110 111 45 110 115 58 103 114 97 58 47 118 121 103 58 99 111 110 116 101 110 116], value: [34 0 0 0 0 0 0 0 0 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 1 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 2 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 3 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 4 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 5 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 6 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 7 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 8 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 9 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 10 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 11 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 12 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 13 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 14 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 15 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 16 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 17 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 18 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 19 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 20 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 21 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 22 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 23 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 24 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 25 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 26 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 27 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 28 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 29 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 30 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 31 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 32 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 33 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9]
//[106 100 101 108 102 105 110 111 45 110 115 58 103 114 97 58 47 118 121 103 58 99 111 110 116 101 110 116], value: [35 0 0 0 0 0 0 0 0 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 1 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 2 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 3 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 4 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 5 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 6 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 7 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 8 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 9 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 10 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 11 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 12 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 13 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 14 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 15 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 16 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 17 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 18 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 19 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 20 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 21 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 22 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 23 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 24 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 25 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 26 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 27 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 28 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 29 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 30 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 31 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 32 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 33 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 34 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9]
//[106 100 101 108 102 105 110 111 45 110 115 58 103 114 97 58 47 118 103 122 58 99 111 110 116 101 110 116], value: [36 0 0 0 0 0 0 0 0 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 1 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 2 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 3 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 4 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 5 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 6 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 7 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 8 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 9 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 10 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 11 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 12 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 13 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 14 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 15 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 16 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 17 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 18 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 19 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 20 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 21 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 22 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 23 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 24 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 25 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 26 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 27 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 28 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 29 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 30 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 31 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 32 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 33 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 34 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9 35 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9]

//[106 100 101 108 102 105 110 111 45 110 115 58 103 114 97 58 100 117 121 58 99 111 110 116 101 110 116], value: [1 0 0 0 0 0 0 0 55 0 0 0 58 48 0 0 1 0 0 0 0 0 2 0 16 0 0 0 220 0 49 5 214 9], err: <nil>
//[106 100 101 108 102 105 110 111 45 110 115 58 103 114 97 58 100 117 121 51 116 58 99 111 110 116 101 110 116], value: [1 0 0 0 0 0 0 0 56 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9]

// jdelfino-ns:gra:duy3t:content ([106 100 101 108 102 105 110 111 45 110 115 58 103 114 97 58 100 117 121 51 116 58 99 111 110 116 101 110 116]), value: [1 0 0 0 0 0 0 0 57 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9]
// jdelfino-ns:gra:duy3t0:content ([106 100 101 108 102 105 110 111 45 110 115 58 103 114 97 58 100 117 121 51 116 48 58 99 111 110 116 101 110 116]), value: [1 0 0 0 0 0 0 0 58 0 0 0 58 48 0 0 1 0 0 0 0 0 0 0 16 0 0 0 214 9]

// 2025/05/28 10:27:16.535 INF Complete, number of keys: 447438, keysize: 12817578, valsize: 21286548
// 2025/05/28 10:27:41.274 INF Complete, number of keys: 447442, keysize: 12817695, valsize: 21286726

//2025/05/28 10:35:19.099 INF Complete, number of keys: 650879, keysize: 18752729, valsize: 460580860
//2025/05/28 10:36:48.870 INF Complete, number of keys: 650897, keysize: 18753252, valsize: 466927255

//2025/05/28 10:55:30.066 INF gra: number of keys: 633285, keysize: 18146792, valsize: 473153136
//2025/05/28 10:55:30.090 INF doc: number of keys: 17635, keysize: 607150, valsize: 31854807
//2025/05/28 10:55:32.570 INF total number of keys: 650921, keysize: 18753956, valsize: 505007947

// 2025/05/28 10:56:37.576 INF gra: number of keys: 633285, keysize: 18146792, valsize: 479499668
// 2025/05/28 10:56:37.599 INF doc: number of keys: 17635, keysize: 607150, valsize: 31854807
// 2025/05/28 10:56:40.153 INF total number of keys: 650921, keysize: 18753956, valsize: 511354479

// [106 100 101 108 102 105 110 111 45 110 115 58 103 114 97 58 49 54 104 44 58 99 111 110 116 101 110 116]), value: [74 0 0 0 0 0 0 0 0 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 1 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 2 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 3 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 4 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 5 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 6 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 7 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 8 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 9 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 10 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 11 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 12 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 13 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 14 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 15 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 16 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 17 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 18 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 19 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 20 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 21 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 22 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 23 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 24 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 25 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 26 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 27 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 28 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 29 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 30 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 31 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 32 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 33 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 34 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 35 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 36 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 37 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 38 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 39 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 40 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 41 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 42 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 43 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 44 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 45 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 46 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 47 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 48 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 49 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 50 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 51 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 52 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 53 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 54 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 55 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 56 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 57 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 58 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 59 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 60 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 61 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 62 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 63 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 64 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 65 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 67 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 68 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 69 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 70 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 71 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 72 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 73 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 74 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9]
// [106 100 101 108 102 105 110 111 45 110 115 58 103 114 97 58 49 54 104 44 58 99 111 110 116 101 110 116]), value: [75 0 0 0 0 0 0 0 0 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 1 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 2 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 3 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 4 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 5 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 6 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 7 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 8 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 9 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 10 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 11 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 12 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 13 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 14 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 15 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 16 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 17 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 18 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 19 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 20 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 21 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 22 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 23 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 24 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 25 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 26 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 27 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 28 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 29 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 30 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 31 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 32 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 33 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 34 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 35 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 36 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 37 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 38 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 39 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 40 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 41 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 42 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 43 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 44 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 45 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 46 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 47 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 48 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 49 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 50 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 51 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 52 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 53 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 54 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 55 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 56 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 57 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 58 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 59 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 60 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 61 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 62 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 63 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 64 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 65 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 67 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 68 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 69 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 70 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 71 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 72 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 73 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 74 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9 75 0 0 0 58 48 0 0 1 0 0 0 0 0 5 0 16 0 0 0 42 9 43 9 44 9 48 9 50 9 51 9]
