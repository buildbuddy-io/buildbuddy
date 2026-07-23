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

	"github.com/buildbuddy-io/buildbuddy/codesearch/annotations"
	"github.com/buildbuddy-io/buildbuddy/codesearch/github"
	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/codesearch/indexprofile"
	"github.com/buildbuddy-io/buildbuddy/codesearch/navindex"
	"github.com/buildbuddy-io/buildbuddy/codesearch/performance"
	"github.com/buildbuddy-io/buildbuddy/codesearch/query"
	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/codesearch/searcher"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	indexCmd       = flag.NewFlagSet("index", flag.ExitOnError)
	searchCmd      = flag.NewFlagSet("search", flag.ExitOnError)
	squeryCmd      = flag.NewFlagSet("squery", flag.ExitOnError)
	decorationsCmd = flag.NewFlagSet("decorations", flag.ExitOnError)
	lookupCmd      = flag.NewFlagSet("lookup", flag.ExitOnError)
	docsCmd        = flag.NewFlagSet("docs", flag.ExitOnError)
	xrefsCmd       = flag.NewFlagSet("xrefs", flag.ExitOnError)

	subcommands = map[string]*flag.FlagSet{
		indexCmd.Name():       indexCmd,
		searchCmd.Name():      searchCmd,
		squeryCmd.Name():      squeryCmd,
		decorationsCmd.Name(): decorationsCmd,
		lookupCmd.Name():      lookupCmd,
		docsCmd.Name():        docsCmd,
		xrefsCmd.Name():       xrefsCmd,
	}

	indexDir     string
	cpuProfile   string
	heapProfile  string
	indexProfile bool
	namespace    string

	reset       = indexCmd.Bool("reset", false, "Delete the index and start fresh")
	results     = searchCmd.Int("results", 100, "Print this many results")
	offset      = searchCmd.Int("offset", 0, "Start printing results this far in")
	snippets    = searchCmd.Int("snippets", 5, "Print this many snippets per result")
	resolveRefs = decorationsCmd.Bool("resolve", false, "Also resolve each reference's ticket to its definition location")
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
		case indexCmd.Name(), searchCmd.Name(), squeryCmd.Name(), decorationsCmd.Name(), lookupCmd.Name(), docsCmd.Name(), xrefsCmd.Name():
			flagset.StringVar(&indexDir, "index_dir", "", "Path to Index Directory")
			flagset.StringVar(&cpuProfile, "cpu_profile", "", "Path to dump a CPU profile")
			flagset.StringVar(&heapProfile, "heap_profile", "", "Path to dump a heap profile")
			flagset.StringVar(&namespace, "namespace", "", "Namespace to index/search/squery in")
			if cmdName == indexCmd.Name() {
				flagset.BoolVar(&indexProfile, "index_profile", false, "Print indexing phase timing and counters")
			}
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

	if indexProfile {
		p := indexprofile.Start()
		defer func() {
			indexprofile.Stop()
			p.PrettyPrint()
		}()
	}

	ctx := performance.WrapContext(context.Background())

	switch cmd.Name() {
	case indexCmd.Name():
		handleIndex(cmd.Args())
	case searchCmd.Name():
		handleSearch(ctx, cmd.Args())
	case squeryCmd.Name():
		handleSquery(ctx, cmd.Args())
	case decorationsCmd.Name():
		handleDecorations(ctx, cmd.Args())
	case lookupCmd.Name():
		handleLookup(ctx, cmd.Args())
	case docsCmd.Name():
		handleDocs(ctx, cmd.Args())
	case xrefsCmd.Name():
		handleXrefs(ctx, cmd.Args())
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
	db, err := index.OpenPebbleDB(indexDir)
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
		// We have a checkout on disk, so read the module path from go.mod at
		// the repo root (absent/unparsable -> "", which disables Go import
		// identities).
		goMod, _ := os.ReadFile(filepath.Join(dir, "go.mod"))
		// Filenames are stored repo-relative (relative to dir), matching the
		// server's convention so path-based identities (TS/JS import_id) join.
		// The RepoContext therefore takes an empty root — the filenames handed
		// to Extract are already relative.
		rctx := annotations.NewRepoContext("", annotations.GoModulePath(goMod))
		log.Printf("indexing dir: %q", dir)
		stopWalk := indexprofile.Timer(indexprofile.PhaseWalk)
		walkErr := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			indexprofile.Add(indexprofile.CounterPathsVisited, 1)
			if _, elem := filepath.Split(path); elem != "" {
				// Skip various temporary or "hidden" files or directories.
				if elem[0] == '.' || elem[0] == '#' || elem[0] == '~' || elem[len(elem)-1] == '~' {
					indexprofile.Add(indexprofile.CounterHiddenPathsSkipped, 1)
					if info != nil && info.IsDir() {
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
				indexprofile.Add(indexprofile.CounterFilesSeen, 1)
				stopRead := indexprofile.Timer(indexprofile.PhaseReadFile)
				buf, err := os.ReadFile(path)
				stopRead()
				if err != nil {
					return err
				}
				indexprofile.Add(indexprofile.CounterFilesRead, 1)
				indexprofile.Add(indexprofile.CounterBytesRead, int64(len(buf)))

				// Store the repo-relative, slash-separated path as the filename
				// so it shares a coordinate system with import_id terms.
				relPath := path
				if rel, err := filepath.Rel(dir, path); err == nil {
					relPath = filepath.ToSlash(rel)
				}
				if err := github.AddFileToIndex(iw, rctx, repoURL, commitSHA, relPath, buf); err != nil {
					log.Infof("Skipping file %s: %s", relPath, err)
				}
			}
			return nil
		})
		stopWalk()
		if walkErr != nil {
			log.Fatal(err.Error())
		}
		github.SetRepoMetadata(iw, repoURL, commitSHA, rctx.GoModulePath())
	}

	if err := iw.Flush(); err != nil {
		log.Fatal(err.Error())
	}
}

func handleSearch(ctx context.Context, args []string) {
	pat := args[0]

	db, err := index.OpenPebbleDB(indexDir)
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

	db, err := index.OpenPebbleDB(indexDir)
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

// handleDecorations prints the click-through references in an indexed file
// (the tree-sitter nav Decorations flow). With -resolve, each reference's
// ticket is also resolved to its definition location, exercising the full
// decorate -> go-to-definition round trip from the command line.
func handleDecorations(ctx context.Context, args []string) {
	if len(args) != 1 {
		log.Fatalf("usage: decorations [-resolve] <file>")
	}
	path := args[0]

	db, err := index.OpenPebbleDB(indexDir)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()
	// A plain reader context: nav runs several queries and doesn't need the
	// per-query performance tracker (which warns on repeated metric keys).
	r := index.NewReader(context.Background(), db, getNamespace(), schema.GitHubFileSchema())

	f, ok := navindex.FindFile(ctx, r, path)
	if !ok {
		log.Fatalf("file %q not found in namespace %q", path, getNamespace())
	}
	refs, err := annotations.Decorate(ctx, f.Lang, f.Content, annotations.NavOptions{
		SelfImportID: f.SelfImportID,
		Path:         path,
		InRepo:       f.InRepo,
	})
	if err != nil {
		log.Fatal(err.Error())
	}
	if len(refs) == 0 {
		fmt.Println("no references found")
		return
	}

	lk := &navindex.DefLookup{R: r}
	for _, ref := range refs {
		text := string(f.Content[ref.Start.Byte:ref.End.Byte])
		fmt.Printf("%d:%d-%d:%d\t%-16s\t%s\n",
			ref.Start.Line, ref.Start.Col, ref.End.Line, ref.End.Col, text, ref.TargetTicket)
		if !*resolveRefs {
			continue
		}
		locs, err := annotations.Resolve(ctx, lk, ref.TargetTicket)
		if err != nil {
			log.Fatal(err.Error())
		}
		if len(locs) == 0 {
			fmt.Println("\t-> (no definition)")
			continue
		}
		for _, loc := range locs {
			fmt.Printf("\t-> %s:%d:%d\n", loc.Path, loc.Start.Line, loc.Start.Col)
		}
	}
}

// handleLookup resolves a single tree-sitter nav ticket to its definition
// location(s) (the CrossReferences / go-to-definition flow).
func handleLookup(ctx context.Context, args []string) {
	if len(args) != 1 {
		log.Fatalf("usage: lookup <ticket>")
	}
	ticket := args[0]

	db, err := index.OpenPebbleDB(indexDir)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()
	r := index.NewReader(context.Background(), db, getNamespace(), schema.GitHubFileSchema())

	locs, err := annotations.Resolve(ctx, &navindex.DefLookup{R: r}, ticket)
	if err != nil {
		log.Fatal(err.Error())
	}
	if len(locs) == 0 {
		fmt.Println("no definition found")
		return
	}
	for _, loc := range locs {
		fmt.Printf("%s:%d:%d\n", loc.Path, loc.Start.Line, loc.Start.Col)
	}
}

// handleDocs prints the hover documentation a ticket resolves to: the
// declaration kind, location, signature, and leading doc comment.
func handleDocs(ctx context.Context, args []string) {
	if len(args) != 1 {
		log.Fatalf("usage: docs <ticket>")
	}
	ticket := args[0]

	db, err := index.OpenPebbleDB(indexDir)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()
	r := index.NewReader(context.Background(), db, getNamespace(), schema.GitHubFileSchema())

	defs, err := annotations.Describe(ctx, &navindex.DefLookup{R: r}, ticket)
	if err != nil {
		log.Fatal(err.Error())
	}
	if len(defs) == 0 {
		fmt.Println("no definition found")
		return
	}
	for _, d := range defs {
		fmt.Printf("%s\t%s:%d:%d\n", d.Kind, d.Path, d.Start.Line, d.Start.Col)
		if d.Signature != "" {
			fmt.Printf("  %s\n", d.Signature)
		}
		for line := range strings.SplitSeq(d.Doc, "\n") {
			if line != "" {
				fmt.Printf("  // %s\n", line)
			}
		}
	}
}

// handleXrefs prints the references-panel data for a ticket: its definition(s)
// and every use site (the find-usages flow).
func handleXrefs(ctx context.Context, args []string) {
	if len(args) != 1 {
		log.Fatalf("usage: xrefs <ticket>")
	}
	ticket := args[0]

	db, err := index.OpenPebbleDB(indexDir)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()
	r := index.NewReader(context.Background(), db, getNamespace(), schema.GitHubFileSchema())
	lk := &navindex.DefLookup{R: r}

	defs, err := annotations.Describe(ctx, lk, ticket)
	if err != nil {
		log.Fatal(err.Error())
	}
	fmt.Printf("Definitions (%d):\n", len(defs))
	for _, d := range defs {
		fmt.Printf("  %s:%d:%d\t%s\n", d.Path, d.Start.Line, d.Start.Col, d.Signature)
	}

	refs, err := annotations.FindReferences(ctx, lk, ticket)
	if err != nil {
		log.Fatal(err.Error())
	}
	fmt.Printf("References (%d):\n", len(refs))
	for _, rf := range refs {
		fmt.Printf("  %s:%d:%d\t%s\n", rf.Path, rf.Start.Line, rf.Start.Col, rf.Snippet)
	}
}
