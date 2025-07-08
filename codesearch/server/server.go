package server

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/codesearch/github"
	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/codesearch/kythestorage"
	"github.com/buildbuddy-io/buildbuddy/codesearch/performance"
	"github.com/buildbuddy-io/buildbuddy/codesearch/query"
	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/codesearch/searcher"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/lockmap"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"golang.org/x/sync/errgroup"

	"kythe.io/kythe/go/services/filetree"
	"kythe.io/kythe/go/services/graph"
	"kythe.io/kythe/go/services/xrefs"
	"kythe.io/kythe/go/serving/identifiers"
	"kythe.io/kythe/go/storage/keyvalue"
	"kythe.io/kythe/go/storage/table"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/index"
	srpb "github.com/buildbuddy-io/buildbuddy/proto/search"
	flagyaml "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
	ftsrv "kythe.io/kythe/go/serving/filetree"
	gsrv "kythe.io/kythe/go/serving/graph"
	xsrv "kythe.io/kythe/go/serving/xrefs"

	kgpb "kythe.io/kythe/proto/graph_go_proto"
	kxpb "kythe.io/kythe/proto/xref_go_proto"
)

const (
	// Used to control how many results may be returned at a time.
	defaultNumResults = 10
	maxNumResults     = 1000
)

var isAlphaNumPath = regexp.MustCompile(`^[A-Za-z/0-9]*$`).MatchString

func init() {
	flagyaml.IgnoreFlagForYAML("experimental_cross_reference_indirection_kinds")
}

func New(env environment.Env, rootDirectory, scratchDirectory string) (*codesearchServer, error) {
	ctx := context.Background()

	if err := disk.EnsureDirectoryExists(scratchDirectory); err != nil {
		return nil, err
	}
	db, err := index.OpenPebbleDB(rootDirectory)
	if err != nil {
		return nil, err
	}

	kdb := kythestorage.OpenRaw(env, db)
	tbl := &table.KVProto{DB: kdb}
	gs := gsrv.NewCombinedTable(tbl)
	ft := &ftsrv.Table{Proto: tbl, PrefixedKeys: true}
	it := &identifiers.Table{Proto: tbl}
	xs := xsrv.NewService(ctx, kdb)

	return &codesearchServer{
		env:              env,
		db:               db,
		scratchDirectory: scratchDirectory,
		repoLocks:        lockmap.New(),

		kdb: kdb,
		xs:  xs,
		gs:  gs,
		it:  it,
		ft:  ft,
	}, nil
}

type codesearchServer struct {
	env              environment.Env
	db               *pebble.DB
	scratchDirectory string

	repoLocks lockmap.Locker

	// Kythe services.
	kdb keyvalue.DB
	xs  xrefs.Service
	gs  graph.Service
	it  identifiers.Service
	ft  filetree.Service
}

// apiArchiveURL takes a url like https://github.com/buildbuddy-io/buildbuddy
// and a commit SHA, username, and access token, and generates a github API zip
// archive download URL like:
// https://api.github.com/repos/buildbuddy-io/buildbuddy-internal/zipball/sha12312312313
func apiArchiveURL(repoURL, commitSHA, username, accessToken string) (string, error) {
	authRepoURL, err := git.AuthRepoURL(repoURL, username, accessToken)
	if err != nil {
		return "", err
	}
	u, err := url.Parse(authRepoURL)
	if err != nil {
		return "", err
	}
	reposPath, err := url.JoinPath("/repos/", u.Path)
	if err != nil {
		return "", err
	}
	u.Path = reposPath
	u.Host = "api.github.com"
	u = u.JoinPath("/zipball/", commitSHA)
	return u.String(), nil
}

// getUserNamespace forces the namespace to match the authenticated user, but
// allows for clients to use a custom namespace within that subspace.
func (css *codesearchServer) getUserNamespace(ctx context.Context, requestedNamespace string) (string, error) {
	if !isAlphaNumPath(requestedNamespace) {
		return "", status.InvalidArgumentError("namespace must match a/b/c")
	}
	gid, err := prefix.UserPrefix(ctx, css.env.GetAuthenticator())
	if err != nil {
		return "", err
	}
	namespace := filepath.Join(gid, requestedNamespace)
	return namespace, nil
}

func (css *codesearchServer) incrementalUpdate(ctx context.Context, req *inpb.IndexRequest) (*inpb.IndexResponse, error) {
	repoURLString := req.GetGitRepo().GetRepoUrl()
	repoURL, err := git.ParseGitHubRepoURL(repoURLString)
	if err != nil {
		return nil, err
	}
	log.Infof("Starting incremental update %q", repoURL)

	r := index.NewReader(ctx, css.db, req.GetNamespace(), schema.MetadataSchema())
	lastIndexedSHA, err := github.GetLastIndexedCommitSha(r, repoURL)
	if err != nil {
		if status.IsNotFoundError(err) {
			return nil, status.InvalidArgumentError(fmt.Sprintf("No previous indexing found for repo %s. Use FULL_REINDEX instead of INCREMENTAL_REINDEX.", repoURL))
		} else {
			return nil, err
		}
	}

	commits := req.GetUpdate().GetCommits()

	if len(commits) == 0 {
		// Nothing to do, bye
		return &inpb.IndexResponse{}, nil
	}

	firstIndexToProcess := -1
	for i, commit := range commits {
		// We currently only support sequential commits, with no gaps.
		// We could do a topological sort, but we just don't need that right now.
		if i >= 1 && commit.GetParentSha() != commits[i-1].GetSha() {
			return nil, status.InvalidArgumentErrorf("commits must be sequential. Commit %s has parent %s, but is not preceded by that commit", commit.GetSha(), commit.GetParentSha())
		}
		if commit.GetParentSha() == lastIndexedSHA {
			firstIndexToProcess = i
		}
	}
	if firstIndexToProcess == -1 {
		return nil, status.InvalidArgumentErrorf("last processed commit was %s; no commits found with this parent", lastIndexedSHA)
	}

	commits = commits[firstIndexToProcess:]

	iw, err := index.NewWriter(css.db, req.GetNamespace())
	if err != nil {
		return nil, err
	}

	for _, commit := range commits {
		if err := github.ProcessCommit(iw, repoURL, commit); err != nil {
			return nil, status.InternalErrorf("failed to process commit %s: %v", commit.GetSha(), err)
		}
	}

	err = github.SetLastIndexedCommitSha(iw, repoURL, commits[len(commits)-1].GetSha())
	if err != nil {
		return nil, fmt.Errorf("failed to finalize update: %w", err)
	}

	if err := iw.Flush(); err != nil {
		return nil, err
	}

	log.Infof("finished incremental update on %s from %s to %s", repoURL, commits[0].GetSha(), commits[len(commits)-1].GetSha())

	return &inpb.IndexResponse{}, nil
}

func (css *codesearchServer) fullyReindex(_ context.Context, req *inpb.IndexRequest) (*inpb.IndexResponse, error) {
	// TODO(jdelfino): This implementation does not remove files which have been deleted since the
	// the previously indexed version of the repository. Note that a namespace can include multiple
	// repos, so implementing this would require explicit iteration and deletion of each document
	// tagged with the given repo URL.
	commitSHA := req.GetRepoState().GetCommitSha()
	username := req.GetGitRepo().GetUsername()
	accessToken := req.GetGitRepo().GetAccessToken()

	repoURL, err := git.ParseGitHubRepoURL(req.GetGitRepo().GetRepoUrl())
	if err != nil {
		return nil, err
	}
	log.Infof("Starting index of %q@%s", repoURL, commitSHA)

	archiveURL, err := apiArchiveURL(repoURL.String(), commitSHA, username, accessToken)
	if err != nil {
		return nil, err
	}

	httpRsp, err := http.Get(archiveURL)
	if err != nil {
		return nil, err
	}
	defer httpRsp.Body.Close()

	tmpFile, err := os.CreateTemp(css.scratchDirectory, "archive-*.zip")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmpFile.Name())

	if _, err := io.Copy(tmpFile, httpRsp.Body); err != nil {
		return nil, err
	}

	zipReader, err := zip.OpenReader(tmpFile.Name())
	if err != nil {
		return nil, err
	}
	defer zipReader.Close()

	iw, err := index.NewWriter(css.db, req.GetNamespace())
	if err != nil {
		return nil, err
	}

	for _, file := range zipReader.File {
		parts := strings.Split(file.Name, string(filepath.Separator))
		if len(parts) == 1 {
			continue
		}

		if file.FileInfo().IsDir() {
			continue
		}

		filename := filepath.Join(parts[1:]...)

		rc, err := file.Open()
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		buf, err := io.ReadAll(rc)
		if err != nil {
			return nil, err
		}

		err = github.AddFileToIndex(iw, repoURL, commitSHA, filename, buf)
		if err != nil {
			log.Infof("File %s can't be indexed, skipping: %v", filename, err)
			continue
		}
	}

	if err := github.SetLastIndexedCommitSha(iw, repoURL, commitSHA); err != nil {
		return nil, err
	}

	if err := iw.Flush(); err != nil {
		return nil, err
	}

	log.Infof("Finished indexing %s at commit %s", req.GetGitRepo().GetRepoUrl(), req.GetRepoState().GetCommitSha())

	return &inpb.IndexResponse{}, nil
}

func (css *codesearchServer) Index(ctx context.Context, req *inpb.IndexRequest) (*inpb.IndexResponse, error) {
	// Validate namespace against side-channel auth
	validatedNamespace, err := css.getUserNamespace(ctx, req.GetNamespace())
	if err != nil {
		return nil, err
	}

	req.Namespace = validatedNamespace

	var rsp *inpb.IndexResponse
	eg := &errgroup.Group{}
	eg.Go(func() error {
		// Only one update at a time is allowed per repo.
		// If multiple threads update the same repo at the same time, they risk
		// adding multiple different versions of the same file.

		// Note that, while go Mutexes do guarantee non-starvation, they don't provide FIFO
		// ordering. So, if multiple repo re-indexes are requested concurrently, it is not
		// guaranteed that they will be processed in any particular order.

		lockKey := fmt.Sprintf("%s-%s", validatedNamespace, req.GetGitRepo().GetRepoUrl())
		unlockFn := css.repoLocks.Lock(lockKey)
		defer unlockFn()

		var err error
		switch req.GetReplacementStrategy() {
		case inpb.ReplacementStrategy_INCREMENTAL:
			rsp, err = css.incrementalUpdate(ctx, req)
		case inpb.ReplacementStrategy_REPLACE_REPO:
			rsp, err = css.fullyReindex(ctx, req)
		case inpb.ReplacementStrategy_DROP_NAMESPACE:
			rsp, err = css.dropNamespace(req)
		default:
			return status.InvalidArgumentErrorf("Invalid replacement strategy %s", req.GetReplacementStrategy())
		}

		if err != nil {
			log.Errorf("Failed indexing %q: %s", req.GetGitRepo().GetRepoUrl(), err)
			return err
		}

		return nil
	})
	if req.GetAsync() {
		return &inpb.IndexResponse{}, nil
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return rsp, nil
}

func (css *codesearchServer) dropNamespace(req *inpb.IndexRequest) (*inpb.IndexResponse, error) {
	log.Infof("Dropping namespace %s", req.GetNamespace())

	writer, err := index.NewWriter(css.db, req.GetNamespace())
	if err != nil {
		return nil, status.InternalErrorf("failed to create index writer for namespace %s: %v", req.GetNamespace(), err)
	}

	if err := writer.DropNamespace(); err != nil {
		return nil, status.InternalErrorf("failed to drop namespace %s: %v", req.GetNamespace(), err)
	}

	err = writer.Flush()
	if err != nil {
		return nil, status.InternalErrorf("failed to flush index writer for namespace %s: %v", req.GetNamespace(), err)
	}

	log.Infof("Dropped namespace %s", req.GetNamespace())
	return &inpb.IndexResponse{}, nil
}

func (css *codesearchServer) RepoStatus(ctx context.Context, req *inpb.RepoStatusRequest) (*inpb.RepoStatusResponse, error) {
	namespace, err := css.getUserNamespace(ctx, req.GetNamespace())
	if err != nil {
		return nil, err
	}

	repoURL, err := git.ParseGitHubRepoURL(req.GetRepoUrl())
	if err != nil {
		return nil, err
	}
	r := index.NewReader(ctx, css.db, namespace, schema.MetadataSchema())

	rev, err := github.GetLastIndexedCommitSha(r, repoURL)
	if err != nil {
		// If there's no status, return an empty commit SHA, but don't error.
		if !status.IsNotFoundError(err) {
			return nil, err
		}
	}

	return &inpb.RepoStatusResponse{
		LastIndexedCommitSha: rev,
	}, nil
}

func (css *codesearchServer) Search(ctx context.Context, req *srpb.SearchRequest) (*srpb.SearchResponse, error) {
	log.Debugf("search req: %+v", req)

	namespace, err := css.getUserNamespace(ctx, req.GetNamespace())
	if err != nil {
		return nil, err
	}

	ctx = performance.WrapContext(ctx)
	numResults := defaultNumResults
	if req.GetNumResults() > 0 && req.GetNumResults() < maxNumResults {
		numResults = int(req.GetNumResults())
	}
	codesearcher := searcher.New(ctx, index.NewReader(ctx, css.db, namespace, schema.GitHubFileSchema()))
	q, err := query.NewReQuery(ctx, req.GetQuery().GetTerm())
	if err != nil {
		return nil, err
	}
	docs, err := codesearcher.Search(q, numResults, int(req.GetOffset()))
	if err != nil {
		return nil, err
	}
	highlighter := q.Highlighter()

	rsp := &srpb.SearchResponse{
		ParsedQuery: &srpb.ParsedQuery{
			RawQuery:    req.GetQuery().GetTerm(),
			ParsedQuery: q.ParsedQuery(),
			Squery:      string(q.SQuery()),
		},
	}
	for _, doc := range docs {
		regions := highlighter.Highlight(doc)
		if len(regions) == 0 {
			log.Warningf("No highlight regions found for doc: %s, dropping", doc.Field(schema.FilenameField).Contents())
			continue
		}

		// Dedupe the regions (by matched line number) so that we don't
		// display the same line multiple times.
		dedupedRegions := make([]types.HighlightedRegion, 0, len(regions))

		lastLine := -1
		for _, region := range regions {
			if region.Line() == lastLine {
				continue
			}
			dedupedRegions = append(dedupedRegions, region)
			lastLine = region.Line()
		}

		result := &srpb.Result{
			Owner:      string(doc.Field(schema.OwnerField).Contents()),
			Repo:       string(doc.Field(schema.RepoField).Contents()),
			Filename:   string(doc.Field(schema.FilenameField).Contents()),
			MatchCount: int32(len(dedupedRegions)),
			Sha:        string(doc.Field(schema.SHAField).Contents()),
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
			result.Snippets = append(result.Snippets, &srpb.Snippet{
				Lines: region.CustomSnippet(precedingLines, trailingLines),
			})
		}
		if req.GetIncludeContent() {
			result.Content = doc.Field(schema.ContentField).Contents()
		}
		rsp.Results = append(rsp.Results, result)
	}
	if t := performance.TrackerFromContext(ctx); t != nil {
		keys := t.Keys()
		performanceMetrics := &srpb.PerformanceMetrics{
			Metrics: make([]*srpb.Metric, len(keys)),
		}
		for i, key := range keys {
			performanceMetrics.Metrics[i] = &srpb.Metric{
				Name:  key.String(),
				Value: t.Get(key),
			}
		}
		rsp.PerformanceMetrics = performanceMetrics
	}
	return rsp, nil
}

func (css *codesearchServer) extendedXrefs(ctx context.Context, req *srpb.ExtendedXrefsRequest) (*srpb.ExtendedXrefsReply, error) {
	// This function exists to populate the references panel in the code browser UI.
	// The overall approach is:
	// 1. Fetch "interesting" edges from the requested tickets
	// 2. Fetch xrefs for each target node in the fetched edges
	// 3. Bucket the target nodes into the appropriate fields in the response.

	ticks := req.GetTickets()

	// See https://kythe.io/docs/schema/ for edge type definitions.
	// Every edge that leads to data that might want to be shown in the references panel of the UI
	// should be included here.
	edgeTypes := []string{
		"/kythe/edge/overrides",
		"%/kythe/edge/overrides",
		"/kythe/edge/overrides/transitive",
		"%/kythe/edge/overrides/transitive",
		"/kythe/edge/satisfies",
		"%/kythe/edge/satisfies",
		"/kythe/edge/extends",
		"%/kythe/edge/extends",
		"/kythe/edge/generates",
		"%/kythe/edge/generates",
	}
	edgeReq := &kgpb.EdgesRequest{
		Ticket: ticks,
		Kind:   edgeTypes,
	}

	edgeReply, err := css.gs.Edges(ctx, edgeReq)
	if err != nil {
		return nil, status.InternalErrorf("failed to get edges for ticket %s: %v", ticks, err)
	}

	// Fetch xrefs for each ticket in the edge reply.
	xrefTickets := make([]string, len(ticks))
	copy(xrefTickets, ticks)

	for _, edgeSet := range edgeReply.GetEdgeSets() {
		for _, edges := range edgeSet.GetGroups() {
			for _, edge := range edges.GetEdge() {
				xrefTickets = append(xrefTickets, edge.GetTargetTicket())
			}
		}
	}

	xrefReq := &kxpb.CrossReferencesRequest{
		Ticket:          xrefTickets,
		DefinitionKind:  kxpb.CrossReferencesRequest_ALL_DEFINITIONS,
		DeclarationKind: kxpb.CrossReferencesRequest_ALL_DECLARATIONS,
		ReferenceKind:   kxpb.CrossReferencesRequest_ALL_REFERENCES,
		Snippets:        kxpb.SnippetsKind_DEFAULT,
	}
	xrefReply, err := css.xs.CrossReferences(ctx, xrefReq)
	if err != nil {
		return nil, status.InternalErrorf("failed to get cross-references for tickets %s: %v", xrefTickets, err)
	}

	// Combine the data from edges and xrefs into the final response.

	repl := &srpb.ExtendedXrefsReply{
		Overrides:    make([]*kxpb.CrossReferencesReply_RelatedAnchor, 0),
		OverriddenBy: make([]*kxpb.CrossReferencesReply_RelatedAnchor, 0),
		Extends:      make([]*kxpb.CrossReferencesReply_RelatedAnchor, 0),
		ExtendedBy:   make([]*kxpb.CrossReferencesReply_RelatedAnchor, 0),
		References:   make([]*kxpb.CrossReferencesReply_RelatedAnchor, 0),
	}

	handleEdges(
		[]string{"/kythe/edge/overrides", "/kythe/edge/overrides/transitive"},
		edgeReply,
		xrefReply,
		func(xrefs *kxpb.CrossReferencesReply_CrossReferenceSet) {
			repl.Overrides = append(repl.Overrides, xrefs.GetDefinition()...)
			// Add callers of the overridden function to the call hierarchy.
			repl.References = append(repl.References, xrefs.GetReference()...)
			repl.References = append(repl.References, xrefs.GetCaller()...)
		})

	handleEdges(
		[]string{"%/kythe/edge/overrides", "%/kythe/edge/overrides/transitive"},
		edgeReply,
		xrefReply,
		func(xrefs *kxpb.CrossReferencesReply_CrossReferenceSet) {
			repl.OverriddenBy = append(repl.Overrides, xrefs.GetDefinition()...)
			// Add callers of any overridding implementations to the call hierarchy.
			repl.References = append(repl.References, xrefs.GetReference()...)
			repl.References = append(repl.References, xrefs.GetCaller()...)
		})

	handleEdges(
		[]string{"/kythe/edge/satisfies", "/kythe/edge/extends"},
		edgeReply,
		xrefReply,
		func(xrefs *kxpb.CrossReferencesReply_CrossReferenceSet) {
			repl.Extends = append(repl.Extends, xrefs.GetDefinition()...)
			// Include references to the superclass/interface
			repl.References = append(repl.References, xrefs.GetReference()...)
		})

	handleEdges(
		[]string{"%/kythe/edge/satisfies", "%/kythe/edge/extends"},
		edgeReply,
		xrefReply,
		func(xrefs *kxpb.CrossReferencesReply_CrossReferenceSet) {
			repl.ExtendedBy = append(repl.Extends, xrefs.GetDefinition()...)
			// Include references to the subclasses/implementations?
			repl.References = append(repl.References, xrefs.GetReference()...)
		})

	handleEdges(
		[]string{"%/kythe/edge/generates"},
		edgeReply,
		xrefReply,
		func(xrefs *kxpb.CrossReferencesReply_CrossReferenceSet) {
			// This is be the location in the .proto file
			repl.GeneratedBy = append(repl.Extends, xrefs.GetDefinition()...)
		})

	handleEdges(
		[]string{"/kythe/edge/generates"},
		edgeReply,
		xrefReply,
		func(xrefs *kxpb.CrossReferencesReply_CrossReferenceSet) {
			// TODO(jdelfino): For protos, this will include references from within generated code,
			// which will not be available to browse. Something should filter these out...
			// but there's not a clear indication in the kythe annotations that a file itself is
			// generated.
			repl.References = append(repl.Extends, xrefs.GetReference()...)
		})

	for _, tick := range ticks {
		// Include xrefs for the original tickets as well.
		repl.Definitions = append(repl.Definitions, xrefReply.GetCrossReferences()[tick].GetDefinition()...)
		repl.References = append(repl.References, xrefReply.GetCrossReferences()[tick].GetReference()...)
		repl.References = append(repl.References, xrefReply.GetCrossReferences()[tick].GetCaller()...)
	}

	return repl, nil
}

// handleEdges is a helper function that pulls sets of edges from an EdgesReply, looks up the xrefs
// for each target node of the matched edges, and calls a handler function on each one.
func handleEdges(edgeTypes []string, edgeReply *kgpb.EdgesReply, xrefs *kxpb.CrossReferencesReply, handler func(anchor *kxpb.CrossReferencesReply_CrossReferenceSet)) {
	for _, edgeSet := range edgeReply.GetEdgeSets() {
		for _, edgeType := range edgeTypes {
			if edges, ok := edgeSet.GetGroups()[edgeType]; ok {
				for _, edge := range edges.GetEdge() {
					xrefs, ok := xrefs.GetCrossReferences()[edge.GetTargetTicket()]
					if !ok {
						continue
					}
					handler(xrefs)
				}
			}
		}
	}
}

func (css *codesearchServer) documentation(ctx context.Context, req *srpb.ExtendedDocumentationRequest) (*srpb.ExtendedDocumentationReply, error) {
	// 1. Find the %/kythe/edge/documents edges for the given ticket.
	// 2. Fetch the nodes for those tickets, and extract the /kythe/text fact from each node, which
	//    contains the docstring.
	// 3. Fetch the node for the original ticket, which contains the node type.
	// 4. Fetch the cross-references for the original ticket, which contains the definition location.

	// Note that, even though the kythe xrefs service has a "Documentation" method, the underlying
	// data does not appear to be populated by kythe's write_tables tool (unless you use the
	// experimental beam implementation), and even then, it doesn't contain node info or definition
	// locations, which we want to show in the UI. So, we do that part manually here.

	ticket := req.GetTicket()
	tickets := []string{ticket}
	reply := &srpb.ExtendedDocumentationReply{}

	// Edges, to find all the "documents" edges for the given ticket.
	edgeReq := &kgpb.EdgesRequest{
		Ticket: tickets,
		Kind:   []string{"%/kythe/edge/documents"},
	}

	edgeReply, err := css.gs.Edges(ctx, edgeReq)
	if err != nil {
		return nil, status.InternalErrorf("failed to get edges for ticket %s: %v", tickets, err)
	}

	for _, edgeSet := range edgeReply.GetEdgeSets() {
		if grp, ok := edgeSet.GetGroups()["%/kythe/edge/documents"]; ok {
			for _, edge := range grp.GetEdge() {
				tickets = append(tickets, edge.TargetTicket)
			}
		}
	}

	// Nodes, to get docstring, and the node kind of the original ticket.
	nodeReply, err := css.gs.Nodes(ctx, &kgpb.NodesRequest{Ticket: tickets})
	if err != nil {
		return nil, status.InternalErrorf("failed to get nodes for tickets %s: %v", tickets, err)
	}

	for tick, node := range nodeReply.GetNodes() {
		if tick == ticket {
			reply.NodeInfo = node
		} else {
			// Documentation nodes hold the docstring in the "kythe/text" fact.
			// https://kythe.io/docs/schema/#doc
			txt, ok := node.GetFacts()["/kythe/text"]
			if ok {
				reply.Docstring = string(txt)
			}
		}
	}

	// Xrefs, for the definition location.
	xrefReq := &kxpb.CrossReferencesRequest{
		Ticket:          []string{ticket},
		DefinitionKind:  kxpb.CrossReferencesRequest_ALL_DEFINITIONS,
		DeclarationKind: kxpb.CrossReferencesRequest_NO_DECLARATIONS,
		ReferenceKind:   kxpb.CrossReferencesRequest_NO_REFERENCES,
		Snippets:        kxpb.SnippetsKind_DEFAULT,
	}
	xrefReply, err := css.xs.CrossReferences(ctx, xrefReq)
	if err != nil {
		return nil, status.InternalErrorf("failed to get cross-references for tickets %s: %v", ticket, err)
	}
	xrefs, ok := xrefReply.GetCrossReferences()[ticket]
	if ok && len(xrefs.GetDefinition()) > 0 {
		reply.Definition = xrefs.GetDefinition()[0]
	}

	return reply, nil
}

func (css *codesearchServer) KytheProxy(ctx context.Context, req *srpb.KytheRequest) (*srpb.KytheResponse, error) {
	var rsp = new(srpb.KytheResponse)
	var err = status.UnimplementedError("method not implemented in codesearch backend")

	switch req.Value.(type) {
	case *srpb.KytheRequest_CorpusRootsRequest:
		corpusRootsReply, corpusRootsErr := css.ft.CorpusRoots(ctx, req.GetCorpusRootsRequest())
		rsp.Value = &srpb.KytheResponse_CorpusRootsReply{
			CorpusRootsReply: corpusRootsReply,
		}
		err = corpusRootsErr
	case *srpb.KytheRequest_DirectoryRequest:
		directoryReply, directoryErr := css.ft.Directory(ctx, req.GetDirectoryRequest())
		rsp.Value = &srpb.KytheResponse_DirectoryReply{
			DirectoryReply: directoryReply,
		}
		err = directoryErr
	case *srpb.KytheRequest_NodesRequest:
		nodesReply, nodesErr := css.gs.Nodes(ctx, req.GetNodesRequest())
		rsp.Value = &srpb.KytheResponse_NodesReply{
			NodesReply: nodesReply,
		}
		err = nodesErr
	case *srpb.KytheRequest_DecorationsRequest:
		decorationsReply, decorationsErr := css.xs.Decorations(ctx, req.GetDecorationsRequest())
		rsp.Value = &srpb.KytheResponse_DecorationsReply{
			DecorationsReply: decorationsReply,
		}
		err = decorationsErr
	case *srpb.KytheRequest_CrossReferencesRequest:
		crossReferencesReply, crossReferencesErr := css.xs.CrossReferences(ctx, req.GetCrossReferencesRequest())
		rsp.Value = &srpb.KytheResponse_CrossReferencesReply{
			CrossReferencesReply: crossReferencesReply,
		}
		err = crossReferencesErr
	case *srpb.KytheRequest_ExtendedXrefsRequest:
		xrefsReply, xrefsErr := css.extendedXrefs(ctx, req.GetExtendedXrefsRequest())
		rsp.Value = &srpb.KytheResponse_ExtendedXrefsReply{
			ExtendedXrefsReply: xrefsReply,
		}
		err = xrefsErr
	case *srpb.KytheRequest_DocsRequest:
		docsReply, docsErr := css.documentation(ctx, req.GetDocsRequest())
		rsp.Value = &srpb.KytheResponse_DocsReply{
			DocsReply: docsReply,
		}
		err = docsErr
	}

	return rsp, err
}

func retrieveValue(lazyValue pebble.LazyValue) ([]byte, error) {
	val, owned, err := lazyValue.Value(nil)
	if err != nil {
		return nil, err
	}
	if owned || val == nil {
		return val, nil
	}
	copiedVal := make([]byte, len(val))
	copy(copiedVal, val)
	return copiedVal, nil
}

func (css *codesearchServer) syncIngestAnnotations(ctx context.Context, req *inpb.IngestAnnotationsRequest) (*inpb.IngestAnnotationsResponse, error) {
	if req.GetAsync() {
		xCtx, cancel := background.ExtendContextForFinalization(ctx, time.Minute)
		defer cancel()
		ctx = xCtx
	}

	tmpFile, err := os.CreateTemp(css.scratchDirectory, "kythe-*.sstable")
	if err != nil {
		return nil, err
	}
	fileName := tmpFile.Name()
	defer func() {
		tmpFile.Close()
		os.Remove(fileName)
	}()

	sstableName, err := digest.CASResourceNameFromProto(req.GetSstableName())
	if err != nil {
		return nil, err
	}
	if err := cachetools.GetBlob(ctx, css.env.GetByteStreamClient(), sstableName, tmpFile); err != nil {
		return nil, err
	}

	tmpFile.Seek(0, 0)
	readHandler, err := sstable.NewSimpleReadable(tmpFile)
	if err != nil {
		return nil, err
	}
	reader, err := sstable.NewReader(readHandler, sstable.ReaderOptions{})
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	iter, err := reader.NewIter(nil, nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	writer, err := css.kdb.Writer(ctx)
	if err != nil {
		return nil, err
	}

	bufSize := 0
	for iKey, iVal := iter.First(); iKey != nil; iKey, iVal = iter.Next() {
		key := iKey.UserKey
		val, err := retrieveValue(iVal)
		if err != nil {
			return nil, err
		}
		writer.Write(key, val)
		bufSize += len(key) + len(val)
		if bufSize >= 100*1e6 {
			if err := writer.Close(); err != nil {
				return nil, err
			}
			writer, err = css.kdb.Writer(ctx)
			if err != nil {
				return nil, err
			}
			bufSize = 0
		}
	}

	if bufSize > 0 {
		if err := writer.Close(); err != nil {
			return nil, err
		}
	}
	return &inpb.IngestAnnotationsResponse{}, nil
}

func (css *codesearchServer) IngestAnnotations(ctx context.Context, req *inpb.IngestAnnotationsRequest) (*inpb.IngestAnnotationsResponse, error) {
	var rsp *inpb.IngestAnnotationsResponse
	eg := &errgroup.Group{}
	eg.Go(func() error {
		r, err := css.syncIngestAnnotations(ctx, req)
		if err != nil {
			log.Errorf("Failed ingesting kythe table %+v: %s", req.GetSstableName(), err)
			return err
		}
		rsp = r
		log.Infof("Finished ingesting kythe table %+v", req.GetSstableName())
		return nil
	})
	if req.GetAsync() {
		return &inpb.IngestAnnotationsResponse{}, nil
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return rsp, nil
}

func (css *codesearchServer) Close(ctx context.Context) {
	css.kdb.Close(ctx)
}
