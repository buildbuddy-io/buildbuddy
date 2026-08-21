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

	"github.com/buildbuddy-io/buildbuddy/codesearch/annotations"
	"github.com/buildbuddy-io/buildbuddy/codesearch/github"
	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/codesearch/nav"
	"github.com/buildbuddy-io/buildbuddy/codesearch/performance"
	"github.com/buildbuddy-io/buildbuddy/codesearch/query"
	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/codesearch/searcher"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/lockmap"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/cockroachdb/pebble"
	"golang.org/x/sync/errgroup"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/index"
	srpb "github.com/buildbuddy-io/buildbuddy/proto/search"
)

const (
	// Used to control how many results may be returned at a time.
	defaultNumResults = 10
	maxNumResults     = 1000
)

var isAlphaNumPath = regexp.MustCompile(`^[A-Za-z/0-9]*$`).MatchString

// The kythe Decorations/CrossReferences endpoints carry no namespace (kythe
// data was group-scoped); nav data is namespace-scoped, so for now we read from
// a configured namespace, defaulting to the group's default namespace. Carrying
// the namespace on the request is a follow-up.
var treeSitterNavNamespace = flag.String("codesearch.treesitter_nav_namespace", "",
	"Namespace (within the authenticated group) to serve tree-sitter navigation from.")

func New(env environment.Env, rootDirectory, scratchDirectory string) (*codesearchServer, error) {
	if err := disk.EnsureDirectoryExists(scratchDirectory); err != nil {
		return nil, err
	}
	db, err := index.OpenPebbleDB(rootDirectory)
	if err != nil {
		return nil, err
	}

	return &codesearchServer{
		env:              env,
		db:               db,
		scratchDirectory: scratchDirectory,
		repoLocks:        lockmap.New[string](),
	}, nil
}

type codesearchServer struct {
	env              environment.Env
	db               *pebble.DB
	scratchDirectory string

	repoLocks lockmap.Locker[string]
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

	// The module path was stored at full-reindex time; refresh it if a commit
	// being processed modifies the root go.mod. Commit filenames are
	// repo-relative, so the RepoContext has no root dir.
	modulePath, err := github.GetRepoModulePath(r, repoURL)
	if err != nil {
		return nil, err
	}
	for _, commit := range commits {
		for _, add := range commit.GetAddsAndUpdates() {
			// Only overwrite on a successful parse: an unparsable or removed
			// go.mod must not wipe the previously-resolved module path.
			if add.GetFilepath() == "go.mod" {
				if mp := annotations.GoModulePath(add.GetContent()); mp != "" {
					modulePath = mp
				}
			}
		}
	}
	rctx := annotations.NewRepoContext("", modulePath)

	iw, err := index.NewWriter(css.db, req.GetNamespace())
	if err != nil {
		return nil, err
	}

	for _, commit := range commits {
		if err := github.ProcessCommit(iw, rctx, repoURL, commit); err != nil {
			return nil, status.InternalErrorf("failed to process commit %s: %v", commit.GetSha(), err)
		}
	}

	err = github.SetRepoMetadata(iw, repoURL, commits[len(commits)-1].GetSha(), modulePath)
	if err != nil {
		return nil, fmt.Errorf("failed to finalize update: %w", err)
	}

	if err := iw.Flush(); err != nil {
		return nil, err
	}

	log.Infof("finished incremental update on %s from %s to %s", repoURL, commits[0].GetSha(), commits[len(commits)-1].GetSha())

	return &inpb.IndexResponse{}, nil
}

// moduleFromArchive returns the Go module path declared in the archive's root
// go.mod, or "" if there is none. Archive entries are nested under a single
// top-level directory, which is stripped to match the indexing loop.
func moduleFromArchive(files []*zip.File) (string, error) {
	for _, file := range files {
		parts := strings.Split(file.Name, string(filepath.Separator))
		if len(parts) > 1 && filepath.Join(parts[1:]...) == "go.mod" {
			rc, err := file.Open()
			if err != nil {
				return "", err
			}
			buf, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				return "", err
			}
			return annotations.GoModulePath(buf), nil
		}
	}
	return "", nil
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

	// Read the module path from the archive's root go.mod up front, so every
	// file is indexed with the repo context that resolves Go import
	// identities. Archive filenames are repo-relative, so rctx has no root dir.
	modulePath, err := moduleFromArchive(zipReader.File)
	if err != nil {
		return nil, err
	}
	rctx := annotations.NewRepoContext("", modulePath)

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

		err = github.AddFileToIndex(iw, rctx, repoURL, commitSHA, filename, buf)
		if err != nil {
			log.Infof("File %s can't be indexed, skipping: %v", filename, err)
			continue
		}
	}

	if err := github.SetRepoMetadata(iw, repoURL, commitSHA, modulePath); err != nil {
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

// navReader opens a GitHubFileSchema reader on the nav namespace within the
// authenticated group. The kythe endpoints carry no namespace (kythe data was
// group-scoped); nav data is namespace-scoped, so for now we read from a
// configured namespace, defaulting to the group's default namespace — the same
// one search reads when no namespace is requested. Carrying the namespace on
// the request is a follow-up.
func (css *codesearchServer) navReader(ctx context.Context) (*index.Reader, error) {
	ns, err := css.getUserNamespace(ctx, *treeSitterNavNamespace)
	if err != nil {
		return nil, err
	}
	return index.NewReader(ctx, css.db, ns, schema.GitHubFileSchema()), nil
}

// KytheProxy answers the code browser's navigation requests. The endpoints and
// reply protos are kythe's (the frontend speaks them), but the data is served
// from tree-sitter over the codesearch index — kythe itself is gone. Request
// types the browser doesn't use are unimplemented.
func (css *codesearchServer) KytheProxy(ctx context.Context, req *srpb.KytheRequest) (*srpb.KytheResponse, error) {
	r, err := css.navReader(ctx)
	if err != nil {
		return nil, err
	}
	rsp := new(srpb.KytheResponse)
	switch req.Value.(type) {
	case *srpb.KytheRequest_DecorationsRequest:
		reply, err := nav.Decorations(ctx, r, req.GetDecorationsRequest())
		rsp.Value = &srpb.KytheResponse_DecorationsReply{DecorationsReply: reply}
		return rsp, err
	case *srpb.KytheRequest_CrossReferencesRequest:
		reply, err := nav.CrossReferences(ctx, r, req.GetCrossReferencesRequest())
		rsp.Value = &srpb.KytheResponse_CrossReferencesReply{CrossReferencesReply: reply}
		return rsp, err
	case *srpb.KytheRequest_ExtendedXrefsRequest:
		reply, err := nav.ExtendedXrefs(ctx, r, req.GetExtendedXrefsRequest())
		rsp.Value = &srpb.KytheResponse_ExtendedXrefsReply{ExtendedXrefsReply: reply}
		return rsp, err
	case *srpb.KytheRequest_DocsRequest:
		reply, err := nav.Documentation(ctx, r, req.GetDocsRequest())
		rsp.Value = &srpb.KytheResponse_DocsReply{DocsReply: reply}
		return rsp, err
	}
	return rsp, status.UnimplementedError("unsupported navigation request type")
}

func (css *codesearchServer) Close(ctx context.Context) {
	css.db.Close()
}
