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
	"strings"

	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/codesearch/performance"
	"github.com/buildbuddy-io/buildbuddy/codesearch/query"
	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/codesearch/searcher"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/cockroachdb/pebble"
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
	kythe_pebble "kythe.io/kythe/go/storage/pebble"
)

const (
	maxFileLen = 10_000_000

	// The maximum amount of bytes from a file to use for language and
	// mimetype detection.
	detectionBufferSize = 1000

	// The following field names are used in the indexed docs.
	filenameField = "filename"
	contentField  = "content"
	languageField = "language"
	ownerField    = "owner"
	repoField     = "repo"
	shaField      = "sha"

	// Used to control how many results may be returned at a time.
	defaultNumResults = 10
	maxNumResults     = 1000
)

func init() {
	flagyaml.IgnoreFlagForYAML("experimental_cross_reference_indirection_kinds")
}

func New(env environment.Env, rootDirectory, scratchDirectory string) (*codesearchServer, error) {
	ctx := context.Background()

	if err := disk.EnsureDirectoryExists(scratchDirectory); err != nil {
		return nil, err
	}
	db, err := pebble.Open(rootDirectory, &pebble.Options{})
	if err != nil {
		return nil, err
	}

	kdb := kythe_pebble.OpenRaw(db)
	tbl := &table.KVProto{DB: kdb}
	gs := gsrv.NewCombinedTable(tbl)
	ft := &ftsrv.Table{Proto: tbl, PrefixedKeys: true}
	it := &identifiers.Table{Proto: tbl}
	xs := xsrv.NewService(ctx, kdb)

	return &codesearchServer{
		env:              env,
		db:               db,
		scratchDirectory: scratchDirectory,

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

func (css *codesearchServer) syncIndex(ctx context.Context, req *inpb.IndexRequest) (*inpb.IndexResponse, error) {
	repoURLString := req.GetGitRepo().GetRepoUrl()
	commitSHA := req.GetRepoState().GetCommitSha()
	username := req.GetGitRepo().GetUsername()
	accessToken := req.GetGitRepo().GetAccessToken()

	archiveURL, err := apiArchiveURL(repoURLString, commitSHA, username, accessToken)
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
	log.Debugf("Copied archive to %q", tmpFile.Name())

	zipReader, err := zip.OpenReader(tmpFile.Name())
	if err != nil {
		return nil, err
	}
	defer zipReader.Close()

	iw, err := index.NewWriter(css.db, req.GetNamespace())
	if err != nil {
		return nil, err
	}

	repoURL, err := git.ParseGitHubRepoURL(repoURLString)
	if err != nil {
		return nil, err
	}

	for _, file := range zipReader.File {
		parts := strings.Split(file.Name, string(filepath.Separator))
		if len(parts) == 1 {
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
		doc, err := schema.MakeDocument(filename, commitSHA, repoURL, buf)
		if err != nil {
			log.Debug(err.Error())
			continue
		}
		if err := iw.UpdateDocument(doc.Field(schema.IDField), doc); err != nil {
			return nil, err
		}
	}

	if err := iw.Flush(); err != nil {
		return nil, err
	}

	return &inpb.IndexResponse{}, nil
}

func (css *codesearchServer) Index(ctx context.Context, req *inpb.IndexRequest) (*inpb.IndexResponse, error) {
	if req.GetNamespace() == "" {
		return nil, fmt.Errorf("a non-empty namespace must be specified")
	}

	var rsp *inpb.IndexResponse
	eg := &errgroup.Group{}
	eg.Go(func() error {
		r, err := css.syncIndex(ctx, req)
		if err != nil {
			log.Errorf("Failed indexing %q: %s", req.GetGitRepo().GetRepoUrl(), err)
			return err
		}
		rsp = r
		log.Printf("Finished indexing %s", req.GetGitRepo().GetRepoUrl())
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

func (css *codesearchServer) Search(ctx context.Context, req *srpb.SearchRequest) (*srpb.SearchResponse, error) {
	if req.GetNamespace() == "" {
		return nil, fmt.Errorf("a non-empty namespace must be specified")
	}
	log.Printf("search req: %+v", req)
	ctx = performance.WrapContext(ctx)
	numResults := defaultNumResults
	if req.GetNumResults() > 0 && req.GetNumResults() < maxNumResults {
		numResults = int(req.GetNumResults())
	}
	codesearcher := searcher.New(ctx, index.NewReader(ctx, css.db, req.GetNamespace()))
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
			Owner:      string(doc.Field(ownerField).Contents()),
			Repo:       string(doc.Field(repoField).Contents()),
			Filename:   string(doc.Field(filenameField).Contents()),
			MatchCount: int32(len(dedupedRegions)),
			Sha:        string(doc.Field(shaField).Contents()),
		}
		for _, region := range dedupedRegions {
			result.Snippets = append(result.Snippets, &srpb.Snippet{
				Lines: region.CustomSnippet(1, 1),
			})
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

func (css *codesearchServer) XrefsService() xrefs.Service {
	return css.xs
}
func (css *codesearchServer) GraphService() graph.Service {
	return css.gs
}
func (css *codesearchServer) IdentifierService() identifiers.Service {
	return css.it
}
func (css *codesearchServer) FiletreeService() filetree.Service {
	return css.ft
}

func (css *codesearchServer) IngestKytheTable(ctx context.Context, req *inpb.KytheIndexRequest) (*inpb.KytheIndexResponse, error) {
	tmpFile, err := os.CreateTemp(css.scratchDirectory, "kythe-*.sstable")
	if err != nil {
		return nil, err
	}
	fileName := tmpFile.Name()
	defer func() {
		// Only clean up the file if it still exists. If Ingest()
		// succeeds (below) then it should not.
		if _, err := os.Stat(fileName); err == nil {
			log.Warningf("ingestion failed (req: %+v); cleaning up tmpfile %q", req, fileName)
			os.Remove(fileName)
		}
	}()

	sstableName := digest.ResourceNameFromProto(req.GetSstableName())
	if err := cachetools.GetBlob(ctx, css.env.GetByteStreamClient(), sstableName, tmpFile); err != nil {
		return nil, err
	}
	if err := tmpFile.Close(); err != nil {
		return nil, err
	}
	if err := css.db.Ingest([]string{fileName}); err != nil {
		return nil, err
	}
	return &inpb.KytheIndexResponse{}, nil
}

func (css *codesearchServer) Close(ctx context.Context) {
	css.kdb.Close(ctx)
}
