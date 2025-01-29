package server

import (
	"archive/zip"
	"context"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

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
)

const (
	maxFileLen = 10_000_000

	// The maximum amount of bytes from a file to use for language and
	// mimetype detection.
	detectionBufferSize = 1000

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
	db, err := pebble.Open(rootDirectory, &pebble.Options{})
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

// getUserNamespace forces the namespace to match the authenticated user, but
// allows for clients to use a custom namespace within that subspace.
func (css *codesearchServer) getUserNamespace(ctx context.Context, requestedNamespace string) (string, error) {
	if !isAlphaNumPath(requestedNamespace) {
		return "", status.InvalidArgumentError("namespace must match a/b/c")
	}
	gid, err := prefix.UserPrefix(ctx, css.env)
	if err != nil {
		return "", err
	}
	namespace := filepath.Join(gid, requestedNamespace)
	return namespace, nil
}

func (css *codesearchServer) syncIndex(_ context.Context, req *inpb.IndexRequest) (*inpb.IndexResponse, error) {
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
	namespace, err := css.getUserNamespace(ctx, req.GetNamespace())
	if err != nil {
		return nil, err
	}

	// Rewrite the request namespace before passing it to syncIndex.
	req.Namespace = namespace

	var rsp *inpb.IndexResponse
	eg := &errgroup.Group{}
	eg.Go(func() error {
		r, err := css.syncIndex(ctx, req)
		if err != nil {
			log.Errorf("Failed indexing %q: %s", req.GetGitRepo().GetRepoUrl(), err)
			return err
		}
		rsp = r
		log.Infof("Finished indexing %s", req.GetGitRepo().GetRepoUrl())
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
	codesearcher := searcher.New(ctx, index.NewReader(ctx, css.db, namespace))
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

	sstableName := digest.ResourceNameFromProto(req.GetSstableName())
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
