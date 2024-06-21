package server

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/codesearch/query"
	"github.com/buildbuddy-io/buildbuddy/codesearch/searcher"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/cockroachdb/pebble"
	"github.com/gabriel-vasile/mimetype"
	"github.com/go-enry/go-enry/v2"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/index"
	srpb "github.com/buildbuddy-io/buildbuddy/proto/search"
	xxhash "github.com/cespare/xxhash/v2"
)

// TODO(tylerw): this should come from a flag?
var skipMime = regexp.MustCompile(`^audio/.*|video/.*|image/.*$`)

const (
	maxFileLen = 10_000_000

	// The maximum amount of bytes from a file to use for language and
	// mimetype detection.
	detectionBufferSize = 1000

	// The following field names are used in the indexed docs.
	filenameField = "filename"
	contentField  = "content"
	languageField = "language"
	repoField     = "repo"
	shaField      = "sha"

	// Used to control how many results may be returned at a time.
	defaultNumResults = 10
	maxNumResults     = 1000
)

func New(rootDirectory, scratchDirectory string) (*codesearchServer, error) {
	if err := disk.EnsureDirectoryExists(scratchDirectory); err != nil {
		return nil, err
	}
	db, err := pebble.Open(rootDirectory, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &codesearchServer{
		db:               db,
		scratchDirectory: scratchDirectory,
	}, nil
}

type codesearchServer struct {
	db               *pebble.DB
	scratchDirectory string
}

func makeDoc(name, repo, commitSha string, buf []byte) (types.Document, error) {
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
			filenameField: types.NewNamedField(types.TrigramField, filenameField, []byte(name), true /*=stored*/),
			contentField:  types.NewNamedField(types.TrigramField, contentField, buf, true /*=stored*/),
			languageField: types.NewNamedField(types.StringTokenField, languageField, []byte(lang), true /*=stored*/),
			repoField:     types.NewNamedField(types.StringTokenField, repoField, []byte(repo), true /*=stored*/),
			shaField:      types.NewNamedField(types.StringTokenField, shaField, []byte(commitSha), true /*=stored*/),
		},
	)
	return doc, nil
}

func (css *codesearchServer) Index(ctx context.Context, req *inpb.IndexRequest) (*inpb.IndexResponse, error) {
	gitRepo := req.GetGitRepo()
	if gitRepo == nil || gitRepo.GetOwnerRepo() == "" {
		return nil, fmt.Errorf("a git_repo must be specified")
	}
	if req.GetNamespace() == "" {
		return nil, fmt.Errorf("a non-empty namespace must be specified")
	}
	// https://github.com/buildbuddy-io/buildbuddy/archive/1d8a3184c996c3d167a281b70a4eeccd5188e5e1.tar.gz
	archiveURL := fmt.Sprintf("https://github.com/%s/archive/%s.zip", gitRepo.GetOwnerRepo(), gitRepo.GetCommitSha())
	log.Debugf("archive URL is %q", archiveURL)

	start := time.Now()
	log.Printf("Started indexing %s @ %s", gitRepo.GetOwnerRepo(), gitRepo.GetCommitSha())

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
		doc, err := makeDoc(filename, gitRepo.GetOwnerRepo(), gitRepo.GetCommitSha(), buf)
		if err != nil {
			log.Debugf(err.Error())
			continue
		}
		if err := iw.AddDocument(doc); err != nil {
			return nil, err
		}
	}

	if err := iw.Flush(); err != nil {
		return nil, err
	}

	log.Printf("Finished indexing %s @ %s [%s]", gitRepo.GetOwnerRepo(), gitRepo.GetCommitSha(), time.Since(start))
	return &inpb.IndexResponse{
		GitRepo: &inpb.GitRepoResponse{},
	}, nil
}

func (css *codesearchServer) Search(ctx context.Context, req *srpb.SearchRequest) (*srpb.SearchResponse, error) {
	if req.GetNamespace() == "" {
		return nil, fmt.Errorf("a non-empty namespace must be specified")
	}
	numResults := defaultNumResults
	if req.GetNumResults() > 0 && req.GetNumResults() < maxNumResults {
		numResults = int(req.GetNumResults())
	}
	codesearcher := searcher.New(index.NewReader(css.db, req.GetNamespace()))
	q, err := query.NewReQuery(req.GetQuery().GetTerm(), numResults)
	if err != nil {
		return nil, err
	}
	docs, err := codesearcher.Search(q)
	if err != nil {
		return nil, err
	}
	highlighter := q.GetHighlighter()

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
		result := &srpb.Result{
			Repo:       string(doc.Field(repoField).Contents()),
			Filename:   string(doc.Field(filenameField).Contents()),
			MatchCount: int32(len(regions)),
			Sha:        string(doc.Field(shaField).Contents()),
		}
		for _, region := range regions {
			result.Snippets = append(result.Snippets, &srpb.Snippet{
				Lines: region.CustomSnippet(1, 1),
			})
		}
		rsp.Results = append(rsp.Results, result)
	}

	return rsp, nil
}
