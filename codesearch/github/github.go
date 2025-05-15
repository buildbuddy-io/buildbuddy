// TODO(jdelfino): Move common github repo extraction code to this file from
// cli.go and server.go
package github

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"

	"github.com/gabriel-vasile/mimetype"
	"github.com/go-enry/go-enry/v2"

	xxhash "github.com/cespare/xxhash/v2"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/index"
)

const (
	maxFileLen = 10_000_000

	// The maximum amount of bytes from a file to use for language and
	// mimetype detection.
	detectionBufferSize = 1000
)

// TODO(tylerw): this should come from a flag?
var (
	skipMime = regexp.MustCompile(`^audio/.*|video/.*|image/.*|application/gzip$`)
)

func lastIndexedDocKey(repoURL *git.RepoURL) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s", repoURL.Host, repoURL.Owner, repoURL.Repo))
}

func makeFileId(repoURL *git.RepoURL, name string) []byte {
	uniqueID := xxhash.Sum64String(repoURL.Owner + repoURL.Repo + name)
	idBytes := []byte(fmt.Sprintf("%d", uniqueID))
	return idBytes
}

func makeLastIndexedDoc(repoURL *git.RepoURL, commitSHA string) types.Document {
	fields := map[string][]byte{
		schema.IDField:        lastIndexedDocKey(repoURL),
		schema.LatestSHAField: []byte(commitSHA),
	}
	doc, err := schema.MetadataSchema().MakeDocument(fields)
	if err != nil {
		log.Fatalf("Failed to make last indexed doc: %s", err)
	}
	return doc
}

func DownloadRepoArchive(archiveURL, scratchDirectory string) (string, func(), error) {
	httpRsp, err := http.Get(archiveURL)
	if err != nil {
		return "", nil, err
	}
	defer httpRsp.Body.Close()

	tmpFile, err := os.CreateTemp(scratchDirectory, "archive-*.zip")
	if err != nil {
		return "", nil, err
	}
	cleanupFn := func() {
		os.Remove(tmpFile.Name())
	}

	if _, err := io.Copy(tmpFile, httpRsp.Body); err != nil {
		cleanupFn()
		return "", nil, err
	}
	return tmpFile.Name(), cleanupFn, nil
}

func AddFileToIndex(w types.IndexWriter, repoURL *git.RepoURL, commitSHA, filepath string, fileContent []byte) error {
	fields, err := extractFields(filepath, commitSHA, repoURL, fileContent)
	if err != nil {
		return err
	}
	doc, err := schema.GitHubFileSchema().MakeDocument(fields)
	if err != nil {
		return err
	}
	if err := w.UpdateDocument(doc.Field(schema.IDField), doc); err != nil {
		return err
	}
	return nil
}

func ProcessCommit(w types.IndexWriter, repoURL *git.RepoURL, commit *inpb.Commit) error {
	idFieldSchema := schema.GitHubFileSchema().Field(schema.IDField)

	for _, delete := range commit.GetDeleteFilepaths() {
		idField := idFieldSchema.MakeField(makeFileId(repoURL, delete))
		err := w.DeleteDocumentByMatchField(idField)
		if err != nil {
			// TODO(jdelfino): Keep going? Abandon? Should these updates be atomic?
			return err
		}
	}

	for _, add := range commit.GetAdds() {
		fields, err := extractFields(add.GetFilepath(), commit.GetSha(), repoURL, add.GetContent())
		if err != nil {
			// TODO(jdelfino): Keep going? Abandon? Should these updates be atomic?
			return err
		}
		doc, err := schema.GitHubFileSchema().MakeDocument(fields)
		if err != nil {
			// TODO(jdelfino): Keep going? Abandon? Should these updates be atomic?
			return err
		}
		if err := w.UpdateDocument(doc.Field(schema.IDField), doc); err != nil {
			// TODO(jdelfino): Keep going? Abandon? Should these updates be atomic?
			return err
		}
	}
	return nil
}

func SetLastIndexedCommitSha(w types.IndexWriter, repoURL *git.RepoURL, commitSHA string) error {
	doc := makeLastIndexedDoc(repoURL, commitSHA)
	if err := w.UpdateDocument(doc.Field(schema.IDField), doc); err != nil {
		return fmt.Errorf("failed to set last indexed commit SHA: %w", err)
	}
	return nil
}

func GetLastIndexedCommitSha(r types.IndexReader, repoURL *git.RepoURL) (string, error) {
	idString := strconv.Quote(string(lastIndexedDocKey(repoURL)))
	results, err := r.RawQuery(fmt.Sprintf("(:eq %s %s)", schema.IDField, idString))
	if err != nil {
		return "", fmt.Errorf("failed to query last indexed commit SHA: %w", err)
	}

	if len(results) == 0 {
		return "", nil
	}
	if len(results) > 1 {
		return "", fmt.Errorf("multiple last indexed commit SHAs found for %s", repoURL)
	}

	docMatch := results[0]
	doc, err := r.GetStoredDocument(docMatch.Docid())
	if err != nil {
		return "", fmt.Errorf("failed to get doc for last indexed commit SHA: %w", err)
	}

	return string(doc.Field(schema.LatestSHAField).Contents()), nil
}

func extractFields(name, commitSha string, repoURL *git.RepoURL, fileContent []byte) (map[string][]byte, error) {
	// Skip long files.
	if len(fileContent) > maxFileLen {
		return nil, fmt.Errorf("skipping %s (file too long)", name)
	}

	var shortBuf []byte
	if len(fileContent) > detectionBufferSize {
		shortBuf = fileContent[:detectionBufferSize]
	} else {
		shortBuf = fileContent
	}

	// Check the mimetype and skip if bad.
	mtype, err := mimetype.DetectReader(bytes.NewReader(shortBuf))
	if err == nil && skipMime.MatchString(mtype.String()) {
		return nil, fmt.Errorf("skipping %s (invalid mime type: %q)", name, mtype.String())
	}

	// Skip non-utf8 encoded files.
	if !utf8.Valid(fileContent) {
		return nil, fmt.Errorf("skipping %s (non-utf8 content)", name)
	}

	// Compute filetype
	lang := strings.ToLower(enry.GetLanguage(filepath.Base(name), shortBuf))

	return (map[string][]byte{
		schema.IDField:       makeFileId(repoURL, name),
		schema.FilenameField: []byte(name),
		schema.ContentField:  fileContent,
		schema.LanguageField: []byte(lang),
		schema.OwnerField:    []byte(repoURL.Owner),
		schema.RepoField:     []byte(repoURL.Repo),
		schema.SHAField:      []byte(commitSha),
	}), nil
}
