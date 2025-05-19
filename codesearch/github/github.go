// TODO(jdelfino): Move common github repo extraction code to this file from
// cli.go and server.go
package github

import (
	"bytes"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

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
		// This indicates a coding error.
		log.Fatalf("Failed to make last indexed doc: %s", err)
	}
	return doc
}

// Adds the contents of a single file from a git repo to the index.
// Will reject the update (and return an error) if the file is too large, is of an unsupported
// mimetype, or contains invalid UTF-8 data.
// This function does not flush the index writer, so the caller is responsible for doing that.
func AddFileToIndex(w types.IndexWriter, repoURL *git.RepoURL, commitSHA, filename string, fileContent []byte) error {
	err := validateFile(fileContent)
	if err != nil {
		log.Infof("File %s can't be indexed, skipping: %v", filename, err)
		return err
	}

	lang := strings.ToLower(enry.GetLanguage(filepath.Base(filename), detectionBuffer(fileContent)))
	fields := map[string][]byte{
		schema.IDField:       makeFileId(repoURL, filename),
		schema.FilenameField: []byte(filename),
		schema.ContentField:  fileContent,
		schema.LanguageField: []byte(lang),
		schema.OwnerField:    []byte(repoURL.Owner),
		schema.RepoField:     []byte(repoURL.Repo),
		schema.SHAField:      []byte(commitSHA),
	}

	doc := schema.GitHubFileSchema().MustMakeDocument(fields)

	if err := w.UpdateDocument(doc.Field(schema.IDField), doc); err != nil {
		return status.InternalErrorf("Failed to update file %s: %v", filename, err)
	}
	return nil
}

// Process the adds and deletes in a single Commit object. If any errors are encountered in
// individual files, processing is halted and an error is returned.
// This function does not flush the index writer, so the caller is responsible for doing that.
func ProcessCommit(w types.IndexWriter, repoURL *git.RepoURL, commit *inpb.Commit) error {
	idFieldSchema := schema.GitHubFileSchema().Field(schema.IDField)

	for _, deletePath := range commit.GetDeleteFilepaths() {
		idField := idFieldSchema.MakeField(makeFileId(repoURL, deletePath))
		if err := w.DeleteDocumentByMatchField(idField); err != nil {
			return status.InternalErrorf("Failed to delete document %s in commit %s: %v", deletePath, commit.GetSha(), err)
		}
	}

	for _, add := range commit.GetAddsAndUpdates() {
		if err := AddFileToIndex(w, repoURL, commit.GetSha(), add.GetFilepath(), add.GetContent()); err != nil {
			return status.InternalErrorf("Failed to add document %s in commit %s: %v", add.GetFilepath(), commit.GetSha(), err)
		}
	}
	return nil
}

// Records the most recently indexed commit SHA in the index.
func SetLastIndexedCommitSha(w types.IndexWriter, repoURL *git.RepoURL, commitSHA string) error {
	doc := makeLastIndexedDoc(repoURL, commitSHA)
	if err := w.UpdateDocument(doc.Field(schema.IDField), doc); err != nil {
		return status.InternalErrorf("failed to set last indexed commit SHA: %v", err)
	}
	return nil
}

// Retrieves the most recently indexed commit SHA from the index.
// Returns status.NotFoundError if no commit has been recorded.
// Returns status.InternalError on any other error.
func GetLastIndexedCommitSha(r types.IndexReader, repoURL *git.RepoURL) (string, error) {
	idString := strconv.Quote(string(lastIndexedDocKey(repoURL)))
	results, err := r.RawQuery(fmt.Sprintf("(:eq %s %s)", schema.IDField, idString))
	if err != nil {
		return "", status.InternalErrorf("failed to query last indexed commit SHA: %v", err)
	}

	if len(results) == 0 {
		return "", status.NotFoundErrorf("no last indexed commit SHA found for %s", repoURL)
	}
	if len(results) > 1 {
		return "", status.InternalErrorf("multiple last indexed commit SHAs found for %s", repoURL)
	}

	docMatch := results[0]
	doc := r.GetStoredDocument(docMatch.Docid())
	sha := string(doc.Field(schema.LatestSHAField).Contents())
	if len(sha) == 0 {
		return "", status.NotFoundErrorf("no last indexed commit SHA found for %s", repoURL)
	}

	return sha, nil
}

func detectionBuffer(content []byte) []byte {
	if len(content) > detectionBufferSize {
		return content[:detectionBufferSize]
	}
	return content
}

func validateFile(content []byte) error {
	if len(content) > maxFileLen {
		return fmt.Errorf("File too long")
	}

	shortBuf := detectionBuffer(content)

	// Check the mimetype and skip if not valid for indexing.
	mtype, err := mimetype.DetectReader(bytes.NewReader(shortBuf))
	if err != nil {
		return fmt.Errorf("Failed to detect mimetype: %w", err)
	}
	if skipMime.MatchString(mtype.String()) {
		return fmt.Errorf("Mimetype not supported for indexing: %s", mtype)
	}

	if !utf8.Valid(content) {
		return fmt.Errorf("Non-UTF8 content in file")
	}
	return nil
}
