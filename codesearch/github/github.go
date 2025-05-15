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

func AddFileToIndex(w types.IndexWriter, repoURL *git.RepoURL, commitSHA, filename string, fileContent []byte) error {
	process, err := validateFile(fileContent)
	if err != nil {
		return fmt.Errorf("File can't be indexed %s: %w", filename, err)
	}
	if !process {
		log.Infof("Skipping add of unsupported file %s", filename)
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
		return fmt.Errorf("Failed to update file %s: %w", filename, err)
	}
	return nil
}

func ProcessCommit(w types.IndexWriter, repoURL *git.RepoURL, commit *inpb.Commit) error {
	idFieldSchema := schema.GitHubFileSchema().Field(schema.IDField)

	for _, deletePath := range commit.GetDeleteFilepaths() {
		idField := idFieldSchema.MakeField(makeFileId(repoURL, deletePath))
		err := w.DeleteDocumentByMatchField(idField)
		if err != nil {
			return fmt.Errorf("Failed to delete document %s in commit %s: %w", deletePath, commit.GetSha(), err)
		}
	}

	for _, add := range commit.GetAddsAndUpdates() {
		err := AddFileToIndex(w, repoURL, commit.GetSha(), add.GetFilepath(), add.GetContent())
		if err != nil {
			return fmt.Errorf("Failed to add document %s in commit %s: %w", add.GetFilepath(), commit.GetSha(), err)
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
	doc := r.GetStoredDocument(docMatch.Docid())
	return string(doc.Field(schema.LatestSHAField).Contents()), nil
}

func detectionBuffer(content []byte) []byte {
	if len(content) > detectionBufferSize {
		return content[:detectionBufferSize]
	}
	return content
}

func validateFile(content []byte) (bool, error) {
	if len(content) > maxFileLen {
		return false, fmt.Errorf("File too long")
	}

	shortBuf := detectionBuffer(content)

	// Check the mimetype and skip if not valid for indexing.
	mtype, err := mimetype.DetectReader(bytes.NewReader(shortBuf))
	if err != nil {
		return false, fmt.Errorf("Failed to detect mimetype: %w", err)
	}
	if skipMime.MatchString(mtype.String()) {
		return false, nil
	}

	if !utf8.Valid(content) {
		return false, fmt.Errorf("Non-UTF8 content in file")
	}
	return true, nil
}
