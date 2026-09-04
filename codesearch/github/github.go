package github

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/codesearch/annotations"
	"github.com/buildbuddy-io/buildbuddy/codesearch/gitclient"
	"github.com/buildbuddy-io/buildbuddy/codesearch/indexprofile"
	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"github.com/go-enry/go-enry/v2"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/index"
)

const (
	// The maximum amount of bytes from a file to use for language and
	// mimetype detection.
	detectionBufferSize = 1000
)

func lastIndexedDocKey(repoURL *git.RepoURL) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s", repoURL.Host, repoURL.Owner, repoURL.Repo))
}

func makeFileId(repoURL *git.RepoURL, name string) []byte {
	return schema.FileID(repoURL.Owner, repoURL.Repo, name)
}

func makeRepoMetadataDoc(repoURL *git.RepoURL, commitSHA, modulePath string) types.Document {
	fields := map[string][]byte{
		schema.IDField:             lastIndexedDocKey(repoURL),
		schema.LatestSHAField:      []byte(commitSHA),
		schema.RepoModulePathField: []byte(modulePath),
	}
	doc, err := schema.MetadataSchema().MakeDocument(fields)
	if err != nil {
		// This indicates a coding error.
		log.Fatalf("Failed to make repo metadata doc: %s", err)
	}
	return doc
}

// Adds the contents of a single file from a git repo to the index. Each
// file's tree-sitter annotations (imports, import_id, symbols) are extracted
// and indexed; rctx supplies the repo-level context that resolves Go import
// identities and may be nil when it is unavailable, in which case symbols are
// still extracted but Go import identities are not. Extraction errors are
// logged and skipped; they never fail indexing.
//
// Will reject the update (and return an error) if the file is too large, is of an unsupported
// mimetype, or contains invalid UTF-8 data.
// This function does not flush the index writer, so the caller is responsible for doing that.
func AddFileToIndex(w types.IndexWriter, rctx *annotations.RepoContext, repoURL *git.RepoURL, commitSHA, filename string, fileContent []byte) error {
	defer indexprofile.Timer(indexprofile.PhaseAddFileToIndex)()

	stopValidate := indexprofile.Timer(indexprofile.PhaseValidateFile)
	err := gitclient.ValidateFile(fileContent)
	stopValidate()
	if err != nil {
		indexprofile.Add(indexprofile.CounterFilesSkipped, 1)
		indexprofile.Add(indexprofile.CounterValidationSkippedFiles, 1)
		return err
	}

	stopLang := indexprofile.Timer(indexprofile.PhaseDetectLanguage)
	lang := strings.ToLower(enry.GetLanguage(filepath.Base(filename), detectionBuffer(fileContent)))
	stopLang()

	fields := map[string][]byte{
		schema.IDField:       makeFileId(repoURL, filename),
		schema.FilenameField: []byte(filename),
		schema.ContentField:  fileContent,
		schema.LanguageField: []byte(lang),
		schema.OwnerField:    []byte(repoURL.Owner),
		schema.RepoField:     []byte(repoURL.Repo),
		schema.SHAField:      []byte(commitSHA),
	}

	// TODO: thread the indexing context through AddFileToIndex so a slow
	// parse can be cancelled; the indexing path has none today.
	ann, annErr := annotations.Extract(context.Background(), lang, filename, fileContent, rctx)
	if annErr != nil {
		log.Warningf("annotation extraction failed for %q: %s", filename, annErr)
	} else if ann != nil {
		if len(ann.Imports) > 0 {
			fields[schema.ImportsField] = []byte(strings.Join(ann.Imports, " "))
		}
		if len(ann.ImportID) > 0 {
			fields[schema.ImportIDField] = []byte(strings.Join(ann.ImportID, " "))
		}
		if len(ann.Symbols) > 0 {
			fields[schema.SymbolsField] = []byte(strings.Join(ann.Symbols, " "))
		}
	}

	stopDoc := indexprofile.Timer(indexprofile.PhaseMakeDocument)
	doc := schema.GitHubFileSchema().MustMakeDocument(fields)
	stopDoc()

	stopUpdate := indexprofile.Timer(indexprofile.PhaseUpdateDocument)
	err = w.UpdateDocument(doc.Field(schema.IDField), doc)
	stopUpdate()
	if err != nil {
		indexprofile.Add(indexprofile.CounterFilesSkipped, 1)
		indexprofile.Add(indexprofile.CounterAddFileErrors, 1)
		return status.InternalErrorf("Failed to update file %s: %v", filename, err)
	}
	indexprofile.Add(indexprofile.CounterFilesIndexed, 1)
	return nil
}

// Process the adds and deletes in a single Commit object. If any errors are encountered in
// individual files, processing is halted and an error is returned.
// This function does not flush the index writer, so the caller is responsible for doing that.
func ProcessCommit(w types.IndexWriter, rctx *annotations.RepoContext, repoURL *git.RepoURL, commit *inpb.Commit) error {
	idFieldSchema := schema.GitHubFileSchema().Field(schema.IDField)

	for _, deletePath := range commit.GetDeleteFilepaths() {
		idField := idFieldSchema.MakeField(makeFileId(repoURL, deletePath))
		if err := w.DeleteDocumentByMatchField(idField); err != nil {
			return status.InternalErrorf("Failed to delete document %s in commit %s: %v", deletePath, commit.GetSha(), err)
		}
	}

	for _, add := range commit.GetAddsAndUpdates() {
		if err := AddFileToIndex(w, rctx, repoURL, commit.GetSha(), add.GetFilepath(), add.GetContent()); err != nil {
			return status.InternalErrorf("Failed to add document %s in commit %s: %v", add.GetFilepath(), commit.GetSha(), err)
		}
	}
	return nil
}

// SetRepoMetadata records the per-repo metadata doc: the most recently
// indexed commit SHA and the repo's Go module path (empty for non-Go repos).
// Both live in one doc, so they must be written together.
func SetRepoMetadata(w types.IndexWriter, repoURL *git.RepoURL, commitSHA, modulePath string) error {
	doc := makeRepoMetadataDoc(repoURL, commitSHA, modulePath)
	if err := w.UpdateDocument(doc.Field(schema.IDField), doc); err != nil {
		return status.InternalErrorf("failed to set repo metadata: %v", err)
	}
	return nil
}

// getRepoMetadataDoc returns the per-repo metadata doc, or status.NotFoundError
// if none has been recorded.
func getRepoMetadataDoc(r types.IndexReader, repoURL *git.RepoURL) (types.Document, error) {
	idString := strconv.Quote(string(lastIndexedDocKey(repoURL)))
	results, err := r.RawQuery(fmt.Sprintf("(:eq %s %s)", schema.IDField, idString))
	if err != nil {
		return nil, status.InternalErrorf("failed to query repo metadata: %v", err)
	}
	if len(results) == 0 {
		return nil, status.NotFoundErrorf("no repo metadata found for %s", repoURL)
	}
	if len(results) > 1 {
		return nil, status.InternalErrorf("multiple repo metadata docs found for %s", repoURL)
	}
	return r.GetStoredDocument(results[0].Docid()), nil
}

// Retrieves the most recently indexed commit SHA from the index.
// Returns status.NotFoundError if no commit has been recorded.
// Returns status.InternalError on any other error.
func GetLastIndexedCommitSha(r types.IndexReader, repoURL *git.RepoURL) (string, error) {
	doc, err := getRepoMetadataDoc(r, repoURL)
	if err != nil {
		return "", err
	}
	sha := string(doc.Field(schema.LatestSHAField).Contents())
	if len(sha) == 0 {
		return "", status.NotFoundErrorf("no last indexed commit SHA found for %s", repoURL)
	}
	return sha, nil
}

// GetRepoModulePath returns the repo's stored Go module path, or "" if no
// metadata has been recorded yet or the repo is not a Go module.
func GetRepoModulePath(r types.IndexReader, repoURL *git.RepoURL) (string, error) {
	doc, err := getRepoMetadataDoc(r, repoURL)
	if err != nil {
		if status.IsNotFoundError(err) {
			return "", nil
		}
		return "", err
	}
	return string(doc.Field(schema.RepoModulePathField).Contents()), nil
}

func detectionBuffer(content []byte) []byte {
	if len(content) > detectionBufferSize {
		return content[:detectionBufferSize]
	}
	return content
}
