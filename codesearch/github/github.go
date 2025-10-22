package github

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
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

	// The maximum number of changes to process in a single incremental update.
	// 1000 was an arbitrary choice, and could be adjusted if needed.
	maxAllowedChanges = 1000
)

var (
	// TODO(tylerw): this should come from a flag?
	skipMime    = regexp.MustCompile(`^audio/.*|video/.*|image/.*|application/gzip$`)
	regexpSha   = regexp.MustCompile("^[0-9a-f]{5,40}$")
	filepathSha = regexp.MustCompile("^:[0-9]{6} [0-9]{6}")
)

func lastIndexedDocKey(repoURL *git.RepoURL) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s", repoURL.Host, repoURL.Owner, repoURL.Repo))
}

func makeFileId(repoURL *git.RepoURL, name string) []byte {
	uniqueID := xxhash.Sum64String(repoURL.Owner + repoURL.Repo + name)
	idBytes := fmt.Appendf(nil, "%d", uniqueID)
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
		return fmt.Errorf("file too long")
	}

	shortBuf := detectionBuffer(content)

	// Check the mimetype and skip if not valid for indexing.
	mtype, err := mimetype.DetectReader(bytes.NewReader(shortBuf))
	if err != nil {
		return fmt.Errorf("failed to detect mimetype: %w", err)
	}
	if skipMime.MatchString(mtype.String()) {
		return fmt.Errorf("mimetype not supported for indexing: %s", mtype)
	}

	if !utf8.Valid(content) {
		return fmt.Errorf("Non-UTF8 content in file")
	}
	return nil
}

type GitClient interface {
	ExecuteCommand(args ...string) (string, error)
	LoadFileContents(fileToLoad string) ([]byte, error)
}

type commandLineGitClient struct {
	repoDir string
}

func NewCommandLineGitClient(repoDir string) GitClient {
	return &commandLineGitClient{repoDir: repoDir}
}

func (c *commandLineGitClient) ExecuteCommand(args ...string) (string, error) {
	command := exec.Command("git", args...)
	command.Dir = c.repoDir
	output, err := command.CombinedOutput()
	if err != nil {
		return "", status.InternalErrorf("failed to run command with args: %v: %v", args, err)
	}
	return string(output), nil
}

func (c *commandLineGitClient) LoadFileContents(filename string) ([]byte, error) {
	filePath := filepath.Join(c.repoDir, filename)
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, status.InternalErrorf("failed to read file %s: %s", filePath, err)
	}
	return content, nil
}

func processDiffTreeLine(gc GitClient, line string, commit *inpb.Commit) error {
	// diff-tree lines are in the format "<src mode> <dst mode> <src sha> <dst sha> <status>\t<src path>\t<dst path(copy/rename only)>""
	// Documentation: https://git-scm.com/docs/git-diff-tree
	parts := strings.Split(line, " ")
	if len(parts) < 5 {
		return status.InternalErrorf("invalid diff-tree line: %s", line)
	}

	fileParts := strings.Split(parts[4], "\t")
	if len(fileParts) < 2 {
		return status.InternalErrorf("invalid diff-tree line: %s", line)
	}

	fileStatus := fileParts[0]
	if fileStatus == "U" {
		return nil // Unmerged file - ignore
	}

	srcFile := fileParts[1]
	if fileStatus == "D" || fileStatus[0] == 'R' {
		if !slices.Contains(commit.DeleteFilepaths, srcFile) {
			commit.DeleteFilepaths = append(commit.DeleteFilepaths, srcFile)
		}
	}

	if fileStatus == "D" {
		// Nothing else to do for deletes
		return nil
	}

	fileToLoad := srcFile
	if fileStatus[0] == 'R' || fileStatus[0] == 'C' {
		// renames and copies have a destination file name in index 6
		if len(fileParts) < 3 {
			return status.InternalErrorf("invalid diff-tree line, R/C with no dest file: %s", line)
		}
		fileToLoad = fileParts[2]
	}

	content, err := gc.LoadFileContents(fileToLoad)
	if err != nil {
		return status.InternalErrorf("failed to read file %s: %s", fileToLoad, err)
	}

	if err := validateFile(content); err != nil {
		log.Infof("File %s can't be indexed, skipping: %v", fileToLoad, err)
		return nil
	}

	commit.AddsAndUpdates = append(commit.AddsAndUpdates, &inpb.File{
		Filepath: fileToLoad,
		Content:  content,
	})
	return nil
}

// ComputeIncrementalUpdate generates an incremental update payload for the codesearch indexer.
// If no updates are found, it returns nil with no error.
//
// The information is extracted using the git command line client on a local clone of a repo.
// The payload contains a list of commits, the file contents for each added/modified file, and a list
// of deleted filenames.
func ComputeIncrementalUpdate(gc GitClient, firstSha, lastSha string) (*inpb.IncrementalUpdate, error) {
	commitRange := fmt.Sprintf("%s..%s", firstSha, lastSha)

	changesStr, err := gc.ExecuteCommand("log", "--raw", "--first-parent", "--format=%H", "--reverse", commitRange)
	if err != nil {
		return nil, err
	}
	changes := strings.Split(strings.TrimSpace(changesStr), "\n")

	if len(changes) > maxAllowedChanges {
		return nil, status.FailedPreconditionErrorf("too many changes in commit range %s..%s: %d", firstSha, lastSha, len(changes))
	}
	if len(changes) == 0 || (len(changes) == 1 && len(changes[0]) == 0) {
		return nil, nil
	}

	result := &inpb.IncrementalUpdate{
		Commits: make([]*inpb.Commit, 0),
	}
	var currentCommit *inpb.Commit
	sha := firstSha

	for _, line := range changes {
		line := strings.TrimSpace(line)

		if regexpSha.MatchString(line) {
			// Commit line
			currentCommit = &inpb.Commit{
				Sha:       line,
				ParentSha: sha,
			}
			result.Commits = append(result.Commits, currentCommit)
			sha = line
		} else if filepathSha.MatchString(line) {
			// Diff line
			processDiffTreeLine(gc, line, currentCommit)
		} // else: ignore other lines
	}
	return result, nil
}
