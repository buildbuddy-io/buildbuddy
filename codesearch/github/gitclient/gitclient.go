// Package gitclient exposes the subset of the codesearch/github package
// needed by clients that assemble incremental update payloads from a
// local git checkout (most notably the `bb index` CLI). Splitting it
// out of //codesearch/github keeps the enry + xxhash + codesearch
// schema/types dependencies out of CLI build closures; only the mimetype
// dependency (needed to validate file contents before sending) comes
// with this package.
package gitclient

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/gabriel-vasile/mimetype"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/index"
)

const (
	maxFileLen = 10_000_000

	// The maximum amount of bytes from a file to use for mimetype detection.
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

// GitClient abstracts the operations needed to assemble an incremental
// update: running git commands against a local repo and reading file
// contents relative to that repo.
type GitClient interface {
	ExecuteCommand(args ...string) (string, error)
	LoadFileContents(fileToLoad string) ([]byte, error)
}

type commandLineGitClient struct {
	repoDir string
}

// NewCommandLineGitClient returns a GitClient that shells out to the
// installed `git` binary, scoped to the given repository directory.
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

// DetectionBuffer returns the prefix of the given file contents that is
// used for mimetype and language detection. Exported for reuse by
// //codesearch/github.
func DetectionBuffer(content []byte) []byte {
	return detectionBuffer(content)
}

// ValidateFile checks that the given file contents are suitable for
// indexing (size, mimetype, utf-8). Exported for reuse by
// //codesearch/github.
func ValidateFile(content []byte) error {
	return validateFile(content)
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
