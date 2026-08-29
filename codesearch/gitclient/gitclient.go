// Package gitclient extracts incremental codesearch index updates from a local
// git checkout. It is deliberately a leaf package: the bb CLI ("bb index")
// depends on it, and it must not pull the indexer (tree-sitter grammars,
// language detection, the index format) into the CLI binary.
package gitclient

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

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
	skipMime    = regexp.MustCompile(`^audio/.*|video/.*|image/.*|application/(x-)?gzip$`)
	regexpSha   = regexp.MustCompile("^[0-9a-f]{5,40}$")
	filepathSha = regexp.MustCompile("^:[0-9]{6} [0-9]{6}")
)

// looksLikeSVG reports whether the prefix of a file is an SVG document: an
// optional XML declaration / comments / DOCTYPE followed by an <svg root
// element (the same heuristic gabriel-vasile/mimetype used).
func looksLikeSVG(prefix []byte) bool {
	p := bytes.TrimLeft(prefix, " \t\r\n\xef\xbb\xbf")
	for len(p) > 0 && p[0] == '<' {
		switch {
		case bytes.HasPrefix(p, []byte("<svg")):
			return true
		case bytes.HasPrefix(p, []byte("<?")):
			i := bytes.Index(p, []byte("?>"))
			if i < 0 {
				return false
			}
			p = p[i+2:]
		case bytes.HasPrefix(p, []byte("<!--")):
			i := bytes.Index(p, []byte("-->"))
			if i < 0 {
				return false
			}
			p = p[i+3:]
		case bytes.HasPrefix(p, []byte("<!")):
			i := bytes.IndexByte(p, '>')
			if i < 0 {
				return false
			}
			p = p[i+1:]
		default:
			return false
		}
		p = bytes.TrimLeft(p, " \t\r\n")
	}
	return false
}

func detectionBuffer(content []byte) []byte {
	if len(content) > detectionBufferSize {
		return content[:detectionBufferSize]
	}
	return content
}

// ValidateFile returns an error if the file content is not indexable (too
// large, a binary/media mimetype, or not valid UTF-8).
func ValidateFile(content []byte) error {
	if len(content) > maxFileLen {
		return fmt.Errorf("file too long")
	}

	shortBuf := detectionBuffer(content)

	// Check the mimetype and skip if not valid for indexing. The standard
	// library's sniffer (the WHATWG mime-sniffing algorithm) knows the common
	// binary image/audio/video/gzip signatures; the gabriel-vasile/mimetype
	// library it replaces knew many more (FLAC, HEIC, TIFF, PSD, ...), but
	// those are binary and rejected by the UTF-8 check below anyway. The two
	// media formats that are valid text and were previously skipped, SVG and
	// Ogg, are handled explicitly; other text-based image formats (XPM, X
	// bitmaps) are now indexed like any other text.
	mtype := http.DetectContentType(shortBuf)
	if mtype == "application/ogg" || looksLikeSVG(shortBuf) {
		mtype = "image/svg+xml"
		if bytes.HasPrefix(shortBuf, []byte("OggS")) {
			mtype = "audio/ogg"
		}
	}
	if skipMime.MatchString(mtype) {
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

	if err := ValidateFile(content); err != nil {
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
