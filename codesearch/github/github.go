// TODO(jdelfino): Move common github repo extraction code to this file from
// cli.go and server.go
package github

import (
	"bytes"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"

	"github.com/gabriel-vasile/mimetype"
	"github.com/go-enry/go-enry/v2"

	xxhash "github.com/cespare/xxhash/v2"
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

func ExtractFields(name, commitSha string, repoURL *git.RepoURL, fileContent []byte) (map[string][]byte, error) {
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

	uniqueID := xxhash.Sum64String(repoURL.Owner + repoURL.Repo + name)
	idBytes := []byte(fmt.Sprintf("%d", uniqueID))
	// Compute filetype
	lang := strings.ToLower(enry.GetLanguage(filepath.Base(name), shortBuf))

	return (map[string][]byte{
		schema.IDField:       idBytes,
		schema.FilenameField: []byte(name),
		schema.ContentField:  fileContent,
		schema.LanguageField: []byte(lang),
		schema.OwnerField:    []byte(repoURL.Owner),
		schema.RepoField:     []byte(repoURL.Repo),
		schema.SHAField:      []byte(commitSha),
	}), nil
}
