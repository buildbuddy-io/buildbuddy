package schema

import (
	"bytes"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"

	"github.com/gabriel-vasile/mimetype"
	"github.com/go-enry/go-enry/v2"
)

const (
	maxFileLen = 10_000_000

	// The maximum amount of bytes from a file to use for language and
	// mimetype detection.
	detectionBufferSize = 1000

	// The following field names are used in the indexed docs.
	FilenameField = "filename"
	ContentField  = "content"
	LanguageField = "language"
	OwnerField    = "owner"
	RepoField     = "repo"
	SHAField      = "sha"
)

// TODO(tylerw): this should come from a flag?
var skipMime = regexp.MustCompile(`^audio/.*|video/.*|image/.*|application/gzip$`)

func MakeDocument(name, commitSha string, repoURL *git.RepoURL, buf []byte) (types.Document, error) {
	// Skip long files.
	if len(buf) > maxFileLen {
		return nil, fmt.Errorf("skipping %s (file too long)", name)
	}

	var shortBuf []byte
	if len(buf) > detectionBufferSize {
		shortBuf = buf[:detectionBufferSize]
	} else {
		shortBuf = buf
	}

	// Check the mimetype and skip if bad.
	mtype, err := mimetype.DetectReader(bytes.NewReader(shortBuf))
	if err == nil && skipMime.MatchString(mtype.String()) {
		return nil, fmt.Errorf("skipping %s (invalid mime type: %q)", name, mtype.String())
	}

	// Skip non-utf8 encoded files.
	if !utf8.Valid(buf) {
		return nil, fmt.Errorf("skipping %s (non-utf8 content)", name)
	}

	// Compute filetype
	lang := strings.ToLower(enry.GetLanguage(filepath.Base(name), shortBuf))
	doc := types.NewMapDocument(
		map[string]types.NamedField{
			FilenameField: types.NewNamedField(types.TrigramField, FilenameField, []byte(name), true /*=stored*/),
			ContentField:  types.NewNamedField(types.SparseNgramField, ContentField, buf, true /*=stored*/),
			LanguageField: types.NewNamedField(types.StringTokenField, LanguageField, []byte(lang), true /*=stored*/),
			OwnerField:    types.NewNamedField(types.StringTokenField, OwnerField, []byte(repoURL.Owner), true /*=stored*/),
			RepoField:     types.NewNamedField(types.StringTokenField, RepoField, []byte(repoURL.Repo), true /*=stored*/),
			SHAField:      types.NewNamedField(types.StringTokenField, SHAField, []byte(commitSha), true /*=stored*/),
		},
	)
	return doc, nil
}
