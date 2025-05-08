// TODO(jdelfino): Move common github repo extraction code to this file from
// cli.go and server.go
package github

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
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

func IndexLocalRepo(dir string, iw types.IndexWriter, docSchema types.DocumentSchema) error {
	repoURL := extractRepoURL(dir)
	commitSHA := extractGitSHA(dir)
	r := &gitIndexer{
		repoURL:   repoURL,
		commitSHA: commitSHA,
		iw:        iw,
		docSchema: docSchema,
	}
	return r.indexLocalRepo(dir)
}

func IndexGitHubRepo(repoURL *git.RepoURL, commitSHA, username, accessToken, scratchDir string, iw types.IndexWriter, docSchema types.DocumentSchema) error {
	r := &gitIndexer{
		repoURL:   repoURL,
		commitSHA: commitSHA,
		iw:        iw,
		docSchema: docSchema,
	}
	tmpFileName, cleanup, err := r.downloadRepo(username, accessToken, scratchDir)
	if err != nil {
		return err
	}
	defer cleanup()
	return r.indexArchive(tmpFileName)
}

type gitIndexer struct {
	repoURL   *git.RepoURL
	commitSHA string
	iw        types.IndexWriter
	docSchema types.DocumentSchema
}

func (r *gitIndexer) indexLocalRepo(dir string) error {
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if _, elem := filepath.Split(path); elem != "" {
			// Skip various temporary or "hidden" files or directories.
			if elem[0] == '.' || elem[0] == '#' || elem[0] == '~' || elem[len(elem)-1] == '~' {
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
		}
		if err != nil {
			return err
		}
		if info != nil && info.Mode()&os.ModeType == 0 {
			buf, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			err = r.processFileContents(path, buf)
			if err != nil {
				return err
			}
		}
		return nil
	})
	// TODO: handle errors properly
	return nil
}

func (r *gitIndexer) downloadRepo(username, accessToken, scratchDirectory string) (string, func(), error) {
	var cleanupFunc func() = nil

	archiveURL, err := apiArchiveURL(r.repoURL, r.commitSHA, username, accessToken)
	if err != nil {
		return "", cleanupFunc, err
	}

	httpRsp, err := http.Get(archiveURL)
	if err != nil {
		return "", cleanupFunc, err
	}
	defer httpRsp.Body.Close()

	tmpFile, err := os.CreateTemp(scratchDirectory, "archive-*.zip")
	if err != nil {
		return "", cleanupFunc, err
	}
	cleanupFunc = func() { os.Remove(tmpFile.Name()) }

	if _, err := io.Copy(tmpFile, httpRsp.Body); err != nil {
		return "", cleanupFunc, err
	}
	return tmpFile.Name(), cleanupFunc, nil
}

func (r *gitIndexer) indexArchive(archivePath string) error {
	zipReader, err := zip.OpenReader(archivePath)
	if err != nil {
		return err
	}

	defer zipReader.Close()
	for _, file := range zipReader.File {
		parts := strings.Split(file.Name, string(filepath.Separator))
		if len(parts) == 1 {
			continue
		}
		filename := filepath.Join(parts[1:]...)

		rc, err := file.Open()
		if err != nil {
			return err
		}
		defer rc.Close()
		buf, err := io.ReadAll(rc)
		if err != nil {
			return err
		}

		err = r.processFileContents(filename, buf)
		if err != nil {
			continue
		}
	}
	return nil
}

func (r *gitIndexer) processFileContents(filename string, buf []byte) error {
	doc, err := makeDocument(r.repoURL, r.commitSHA, filename, buf, r.docSchema)
	if err != nil {
		return err
	}

	if err := r.iw.UpdateDocument(doc.Field(schema.IDField), doc); err != nil {
		return err
	}
	return nil
}

func makeDocument(repoURL *git.RepoURL, commitSHA, filename string, fileContent []byte, docSchema types.DocumentSchema) (types.Document, error) {
	// Skip long files.
	if len(fileContent) > maxFileLen {
		return nil, fmt.Errorf("skipping %s (file too long)", filename)
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
		return nil, fmt.Errorf("skipping %s (invalid mime type: %q)", filename, mtype.String())
	}

	// Skip non-utf8 encoded files.
	if !utf8.Valid(fileContent) {
		return nil, fmt.Errorf("skipping %s (non-utf8 content)", filename)
	}

	uniqueID := xxhash.Sum64String(repoURL.Owner + repoURL.Repo + filename)
	idBytes := []byte(fmt.Sprintf("%d", uniqueID))
	// Compute filetype
	lang := strings.ToLower(enry.GetLanguage(filepath.Base(filename), shortBuf))

	return docSchema.MakeDocument(map[string][]byte{
		schema.IDField:       idBytes,
		schema.FilenameField: []byte(filename),
		schema.ContentField:  fileContent,
		schema.LanguageField: []byte(lang),
		schema.OwnerField:    []byte(repoURL.Owner),
		schema.RepoField:     []byte(repoURL.Repo),
		schema.SHAField:      []byte(commitSHA),
	})
}

// apiArchiveURL takes a url like https://github.com/buildbuddy-io/buildbuddy
// and a commit SHA, username, and access token, and generates a github API zip
// archive download URL like:
// https://api.github.com/repos/buildbuddy-io/buildbuddy-internal/zipball/sha12312312313
func apiArchiveURL(repoURL *git.RepoURL, commitSHA, username, accessToken string) (string, error) {
	authRepoURL, err := git.AuthRepoURL(repoURL.String(), username, accessToken)
	if err != nil {
		return "", err
	}
	u, err := url.Parse(authRepoURL)
	if err != nil {
		return "", err
	}
	reposPath, err := url.JoinPath("/repos/", u.Path)
	if err != nil {
		return "", err
	}
	u.Path = reposPath
	u.Host = "api.github.com"
	u = u.JoinPath("/zipball/", commitSHA)
	return u.String(), nil
}

func extractRepoURL(dir string) *git.RepoURL {
	cmd := exec.Command("git", "config", "--get", "remote.origin.url")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err == nil {
		if repoURL, err := git.ParseGitHubRepoURL(strings.TrimSpace(string(out))); err == nil {
			return repoURL
		}
	}
	return &git.RepoURL{}
}

func extractGitSHA(dir string) string {
	cmd := exec.Command("git", "rev-parse", "--verify", "HEAD")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err == nil {
		return strings.TrimSpace(string(out))
	}
	return ""
}
