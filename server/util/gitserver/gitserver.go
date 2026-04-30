// gitserver provides a simple git server that can be accessed via HTTP.
//
// The git server hosts a number of "projects" which are all stored under a
// single project root directory. Each project is located under an OWNER/REPO
// subdirectory within the root directory.
//
// Before pushing a local repository to a project, the project must first be
// created with the server using InitProject.
//
// Each project directory is a "bare" git repository with a few additional
// special files: - git-daemon-export-ok: a marker file indicating that the
// project can be
//   served. This is a standard git file.
// - project.json: a JSON file containing access controls for the project.
//   This is nonstandard, and specific to the gitserver implementation.

// Example usage:
//
//	func main() {
//		projectRoot := "/tmp/.gitprojects"
//		// Add a new project.
//		err := gitserver.InitProject(projectRoot, "testorg", "testrepo")
//		// Run the server.
//		server := gitserver.NewHandler(projectRoot, gitserver.Options{AccessToken: "test-token"})
//		http.ListenAndServe(":8999", server)
//	}
//
// The above example will create an empty project named "testorg/testrepo"
// in the project root directory, /tmp/.gitprojects. The repository can then
// be accessed using the token "test-token". For example:
//
// $ git clone http://x-token:test-token@localhost:8999/testorg/testrepo /tmp/testrepo
// $ cd /tmp/testrepo
// $ touch README.md && git add README.md && git commit -m "add README"
// $ git push -u origin master

package gitserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cgi"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

const (
	// File name of the project settings file within each project directory.
	// This file contains a JSON-encoded ProjectSettings struct.
	projectSettingsFileName = "project.json"
)

// ProjectSettings contains settings for a remote git repository.
type ProjectSettings struct {
	// Public indicates whether the repo is readable by anyone who knows the
	// repo URL, without needing to provide any authentication or authorization.
	Public bool `json:"public"`
}

// CreateProject creates a new bare git repository to be served by the gitserver
// serving from the given gitProjectRoot directory.
//
// The repository will be initialized as a bare repository without a working
// tree.
func CreateProject(gitProjectRoot, owner, repo string, settings *ProjectSettings) error {
	repoPath := filepath.Join(gitProjectRoot, owner, repo)
	if err := os.MkdirAll(repoPath, 0755); err != nil {
		return fmt.Errorf("failed to create repo path %q: %w", repoPath, err)
	}
	// Init as bare repository.
	cmd := exec.Command("git", "init", "--bare", repoPath)
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("git init --bare failed: %q (%w)", strings.TrimSpace(stderr.String()), err)
	}
	// Create git-daemon-export-ok file to make it available for serving.
	if err := os.WriteFile(filepath.Join(repoPath, "git-daemon-export-ok"), nil, 0644); err != nil {
		return fmt.Errorf("create git-daemon-export-ok: %w", err)
	}
	// Write project settings.
	configPath := filepath.Join(repoPath, projectSettingsFileName)
	b, err := json.Marshal(settings)
	if err != nil {
		return fmt.Errorf("marshal project settings: %w", err)
	}
	if err := os.WriteFile(configPath, b, 0644); err != nil {
		return fmt.Errorf("write project settings: %w", err)
	}
	return nil
}

// ProjectExists returns whether a project exists.
func ProjectExists(gitProjectRoot, owner, repo string) (bool, error) {
	repoPath := filepath.Join(gitProjectRoot, owner, repo)
	_, err := os.Stat(repoPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("stat: %w", err)
	}
	return true, nil
}

// ReadProjectSettings reads the project settings for the given project.
func ReadProjectSettings(gitProjectRoot, owner, repo string) (*ProjectSettings, error) {
	repoPath := filepath.Join(gitProjectRoot, owner, repo)
	configPath := filepath.Join(repoPath, projectSettingsFileName)
	b, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	settings := &ProjectSettings{}
	if err := json.Unmarshal(b, settings); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	return settings, nil
}

// Options contains configuration for the gitserver.
type Options struct {
	// AccessToken specifies a global access token that grants access to all
	// repositories. It is intended for testing basic authentication scenarios
	// without having to explicitly configure access controls for each repo.
	AccessToken string

	// LogWriter handles output from the git-http-backend CGI program.
	//
	// If nil, logs are written using log.Info. To explicitly ignore logs, use
	// io.Discard.
	LogWriter io.Writer
}

// NewHandler returns an HTTP handler that serves requests on / from the given
// projectRoot.
//
// All repositories use the "owner/repo" format. To set up static repository
// credentials, use BasicAuthHandler, and wrap this handler with it.
//
// If the projectRoot is non-empty, then it must only contain bare repositories.
// Typically, it's easiest to start with projectRoot as an empty directory, then
// populate it by pushing repositories to the server. It's possible to use an
// existing projectRoot but the repositories have to be "bare" repositories,
// without any commits checked out.
func NewHandler(projectRoot string, opts Options) http.Handler {
	logWriter := opts.LogWriter
	if logWriter == nil {
		logWriter = &infoLogger{}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteUser, ok := auth(w, r, projectRoot, opts.AccessToken)
		if !ok {
			return
		}
		// Note: we don't set GIT_HTTP_EXPORT_ALL. Instead, the file
		// git-daemon-export-ok is used to export each repo separately.
		env := []string{"GIT_PROJECT_ROOT=" + projectRoot}
		if remoteUser != nil {
			env = append(env, "REMOTE_USER="+*remoteUser)
		}
		cgiHandler := &cgi.Handler{
			Path:   "/usr/lib/git-core/git-http-backend",
			Root:   "/",
			Env:    env,
			Stderr: logWriter,
		}
		cgiHandler.ServeHTTP(w, r)
	})
}

// auth authenticates and authorizes a git HTTP request.
//
// The first return value indicates the authenticated and authorized user, or
// nil if unauthenticated or unauthorized.
//
// The second return value 'ok' indicates whether request processing should
// proceed. If it is false, auth will have written an error response to
// the response writer, and the caller should not perform further writes.
func auth(w http.ResponseWriter, r *http.Request, gitProjectRoot, globalAccessToken string) (remoteUser *string, ok bool) {
	// git-receive-pack (e.g. git push) requires auth. If we don't set
	// REMOTE_USER for git-receive-pack requests, then the request will be
	// rejected by git-http-backend, returning an unstructured error message. So
	// we proactively check for this case here and return a 401 if appropriate.
	isUpload := r.URL.Query().Get("service") == "git-receive-pack"

	owner, repo, err := parseOwnerRepo(r)
	if err != nil {
		http.Error(w, "invalid URL: failed to parse owner/repo", 400)
		return nil, false
	}

	// If the basic auth password matches the global access token, skip
	// repo-specific access controls.
	if globalAccessToken != "" {
		if _, token, ok := r.BasicAuth(); ok && token == globalAccessToken {
			remoteUser = &owner
			return remoteUser, true
		}
	}

	project, err := ReadProjectSettings(gitProjectRoot, owner, repo)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "Repository not found", http.StatusNotFound)
			return nil, false
		}
		log.CtxErrorf(r.Context(), "Failed to read project settings: %s", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return nil, false
	}

	// If the repo is public and this is not a write request, then access is
	// granted.
	if project.Public && !isUpload {
		return nil, true
	}

	// If the repo is not public or this is a write request, then we need to
	// authenticate.
	w.Header().Set("WWW-Authenticate", "Basic")
	http.Error(w, "Unauthorized", http.StatusUnauthorized)
	return nil, false
}

func parseOwnerRepo(r *http.Request) (string, string, error) {
	p := r.URL.Path
	p = path.Clean(p)
	p = strings.TrimPrefix(p, "/")
	parts := strings.Split(p, "/")
	if len(parts) < 2 {
		return "", "", fmt.Errorf("invalid number of path components")
	}
	return parts[0], parts[1], nil
}

type infoLogger struct{}

func (l *infoLogger) Write(p []byte) (n int, err error) {
	log.Infof("git-http-backend: %s", string(p))
	return len(p), nil
}
