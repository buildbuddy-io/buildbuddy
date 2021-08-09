package static

import (
	"io/fs"
	"net/http"
	"os"
	"path/filepath"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
)

func FSFromRelPath(relPath string) (fs.FS, error) {
	// Figure out where our runfiles (static content bundled with the binary) live.
	rfp, err := bazel.RunfilesPath()
	if err != nil {
		return nil, err
	}
	dirFS := os.DirFS(filepath.Join(rfp, relPath))
	return dirFS, nil
}

// StaticFileServer implements a static file http server that serves static
// files out of the runfiles bundled with this application.
type StaticFileServer struct {
	handler http.Handler
}

// NewStaticFileServer returns a new static file server that will serve the
// content in relpath, optionally stripping the prefix.
func NewStaticFileServer(env environment.Env, fs fs.FS) (*StaticFileServer, error) {
	// Handle "/static/*" requests by serving those static files out of the bundled runfiles.
	return &StaticFileServer{
		handler: http.FileServer(http.FS(fs)),
	}, nil
}

// ServeHTTP implements the HTTP HandlerFunc interface.
func (s *StaticFileServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if (r.URL.Query().Get("hash")) != "" {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable") // 1 year
	}

	s.handler.ServeHTTP(w, r)
}
