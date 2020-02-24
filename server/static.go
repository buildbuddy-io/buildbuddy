package static

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
)

// StaticFileServer implements a static file http server that serves static
// files out of the runfiles bundled with this application.
type StaticFileServer struct {
	relPath string
	handler http.Handler
}

// NewStaticFileServer returns a new static file server that will serve the
// content in relpath, optionally stripping the prefix.
func NewStaticFileServer(relPath string, stripPrefix bool, rootPaths []string) (*StaticFileServer, error) {
	// Figure out where our runfiles (static content bundled with the binary) live.
	rfp, err := bazel.RunfilesPath()
	if err != nil {
		return nil, err
	}
	// Handle "/static/*" requests by serving those static files out of the bundled runfiles.
	pkgStaticDir := filepath.Join(rfp, relPath)
	handler := http.FileServer(http.Dir(pkgStaticDir))
	if stripPrefix {
		handler = http.StripPrefix(relPath, handler)
	}
	if len(rootPaths) > 0 {
		handler = handleRootPaths(rootPaths, handler)
	}
	return &StaticFileServer{
		relPath: relPath,
		handler: handler,
	}, nil
}

// ServeHTTP implements the HTTP HandlerFunc interface.
func (s *StaticFileServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cwd, _ := os.Getwd()
	fp := filepath.Join(cwd, s.relPath, r.URL.Path)
	_, err := os.Stat(fp)
	log.Printf("SFS @ %s got request for: %s, (file: %s, exists? %t)", s.relPath, r.URL.Path, fp, !os.IsNotExist(err))
	s.handler.ServeHTTP(w, r)
}

func handleRootPaths(rootPaths []string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, rootPath := range rootPaths {
			if strings.HasPrefix(r.URL.Path, rootPath) {
				r.URL.Path = "/"
			}
		}

		h.ServeHTTP(w, r)
	})
}
