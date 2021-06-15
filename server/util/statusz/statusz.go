package statusz

import (
	"context"
	"html/template"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/version"
)

const templateContents = `<!DOCTYPE html>
<html>
  <head>
    <title>/statusz</title>
    <style>
	body {
	  font-family: sans-serif;
	  background: #fff;
	}
	h1 {
	  clear: both;
	  width: 100%;
	  text-align: center;
	  font-size: 120%;
	  background: #eeeeff;
	}
	h2 {
	  clear: both;
	  width: 100%;
	  font-size: 110%;
	  text-align: center;
	  background: #fffddd;
	}
	.lefthand {
	  float: left;
	  width: 80%;
	}
	.righthand {
	  text-align: right;
	}
    </style>
  </head>
    <h1>Status for {{.BinaryName}}</h1>
  <div>
  <div class=lefthand>
    Started at <span>{{.StartTime.Format "Jan 02, 2006 15:04:05 PST" }}</span><br>
    Current time <span>{{.CurrentTime.Format "Jan 02, 2006 15:04:05 PST" }}</span><br>
    App Version <span>{{.AppVersion}} ({{.Commit}})</span><br>
    Go Version <span>{{.GoVersion}}</span><br>
  </div>
  <div class=righthand>
    {{.Username}}@{{.Hostname}}<br>
  </div>

  <br><br><br><br><br>
  {{range $idx, $item := .Sections}}
  <div>
    <h2>
      {{$item.Name}}
    </h2>
    <div>
      {{$item.Description}}<br>
      {{$item.HTML}}
    </div>
  </div>
  {{end}}
</html>
`

var (
	startTime          time.Time
	hostname           string
	username           string
	binaryName         = filepath.Base(os.Args[0])
	statusPageTemplate = template.Must(template.New("statusz").Parse(templateContents))
	DefaultHandler     = NewHandler()
)

func init() {
	startTime = time.Now()
	if h, err := os.Hostname(); err == nil {
		hostname = h
	}
	if u, err := user.Current(); err == nil {
		username = u.Username
	}
}

type StatusReporter interface {
	// Returns nil on success, error on failure. Returning an error will
	// indicate to the health checker that this service is unhealthy and
	// that the server is not ready to serve.
	Statusz(ctx context.Context) string
}
type StatusFunc func(ctx context.Context) string

func (f StatusFunc) Statusz(ctx context.Context) string {
	return f(ctx)
}

type Section struct {
	Name        string
	Description string
	Fn          StatusFunc
}

type Handler struct {
	mu       *sync.RWMutex // PROTECTS(sections)
	sections map[string]*Section
}

func NewHandler() *Handler {
	return &Handler{
		mu:       &sync.RWMutex{},
		sections: make(map[string]*Section, 0),
	}
}

func (h *Handler) AddSection(name, description string, statusReporter StatusReporter) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sections[name] = &Section{
		Name:        name,
		Description: description,
		Fn:          statusReporter.Statusz,
	}
}

type renderedSection struct {
	Name        string
	Description string
	HTML        template.HTML
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	ctx := r.Context()
	orderedSections := make([]*renderedSection, 0, len(h.sections))
	for _, s := range h.sections {
		rs := &renderedSection{
			Name:        s.Name,
			Description: s.Description,
			HTML:        template.HTML(s.Fn(ctx)),
		}
		orderedSections = append(orderedSections, rs)
	}
	sort.Slice(orderedSections, func(i, j int) bool {
		return orderedSections[i].Name < orderedSections[j].Name
	})

	data := struct {
		BinaryName  string
		Commit      string
		Username    string
		Hostname    string
		StartTime   time.Time
		CurrentTime time.Time
		AppVersion  string
		GoVersion   string
		Sections    []*renderedSection
	}{
		BinaryName:  binaryName,
		Commit:      version.Commit(),
		Username:    username,
		Hostname:    hostname,
		StartTime:   startTime,
		CurrentTime: time.Now(),
		AppVersion:  version.AppVersion(),
		GoVersion:   version.GoVersion(),
		Sections:    orderedSections,
	}
	statusPageTemplate.Execute(w, data)
}

func AddSection(name, description string, statusReporter StatusReporter) {
	DefaultHandler.AddSection(name, description, statusReporter)
}

func Server() http.Handler {
	return DefaultHandler
}
