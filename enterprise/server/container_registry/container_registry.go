package container_registry

import (
	"bytes"
	"context"
	"flag"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"github.com/distribution/distribution/v3/configuration"
	registry_handlers "github.com/distribution/distribution/v3/registry/handlers"
	_ "github.com/distribution/distribution/v3/registry/storage/driver/filesystem"
	"github.com/k0kubun/pp/v3"
	"gopkg.in/yaml.v3"
)

var (
	port          = flag.Int("container_registry.port", 0, "The port to run the container registry on. If it is unspecified, the container registry will not run. The recommended port to run on is 443, as that is what `docker buildx` pushes to, regardless of port is specified to it.")
	rootDirectory = flag.String("container_registry.disk.root_directory", "", "The directory to use for container registry storage.")
)

const (
	bb = "bb"
	cr = "cr"
)

const (
	pathSeparator = "/"
	v2Path        = pathSeparator + "v2" + pathSeparator
	blobsPath     = "blobs" + pathSeparator
	manifestsPath = "manifests" + pathSeparator
	uploadsPath   = "uploads" + pathSeparator
	tagsPath      = "tags" + pathSeparator
	referrersPath = "referrers" + pathSeparator
	list          = "list"
)

const (
	name      = "name"
	reference = "reference"
	digest    = "digest"
)

var (
	nameRegexSrc = func() string {
		pathComponent := `[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*`
		return `(?P<` + name + `>` + pathComponent + `(?:` + pathSeparator + pathComponent + `)*)`
	}()
	referenceRegexSrc = `(?P<` + reference + `>[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127})`
	digestRegexSrc    = func() string {
		algorithmComponent := `[a-z0-9]+`
		algorithmSeparator := `[+._-]`
		algorithm := algorithmComponent + `(?:` + algorithmSeparator + algorithmComponent + `)*`
		encoded := `[a-zA-Z0-9=_-]+`
		return `(?P<` + digest + `>` + algorithm + `:` + encoded + `)`
	}()
)

var (
	nameRegex = regexp.MustCompile(
		`^` + nameRegexSrc + `$`,
	)
	referenceRegex = regexp.MustCompile(
		`^` + referenceRegexSrc + `$`,
	)
	digestRegex = regexp.MustCompile(
		`^` + digestRegexSrc + `$`,
	)

	end1Regex = regexp.MustCompile(
		`^` + v2Path + `$`,
	)
	end2Regex = regexp.MustCompile(
		`^` + v2Path + nameRegexSrc + pathSeparator + blobsPath + digestRegexSrc + `$`,
	)
	end3Regex = regexp.MustCompile(
		`^` + v2Path + nameRegexSrc + pathSeparator + manifestsPath + referenceRegexSrc + `$`,
	)
	end4Regex = regexp.MustCompile(
		`^` + v2Path + nameRegexSrc + pathSeparator + blobsPath + uploadsPath + `$`,
	)
	end5Regex = regexp.MustCompile(
		`^` + v2Path + nameRegexSrc + pathSeparator + blobsPath + uploadsPath + referenceRegexSrc + `$`,
	)
	end6Regex = end5Regex
	end7Regex = end3Regex
	end8Regex = regexp.MustCompile(
		`^` + v2Path + nameRegexSrc + pathSeparator + tagsPath + list + `$`,
	)
	end9Regex  = end3Regex
	end10Regex = end2Regex
	end11Regex = end4Regex
	end12Regex = regexp.MustCompile(
		`^` + v2Path + nameRegexSrc + pathSeparator + referrersPath + digestRegexSrc + `$`,
	)
	end13Regex = end5Regex
)

const (
	digestKey       = "digest"
	lastKey         = "last"
	nKey            = "n"
	mountKey        = "mount"
	fromKey         = "from"
	artifactTypeKey = "artifactType"
)

var _ http.Handler = ((*registryHandler)(nil))

type Nameable interface {
	Name() string
}

type Named struct {
	name string
}

func (n *Named) Name() string { return n.name }

type end1 struct{}

func (_ *end1) Name() string { return "" }

type end2 struct {
	Named
	method string
	digest string
}

type end3 struct {
	Named
	method    string
	reference string
}

type end4a struct {
	Named
}

type end4b struct {
	Named
	digest string
}

type end5 struct {
	Named
	reference string
}

type end6 struct {
	Named
	reference string
	digest    string
}

type end7 struct {
	Named
	reference string
}

type end8a struct {
	Named
}

type end8b struct {
	Named
	n    int
	last int
}

type end9 struct {
	Named
	reference string
}

type end10 struct {
	Named
	digest string
}

type end11 struct {
	Named
	mount string
	from  string
}

type end12a struct {
	Named
	digest string
}

type end12b struct {
	Named
	digest       string
	artifactType string
}

type end13 struct {
	Named
	reference string
}

func Endpoint(method string, uri *url.URL) Nameable {
	strings.CutPrefix(uri.Path, v2Path)
	if method == "" {
		method = http.MethodGet
	}
	switch method {
	case http.MethodGet:
		if end1Regex.FindStringSubmatch(uri.Path) != nil {
			return &end1{}
		}
		if m := end2Regex.FindStringSubmatch(uri.Path); m != nil {
			return &end2{
				method: method,
				Named:  Named{name: m[end2Regex.SubexpIndex(name)]},
				digest: m[end2Regex.SubexpIndex(digest)],
			}
		}
		if m := end3Regex.FindStringSubmatch(uri.Path); m != nil {
			return &end3{
				method:    method,
				Named:     Named{name: m[end3Regex.SubexpIndex(name)]},
				reference: m[end3Regex.SubexpIndex(reference)],
			}
		}
		if m := end8Regex.FindStringSubmatch(uri.Path); m != nil {
			if uri.Query().Has(nKey) && uri.Query().Has(lastKey) {
				if n, err := strconv.Atoi(uri.Query().Get(nKey)); err == nil {
					if last, err := strconv.Atoi(uri.Query().Get(lastKey)); err == nil {
						return &end8b{
							Named: Named{name: m[end8Regex.SubexpIndex(name)]},
							n:     n,
							last:  last,
						}
					}
				}
			}
			return &end8a{
				Named: Named{name: m[end8Regex.SubexpIndex(name)]},
			}
		}
		if m := end12Regex.FindStringSubmatch(uri.Path); m != nil {
			if uri.Query().Has(artifactTypeKey) {
				return &end12b{
					Named:        Named{name: m[end12Regex.SubexpIndex(name)]},
					digest:       m[end12Regex.SubexpIndex(digest)],
					artifactType: uri.Query().Get(artifactTypeKey),
				}
			}
			return &end12a{
				Named:  Named{name: m[end12Regex.SubexpIndex(name)]},
				digest: m[end12Regex.SubexpIndex(digest)],
			}
		}
		if m := end13Regex.FindStringSubmatch(uri.Path); m != nil {
			return &end13{
				Named:     Named{name: m[end13Regex.SubexpIndex(name)]},
				reference: m[end13Regex.SubexpIndex(reference)],
			}
		}
	case http.MethodHead:
		if m := end2Regex.FindStringSubmatch(uri.Path); m != nil {
			return &end2{
				method: method,
				Named:  Named{name: m[end2Regex.SubexpIndex(name)]},
				digest: m[end2Regex.SubexpIndex(digest)],
			}
		}
		if m := end3Regex.FindStringSubmatch(uri.Path); m != nil {
			return &end3{
				method:    method,
				Named:     Named{name: m[end3Regex.SubexpIndex(name)]},
				reference: m[end3Regex.SubexpIndex(reference)],
			}
		}
	case http.MethodPost:
		if m := end4Regex.FindStringSubmatch(uri.Path); m != nil {
			if uri.Query().Has(digestKey) {
				digest := uri.Query().Get(digestKey)
				if digestRegex.MatchString(digest) {
					return &end4b{
						Named:  Named{name: m[end4Regex.SubexpIndex(name)]},
						digest: uri.Query().Get(digestKey),
					}
				}
			}
			return &end4a{
				Named: Named{name: m[end4Regex.SubexpIndex(name)]},
			}
		}
		if m := end11Regex.FindStringSubmatch(uri.Path); m != nil {
			if uri.Query().Has(mountKey) && uri.Query().Has(fromKey) {
				mount := uri.Query().Get(mountKey)
				if digestRegex.MatchString(mount) {
					from := uri.Query().Get(fromKey)
					if nameRegex.MatchString(from) {
						return &end11{
							Named: Named{name: m[end11Regex.SubexpIndex(name)]},
							mount: mount,
							from:  from,
						}
					}
				}
			}
		}
	case http.MethodPatch:
		if m := end5Regex.FindStringSubmatch(uri.Path); m != nil {
			return &end5{
				Named:     Named{name: m[end5Regex.SubexpIndex(name)]},
				reference: m[end5Regex.SubexpIndex(reference)],
			}
		}
	case http.MethodPut:
		if m := end6Regex.FindStringSubmatch(uri.Path); m != nil {
			if uri.Query().Has(digestKey) {
				digest := uri.Query().Get(digestKey)
				if digestRegex.MatchString(digest) {
					return &end6{
						Named:     Named{name: m[end6Regex.SubexpIndex(name)]},
						reference: m[end6Regex.SubexpIndex(reference)],
						digest:    uri.Query().Get(digestKey),
					}
				}
			}
		}
		if m := end7Regex.FindStringSubmatch(uri.Path); m != nil {
			return &end7{
				Named:     Named{name: m[end7Regex.SubexpIndex(name)]},
				reference: m[end7Regex.SubexpIndex(reference)],
			}
		}
	case http.MethodDelete:
		if m := end9Regex.FindStringSubmatch(uri.Path); m != nil {
			return &end9{
				Named:     Named{name: m[end9Regex.SubexpIndex(name)]},
				reference: m[end9Regex.SubexpIndex(reference)],
			}
		}
		if m := end10Regex.FindStringSubmatch(uri.Path); m != nil {
			return &end10{
				Named:  Named{name: m[end10Regex.SubexpIndex(name)]},
				digest: m[end10Regex.SubexpIndex(digest)],
			}
		}
	}
	return nil
}

type registry struct {
	server  *http.Server
	handler *registryHandler
}

type registryHandler struct {
	registryApp *registry_handlers.App

	end2Handler   func(http.ResponseWriter, *end2)
	end3Handler   func(http.ResponseWriter, *end3)
	end4aHandler  func(http.ResponseWriter, *end4a)
	end4bHandler  func(http.ResponseWriter, *end4b)
	end5Handler   func(http.ResponseWriter, *end5)
	end6Handler   func(http.ResponseWriter, *end6)
	end7Handler   func(http.ResponseWriter, *end7)
	end8aHandler  func(http.ResponseWriter, *end8a)
	end8bHandler  func(http.ResponseWriter, *end8b)
	end9Handler   func(http.ResponseWriter, *end9)
	end10Handler  func(http.ResponseWriter, *end10)
	end11Handler  func(http.ResponseWriter, *end11)
	end12aHandler func(http.ResponseWriter, *end12a)
	end12bHandler func(http.ResponseWriter, *end12b)
	end13Handler  func(http.ResponseWriter, *end13)
}

func Register(env *real_environment.RealEnv) error {
	if *port == 0 {
		// only set up the container registry if a port is specified.
		return nil
	}
	rs, err := NewRegistryServer(env.GetServerContext(), env.GetListenAddr())
	if err != nil {
		return status.InternalErrorf("Error initializing container registry: %s", err)
	}
	go rs.GetServer().ListenAndServe()
	env.SetContainerRegistry(rs)
	return nil
}

func NewRegistryServer(ctx context.Context, listenHost string) (*registry, error) {
	addr := net.JoinHostPort(listenHost, strconv.Itoa(*port))
	u := build_buddy_url.WithPath("")
	u.Host = net.JoinHostPort(u.Hostname(), strconv.Itoa(*port))
	containerRegistryHost := u.String()
	type section map[string]any
	cfgMap := section{
		"version": "0.1",
		"storage": section{
			"filesystem": section{
				"rootdirectory": *rootDirectory,
			},
		},
		"http": section{
			"addr": addr,
			"host": containerRegistryHost,
		},
	}
	b, err := yaml.Marshal(cfgMap)
	if err != nil {
		return nil, err
	}
	cfg, err := configuration.Parse(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	rh := &registryHandler{
		registryApp: registry_handlers.NewApp(
			ctx,
			cfg,
		),
	}
	rs := &registry{
		server: &http.Server{
			Addr:    addr,
			Handler: rh,
		},
		handler: rh,
	}
	return rs, nil
}

func (r *registry) GetServer() *http.Server {
	return r.server
}

func (h *registryHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	log.Debugf("Container registry request:\n%s", pp.Sprint(req.WithContext(context.Background())))
	u, err := url.ParseRequestURI(req.RequestURI)
	if err != nil {
		log.Debugf("Failed to parse container registry RequestURI. RequestURI: %s, Error: %s", req.RequestURI, err)
		resp.WriteHeader(http.StatusBadRequest)
	}
	e := Endpoint(req.Method, u)
	if e == nil {
		log.Debugf("Bad request made to container registry. Method: %s, RequestURI: %s", req.Method, req.RequestURI)
		resp.WriteHeader(http.StatusBadRequest)
	}
	if e.Name() == "" || strings.HasPrefix(e.Name(), cr+pathSeparator) {
		h.registryApp.ServeHTTP(resp, req)
	} else if strings.HasPrefix(e.Name(), bb+pathSeparator) {
		switch e := e.(type) {
		case *end1:
			// we depend on the normal registryApp for this endpoint, this code should never be reached.
			log.Errorf("Tried to access end-1 on the bb-specific container registry; this call should have been intercepted by the standard registry.")
		case *end2:
			// Must be implemented for pull
			if h.end2Handler != nil {
				h.end2Handler(resp, e)
				return
			}
			log.Errorf("end-2 is not implemented on the bb-specific container registry; this endpoint is required for pull which is the minimum a container registry may implement.")
		case *end3:
			// Must be implemented for pull
			if h.end3Handler != nil {
				h.end3Handler(resp, e)
				return
			}
			log.Errorf("end-3 is not implemented on the bb-specific container registry; this endpoint is required for pull which is the minimum a container registry may implement.")
		case *end4a:
			if h.end4aHandler != nil {
				h.end4aHandler(resp, e)
				return
			}
		case *end4b:
			if h.end4bHandler != nil {
				h.end4bHandler(resp, e)
				return
			}
		case *end5:
			if h.end5Handler != nil {
				h.end5Handler(resp, e)
				return
			}
		case *end6:
			if h.end6Handler != nil {
				h.end6Handler(resp, e)
				return
			}
		case *end7:
			if h.end7Handler != nil {
				h.end7Handler(resp, e)
				return
			}
		case *end8a:
			if h.end8aHandler != nil {
				h.end8aHandler(resp, e)
				return
			}
		case *end8b:
			if h.end8bHandler != nil {
				h.end8bHandler(resp, e)
				return
			}
		case *end9:
			if h.end9Handler != nil {
				h.end9Handler(resp, e)
				return
			}
		case *end10:
			if h.end10Handler != nil {
				h.end10Handler(resp, e)
				return
			}
		case *end11:
			if h.end11Handler != nil {
				h.end11Handler(resp, e)
				return
			}
		case *end12a:
			if h.end12aHandler != nil {
				h.end12aHandler(resp, e)
				return
			}
		case *end12b:
			if h.end12bHandler != nil {
				h.end12bHandler(resp, e)
				return
			}
		case *end13:
			if h.end13Handler != nil {
				h.end13Handler(resp, e)
				return
			}
		}
		resp.WriteHeader(http.StatusNotImplemented)
	} else {
		// only support names beginning with cr/ or bb/
		resp.WriteHeader(http.StatusBadRequest)
	}
}
