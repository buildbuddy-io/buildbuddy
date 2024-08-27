package registry

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
)

type registry struct {
	log              *log.Logger
	blobs            blobs
	manifests        manifests
	referrersEnabled bool
	warnings         map[float64]string
	env              environment.Env
}

// https://docs.docker.com/registry/spec/api/#api-version-check
// https://github.com/opencontainers/distribution-spec/blob/master/spec.md#api-version-check
func (r *registry) v2(resp http.ResponseWriter, req *http.Request) *regError {
	if r.warnings != nil {
		rnd := rand.Float64()
		for prob, msg := range r.warnings {
			if prob > rnd {
				resp.Header().Add("Warning", fmt.Sprintf(`299 - "%s"`, msg))
			}
		}
	}

	ctx := req.Context()
	ctx, err := prefix.AttachUserPrefixToContext(ctx, r.env)
	if err != nil {
		panic(err)
	}

	if isBlob(req) {
		return r.blobs.handle(ctx, resp, req)
	}
	if isManifest(req) {
		return r.manifests.handle(resp, req)
	}
	if isTags(req) {
		return r.manifests.handleTags(resp, req)
	}
	if isCatalog(req) {
		return r.manifests.handleCatalog(resp, req)
	}
	if r.referrersEnabled && isReferrers(req) {
		return r.manifests.handleReferrers(resp, req)
	}
	resp.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	if req.URL.Path != "/v2/" && req.URL.Path != "/v2" {
		return &regError{
			Status:  http.StatusNotFound,
			Code:    "METHOD_UNKNOWN",
			Message: "We don't understand your method + url",
		}
	}
	resp.WriteHeader(200)
	return nil
}

func (r *registry) root(resp http.ResponseWriter, req *http.Request) {
	if rerr := r.v2(resp, req); rerr != nil {
		r.log.Printf("%s %s %d %s %s", req.Method, req.URL, rerr.Status, rerr.Code, rerr.Message)
		rerr.Write(resp)
		return
	}
	r.log.Printf("%s %s", req.Method, req.URL)
}

// New returns a handler which implements the docker registry protocol.
// It should be registered at the site root.
// TODO(iain): paths might cross with the rest of our stuff :-(
func New(env environment.Env, opts ...Option) http.Handler {
	r := &registry{
		log: log.New(os.Stderr, "", log.LstdFlags),
		blobs: blobs{
			blobHandler: &handler{cache: env.GetCache()},
			uploads:     map[string][]byte{},
			log:         log.New(os.Stderr, "", log.LstdFlags),
		},
		manifests: manifests{
			env:   env,
			cache: env.GetCache(),
		},
		env: env,
	}
	for _, o := range opts {
		o(r)
	}
	return http.HandlerFunc(r.root)
}

// Option describes the available options
// for creating the registry.
type Option func(r *registry)

// WithReferrersSupport enables the referrers API endpoint (OCI 1.1+)
func WithReferrersSupport(enabled bool) Option {
	return func(r *registry) {
		r.referrersEnabled = enabled
	}
}
