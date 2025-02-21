package ociregistry

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"regexp"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"

	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
)

const (
	rangeHeaderBytesPrefix = "bytes="

	dockerContentDigestHeader = "Docker-Content-Digest"
)

var (
	blobsOrManifestsReqRegexp = regexp.MustCompile("/v2/(.+?)/(blobs|manifests)/(.+)")

	enableRegistry = flag.Bool("ociregistry.enabled", false, "Whether to enable registry services")
)

type registry struct {
	env environment.Env
}

func Register(env *real_environment.RealEnv) error {
	if !*enableRegistry {
		return nil
	}

	r, err := New(env)
	if err != nil {
		return err
	}

	env.SetOCIRegistry(r)
	return nil
}

func New(env environment.Env) (*registry, error) {
	r := &registry{
		env: env,
	}
	return r, nil
}

func (r *registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.handleRegistryRequest(w, req)
}

// The OCI registry is intended to be a read-through cache for public OCI images
// (to cut down on the number of API calls to Docker Hub and on bandwidth).
// handleRegistryRequest implements just enough of the [OCI Distribution Spec](https://github.com/opencontainers/distribution-spec/blob/main/spec.md)
// to allow clients to pull OCI images from remote registries that do not require authentication.
// Pushing images is not supported.
func (r *registry) handleRegistryRequest(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	log.CtxDebugf(ctx, "%s %s", req.Method, req.URL)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, r.env)
	if err != nil {
		http.Error(w, fmt.Sprintf("could not attach user prefix: %s", err), http.StatusInternalServerError)
		return
	}
	// Clients issue a GET or HEAD /v2/ request to verify that this  is a registry endpoint.
	if req.RequestURI == "/v2/" {
		w.WriteHeader(http.StatusOK)
		return
	}

	if m := blobsOrManifestsReqRegexp.FindStringSubmatch(req.RequestURI); len(m) == 4 {
		// The image repository name, which can include a registry host and optional port.
		// For example, "alpine" is a repository name. By default, the registry portion is index.docker.io.
		// "mycustomregistry.com:8080/alpine" is also a repository name. The registry portion is mycustomregistry.com:8080.
		repository := m[1]

		blobsOrManifests := m[2]

		// For manifests, the identifier can be a tag (such as "latest") or a digest
		// (such as "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").
		// The OCI image distribution spec refers to this string as <identifier>.
		// However, go-containerregistry has a separate Reference type and refers to this string as `identifier`.
		identifier := m[3] // referrred to as <reference> in the OCI distribution spec, can be a tag or digest

		r.handleBlobsOrManifestsRequest(ctx, w, req, blobsOrManifests, repository, identifier)
		return
	}

	http.NotFound(w, req)
}

func (r *registry) handleBlobsOrManifestsRequest(ctx context.Context, w http.ResponseWriter, inreq *http.Request, blobsOrManifests, repository, identifier string) {
	ref, err := parseReference(repository, identifier)
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing image repository '%s' and identifier '%s': %s", repository, identifier, err), http.StatusNotFound)
		return
	}
	u := url.URL{
		Scheme: ref.Context().Scheme(),
		Host:   ref.Context().RegistryStr(),
		Path:   fmt.Sprintf("/v2/%s/%s/%s", ref.Context().RepositoryStr(), blobsOrManifests, ref.Identifier()),
	}
	upreq, err := http.NewRequest(inreq.Method, u.String(), nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("could not make %s request to upstream registry '%s': %s", inreq.Method, u.String(), err), http.StatusNotFound)
		return
	}
	upresp, err := http.DefaultClient.Do(upreq.WithContext(ctx))
	if err != nil {
		http.Error(w, fmt.Sprintf("transport error making %s request to upstream registry '%s': %s", inreq.Method, u.String(), err), http.StatusNotFound)
		return
	}
	defer upresp.Body.Close()
	w.Header().Add("Content-Type", upresp.Header.Get("Content-Type"))
	w.Header().Add("Docker-Content-Digest", upresp.Header.Get("Docker-Content-Digest"))
	w.Header().Add("Content-Length", upresp.Header.Get("Content-Length"))
	w.WriteHeader(upresp.StatusCode)
	_, err = io.Copy(w, upresp.Body)
	if err != nil {
		if err != context.Canceled {
			log.CtxWarningf(ctx, "error writing response body for '%s', upstream '%s': %s", inreq.URL.String(), u.String(), err)
		}
		return
	}
}

func parseReference(repository, identifier string) (gcrname.Reference, error) {
	joiner := ":"
	if _, err := gcr.NewHash(identifier); err == nil {
		joiner = "@"
	}
	ref, err := gcrname.ParseReference(repository + joiner + identifier)
	if err != nil {
		return nil, err
	}
	return ref, nil
}
