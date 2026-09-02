package oci

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetch"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/distribution/reference"
	"github.com/google/go-containerregistry/pkg/authn"
	ctrname "github.com/google/go-containerregistry/pkg/name"
	ctr "github.com/google/go-containerregistry/pkg/v1"
)

const (
	// resolveImageDigestLRUMaxEntries limits the number of entries in the image-tag-to-digest cache.
	resolveImageDigestLRUMaxEntries = 1000
	resolveImageDigestLRUDuration   = 15 * time.Minute
)

var (
	registries             = flag.Slice("executor.container_registries", []Registry{}, "")
	defaultKeychainEnabled = flag.Bool("executor.container_registry_default_keychain_enabled", false, "Enable the default container registry keychain, respecting both docker configs and podman configs.")

	// cacheEnabledPercent only matters when the executor fetches from
	// registries itself (OCI fetcher off). It never applies to anonymous
	// tasks, which cannot share a cache.
	cacheEnabledPercent = flag.Int("executor.container_registry.use_cache_percent", 0, "Percentage of image pulls that should use the BuildBuddy remote cache for manifests and layers.")
)

type Registry struct {
	Hostnames []string `yaml:"hostnames" json:"hostnames"`
	Username  string   `yaml:"username" json:"username"`
	Password  string   `yaml:"password" json:"password" config:"secret"`
}

type Credentials struct {
	Username string
	Password string

	// Set if registry auth should be bypassed (can only be set by server
	// admins).
	bypassRegistry bool
}

func CredentialsFromProto(creds *rgpb.Credentials) (Credentials, error) {
	return credentials(creds.GetUsername(), creds.GetPassword())
}

// Extracts the container registry Credentials from the provided platform
// properties, falling back to credentials specified in
// --executor.container_registries if the platform properties credentials are
// absent, then falling back to the default keychain (docker/podman config JSON)
func CredentialsFromProperties(props *platform.Properties) (Credentials, error) {
	imageRef := props.ContainerImage
	if imageRef == "" {
		return Credentials{}, nil
	}

	// Server admins can bypass registry auth (this platform property is guarded
	// by an authorization check in the execution server).
	if props.ContainerRegistryBypass {
		return Credentials{
			bypassRegistry: true,
			// Still forward the username and password - there might be some
			// cases where we actually do have credentials (e.g. our own private
			// images) but still want to bypass the registry if the image is
			// cached.
			Username: props.ContainerRegistryUsername,
			Password: props.ContainerRegistryPassword,
		}, nil
	}

	creds, err := credentials(props.ContainerRegistryUsername, props.ContainerRegistryPassword)
	if err != nil {
		return Credentials{}, fmt.Errorf("Received invalid container-registry-username / container-registry-password combination: %w", err)
	} else if !creds.IsEmpty() {
		return creds, nil
	}

	// If no credentials were provided, fallback to any specified by
	// --executor.container_registries.
	ref, err := reference.ParseNormalizedNamed(imageRef)
	if err != nil {
		log.Debugf("Failed to parse image ref %q: %s", imageRef, err)
		return Credentials{}, nil
	}
	refHostname := reference.Domain(ref)
	for _, cfg := range *registries {
		if slices.Contains(cfg.Hostnames, refHostname) {
			return Credentials{
				Username: cfg.Username,
				Password: cfg.Password,
			}, nil
		}
	}

	// No matching registries were found in the executor config. Fall back to
	// the default keychain.
	if *defaultKeychainEnabled {
		return resolveWithDefaultKeychain(ref)
	}

	return Credentials{}, nil
}

// Reads the auth configuration from a set of commonly supported config file
// locations such as ~/.docker/config.json or
// $XDG_RUNTIME_DIR/containers/auth.json, and returns any configured
// credentials, possibly by invoking a credential helper if applicable.
func resolveWithDefaultKeychain(ref reference.Named) (Credentials, error) {
	// TODO: parse the errors below and if they're 403/401 errors then return
	// Unauthenticated/PermissionDenied
	ctrRef, err := ctrname.ParseReference(ref.String())
	if err != nil {
		log.Debugf("Failed to parse image ref %q: %s", ref.String(), err)
		return Credentials{}, nil
	}
	authenticator, err := authn.DefaultKeychain.Resolve(ctrRef.Context())
	if err != nil {
		return Credentials{}, status.UnavailableErrorf("resolve default keychain: %s", err)
	}
	authConfig, err := authenticator.Authorization()
	if err != nil {
		return Credentials{}, status.UnavailableErrorf("authorize via default keychain: %s", err)
	}
	if authConfig == nil {
		return Credentials{}, nil
	}
	return Credentials{
		Username: authConfig.Username,
		Password: authConfig.Password,
	}, nil
}

func credentials(username, password string) (Credentials, error) {
	if username == "" && password != "" {
		return Credentials{}, status.InvalidArgumentError(
			"malformed credentials: password present with no username")
	} else if username != "" && password == "" {
		return Credentials{}, status.InvalidArgumentError(
			"malformed credentials: username present with no password - if setting 'container-registry-password=$( some-command )', check whether the command failed")
	} else {
		return Credentials{
			Username: username,
			Password: password,
		}, nil
	}
}

func (c Credentials) ToProto() *rgpb.Credentials {
	return &rgpb.Credentials{
		Username: c.Username,
		Password: c.Password,
	}
}

func (c Credentials) IsEmpty() bool {
	return c == Credentials{}
}

func (c Credentials) String() string {
	if c.IsEmpty() {
		return ""
	}
	return c.Username + ":" + c.Password
}

func (c Credentials) Equals(o Credentials) bool {
	return c.Username == o.Username && c.Password == o.Password
}

// Resolver is the executor's view of OCI images. Every image operation goes
// through an ocifetch.Fetcher; fetcherFor decides which one.
type Resolver struct {
	env      environment.Env
	registry *ocifetch.RegistryUpstream

	imageTagToDigestLRU lru.LRU[string]

	mu sync.Mutex
	// Fetchers are built on first use and then reused so that their access
	// proof caches and singleflight groups outlive one pull.
	remoteFetcher *ocifetch.Fetcher // OCIFetcher service upstream, no local cache
	cachedFetcher *ocifetch.Fetcher // registry upstream, remote cache as store
	directFetcher *ocifetch.Fetcher // registry upstream, no cache
}

func NewResolver(env environment.Env) (*Resolver, error) {
	registry, err := ocifetch.NewRegistryUpstream("oci")
	if err != nil {
		return nil, err
	}
	imageTagToDigestLRU, err := lru.New[string](&lru.Config[string]{
		SizeFn:     func(_ string) int64 { return 1 },
		MaxSize:    int64(resolveImageDigestLRUMaxEntries),
		TTL:        resolveImageDigestLRUDuration,
		Clock:      env.GetClock(),
		ThreadSafe: true,
	})
	if err != nil {
		return nil, err
	}
	return &Resolver{
		env:                 env,
		registry:            registry,
		imageTagToDigestLRU: imageTagToDigestLRU,
	}, nil
}

// fetcherFor returns the Fetcher for the calling task. This is the only place
// where the executor's image fetching paths are told apart:
//
//   - useOCIFetcher: the OCIFetcher service on the cache target fetches from
//     the registry and caches. The executor keeps no cache of its own and
//     makes no registry requests.
//   - fetcher off, authenticated task, cache enabled by
//     executor.container_registry.use_cache_percent: the executor fetches
//     from the registry itself and reads and writes the remote cache
//     directly. Groups excluded from the OCI fetcher take this path.
//   - fetcher off, anonymous task (or cache disabled): the executor fetches
//     from the registry itself and caches nothing.
func (r *Resolver) fetcherFor(ctx context.Context, useOCIFetcher bool) (*ocifetch.Fetcher, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case useOCIFetcher:
		if r.remoteFetcher == nil {
			client := r.env.GetOCIFetcherClient()
			if client == nil {
				return nil, status.FailedPreconditionError("OCIFetcherClient is required when useOCIFetcher is true")
			}
			upstream, err := ocifetch.NewRemoteFetcherUpstream(client)
			if err != nil {
				return nil, err
			}
			f, err := ocifetch.New(upstream, nil)
			if err != nil {
				return nil, err
			}
			r.remoteFetcher = f
		}
		return r.remoteFetcher, nil
	case !cacheEnabled() || isAnonymousUser(ctx):
		if r.directFetcher == nil {
			f, err := ocifetch.New(r.registry, nil)
			if err != nil {
				return nil, err
			}
			r.directFetcher = f
		}
		return r.directFetcher, nil
	default:
		if r.cachedFetcher == nil {
			store, err := ocifetch.NewCacheStore(r.env.GetByteStreamClient(), r.env.GetActionCacheClient())
			if err != nil {
				return nil, err
			}
			f, err := ocifetch.New(r.registry, store)
			if err != nil {
				return nil, err
			}
			r.cachedFetcher = f
		}
		return r.cachedFetcher, nil
	}
}

func cacheEnabled() bool {
	if *cacheEnabledPercent >= 100 {
		return true
	}
	if *cacheEnabledPercent <= 0 {
		return false
	}
	return rand.Intn(100) < *cacheEnabledPercent
}

// AuthenticateWithRegistry checks that the credentials grant access to the
// image by fetching its manifest metadata. Nothing is served from a cache, so
// a nil return means the registry (reached directly or via the OCIFetcher
// service) accepted the credentials just now.
func (r *Resolver) AuthenticateWithRegistry(ctx context.Context, imageName string, platform *rgpb.Platform, credentials Credentials, useOCIFetcher bool) error {
	if credentials.bypassRegistry {
		return nil
	}
	log.CtxDebugf(ctx, "Authenticating with registry for %q", imageName)
	imageRef, err := ctrname.ParseReference(imageName)
	if err != nil {
		return status.InvalidArgumentErrorf("invalid image reference %q: %s", imageName, err)
	}
	f, err := r.fetcherFor(ctx, useOCIFetcher)
	if err != nil {
		return err
	}
	_, err = f.FetchManifestMetadata(ctx, imageRef, credentials.ToProto(), ocifetch.Options{})
	return err
}

// ResolveImageDigest takes an image name and returns an image name with a digest.
// If the input image name includes a digest, a canonicalized version of the name is returned.
// If the input image name refers to a tag (either explictly or implicity), ResolveImageDigest
// will fetch the manifest metadata to resolve it.
// ResolveImageDigest keeps an LRU cache that maps between canonical image names with tags
// to image names with digests, to reduce the number of metadata requests.
func (r *Resolver) ResolveImageDigest(ctx context.Context, imageName string, platform *rgpb.Platform, credentials Credentials, useOCIFetcher bool) (string, error) {
	if imageRefWithDigest, err := ctrname.NewDigest(imageName); err == nil {
		return imageRefWithDigest.String(), nil
	}
	tagRef, err := ctrname.ParseReference(imageName)
	if err != nil {
		return "", status.InvalidArgumentErrorf("invalid image name %q", imageName)
	}
	if nameWithDigest, ok := r.imageTagToDigestLRU.Get(tagRef.String()); ok {
		return nameWithDigest, nil
	}
	f, err := r.fetcherFor(ctx, useOCIFetcher)
	if err != nil {
		return "", err
	}
	desc, err := f.FetchManifestMetadata(ctx, tagRef, credentials.ToProto(), ocifetch.Options{})
	if err != nil {
		return "", err
	}
	imageNameWithDigest := tagRef.Context().Digest(desc.Digest.String()).String()
	r.imageTagToDigestLRU.Add(tagRef.String(), imageNameWithDigest)
	return imageNameWithDigest, nil
}

// Resolve returns an Image for imageName whose config and layers are read
// through the Fetcher chosen by fetcherFor.
func (r *Resolver) Resolve(ctx context.Context, imageName string, platform *rgpb.Platform, credentials Credentials, useOCIFetcher bool) (ctr.Image, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	imageRef, err := ctrname.ParseReference(imageName)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image %q", imageName)
	}
	log.CtxDebugf(ctx, "Resolving image %q", imageRef)
	f, err := r.fetcherFor(ctx, useOCIFetcher)
	if err != nil {
		return nil, err
	}
	return f.Image(
		ctx,
		imageRef,
		ctr.Platform{
			Architecture: platform.GetArch(),
			OS:           platform.GetOs(),
			Variant:      platform.GetVariant(),
		},
		credentials.ToProto(),
		ocifetch.Options{BypassRegistry: credentials.bypassRegistry},
	)
}

// RuntimePlatform returns the platform on which the program is being executed,
// as reported by the go runtime.
func RuntimePlatform() *rgpb.Platform {
	return &rgpb.Platform{
		Arch: runtime.GOARCH,
		Os:   runtime.GOOS,
	}
}

func isAnonymousUser(ctx context.Context) bool {
	_, err := claims.ClaimsFromContext(ctx)
	return authutil.IsAnonymousUserError(err)
}

// RegistryETLDPlusOne extracts the eTLD+1 of the registry host from a
// container image reference string. It uses go-containerregistry to parse the
// reference, which handles implicit docker.io defaults, tags, digests, and
// ports. For IP-address registries, it returns the raw IP. Returns
// "[UNKNOWN]" if the reference cannot be parsed.
func RegistryETLDPlusOne(imageRef string) string {
	ref, err := ctrname.ParseReference(imageRef)
	if err != nil {
		return "[UNKNOWN]"
	}
	host := ref.Context().RegistryStr()
	// Strip port if present (RegistryStr may include it).
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return httpclient.HostLabel(host)
}
