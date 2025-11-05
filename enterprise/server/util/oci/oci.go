package oci

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"runtime"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocicache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocimanifest"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/distribution/reference"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/v1/partial"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/google/go-containerregistry/pkg/v1/types"

	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
)

var (
	registries             = flag.Slice("executor.container_registries", []Registry{}, "")
	mirrors                = flag.Slice("executor.container_registry_mirrors", []MirrorConfig{}, "")
	defaultKeychainEnabled = flag.Bool("executor.container_registry_default_keychain_enabled", false, "Enable the default container registry keychain, respecting both docker configs and podman configs.")
	allowedPrivateIPs      = flag.Slice("executor.container_registry_allowed_private_ips", []string{}, "Allowed private IP ranges for container registries. Private IPs are disallowed by default.")

	useCachePercent = flag.Int("executor.container_registry.use_cache_percent", 0, "Percentage of image pulls that should use the BuildBuddy remote cache for manifests and layers.")
	// TODO: remove from configs and delete
	_ = flag.Bool("executor.container_registry.write_manifests_to_cache", false, "", flag.Internal)
	_ = flag.Bool("executor.container_registry.read_manifests_from_cache", false, "", flag.Internal)
	_ = flag.Bool("executor.container_registry.write_layers_to_cache", false, "", flag.Internal)
	_ = flag.Bool("executor.container_registry.read_layers_from_cache", false, "", flag.Internal)
)

type MirrorConfig struct {
	OriginalURL string `yaml:"original_url" json:"original_url"`
	MirrorURL   string `yaml:"mirror_url" json:"mirror_url"`
}

func (mc MirrorConfig) matches(u *url.URL) (bool, error) {
	originalURL, err := url.Parse(mc.OriginalURL)
	if err != nil {
		return false, err
	}
	match := originalURL.Host == u.Host
	return match, nil
}

func (mc MirrorConfig) rewriteRequest(originalRequest *http.Request) (*http.Request, error) {
	mirrorURL, err := url.Parse(mc.MirrorURL)
	if err != nil {
		return nil, err
	}
	originalURL := originalRequest.URL.String()
	req := originalRequest.Clone(originalRequest.Context())
	req.URL.Scheme = mirrorURL.Scheme
	req.URL.Host = mirrorURL.Host
	//Set X-Forwarded-Host so the mirror knows which remote registry to make requests to.
	//ociregistry looks for this header and will default to forwarding requests to Docker Hub if not found.
	req.Header.Set("X-Forwarded-Host", originalRequest.URL.Host)
	log.Debugf("%q rewritten to %s", originalURL, req.URL.String())
	return req, nil
}

func (mc MirrorConfig) rewriteFallbackRequest(originalRequest *http.Request) (*http.Request, error) {
	originalURL, err := url.Parse(mc.OriginalURL)
	if err != nil {
		return nil, err
	}
	req := originalRequest.Clone(originalRequest.Context())
	req.URL.Scheme = originalURL.Scheme
	req.URL.Host = originalURL.Host
	log.Debugf("(fallback) %q rewritten to %s", originalURL, req.URL.String())
	return req, nil
}

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
		for _, cfgHostname := range cfg.Hostnames {
			if refHostname == cfgHostname {
				return Credentials{
					Username: cfg.Username,
					Password: cfg.Password,
				}, nil
			}
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
	ctrRef, err := gcrname.ParseReference(ref.String())
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
			"malformed credentials: username present with no password")
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

type Resolver struct {
	env environment.Env

	allowedPrivateIPs []*net.IPNet
}

func NewResolver(env environment.Env) (*Resolver, error) {
	allowedPrivateIPNets := make([]*net.IPNet, 0, len(*allowedPrivateIPs))
	for _, r := range *allowedPrivateIPs {
		_, ipNet, err := net.ParseCIDR(r)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("invald value %q for executor.container_registry_allowed_private_ips flag: %s", r, err)
		}
		allowedPrivateIPNets = append(allowedPrivateIPNets, ipNet)
	}
	return &Resolver{
		env:               env,
		allowedPrivateIPs: allowedPrivateIPNets,
	}, nil
}

// AuthenticateWithRegistry makes a HEAD request to a remote registry with the input credentials.
// Any errors encountered are returned.
// Otherwise, the function returns nil and it is safe to assume the input credentials grant access
// to the image.
func (r *Resolver) AuthenticateWithRegistry(ctx context.Context, imageName string, platform *rgpb.Platform, credentials Credentials) error {
	if credentials.bypassRegistry {
		return nil
	}

	imageRef, err := gcrname.ParseReference(imageName)
	if err != nil {
		return status.InvalidArgumentErrorf("invalid image %q", imageName)
	}
	log.CtxInfof(ctx, "Authenticating with registry %q", imageRef.Context().RegistryStr())

	remoteOpts := r.getRemoteOpts(ctx, platform, credentials)
	// Authenticate doesn't need caching, just needs to verify access
	cacher, err := ocicache.NewOCITeeCacher(false, r.env, remoteOpts...)
	if err != nil {
		return status.InternalErrorf("error creating cacher: %s", err)
	}
	_, err = cacher.Head(ctx, imageRef)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return status.PermissionDeniedErrorf("not authorized to access image manifest: %s", err)
		}
		return status.UnavailableErrorf("could not authorize to remote registry: %s", err)
	}
	return nil
}

// ResolveImageDigest takes an image name and returns an image name with a digest.
// If the input image name includes a digest, a canonicalized version of the name is returned.
// If the input image name refers to a tag (either explictly or implicity), ResolveImageDigest
// will make a HEAD request to the remote registry.
// The OCITeeCacher keeps an in-memory cache that maps between canonical image names with tags
// to image names with digests, to reduce the number of HEAD requests.
func (r *Resolver) ResolveImageDigest(ctx context.Context, imageName string, platform *rgpb.Platform, credentials Credentials) (string, error) {
	if imageRefWithDigest, err := gcrname.NewDigest(imageName); err == nil {
		return imageRefWithDigest.String(), nil
	}
	tagRef, err := gcrname.ParseReference(imageName)
	if err != nil {
		return "", status.InvalidArgumentErrorf("invalid image name %q", imageName)
	}

	remoteOpts := r.getRemoteOpts(ctx, platform, credentials)
	// ResolveImageDigest just needs to lookup tag->digest mapping, doesn't need AC/CAS caching
	cacher, err := ocicache.NewOCITeeCacher(false, r.env, remoteOpts...)
	if err != nil {
		return "", status.InternalErrorf("error creating cacher: %s", err)
	}
	desc, err := cacher.Head(ctx, tagRef)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return "", status.PermissionDeniedErrorf("not authorized to access image manifest: %s", err)
		}
		return "", status.UnavailableErrorf("could not authorize to remote registry: %s", err)
	}
	return tagRef.Context().Digest(desc.Digest.String()).String(), nil
}

func (r *Resolver) Resolve(ctx context.Context, imageName string, platform *rgpb.Platform, credentials Credentials) (gcr.Image, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	imageRef, err := gcrname.ParseReference(imageName)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image %q", imageName)
	}
	log.CtxInfof(ctx, "Resolving image %q", imageRef)

	useCache := false
	if *useCachePercent >= 100 {
		useCache = true
	} else if *useCachePercent > 0 && *useCachePercent < 100 {
		useCache = rand.Intn(100) < *useCachePercent
	}

	remoteOpts := r.getRemoteOpts(ctx, platform, credentials)
	cacher, err := ocicache.NewOCITeeCacher(useCache, r.env, remoteOpts...)
	if err != nil {
		return nil, status.InternalErrorf("error creating cacher: %s", err)
	}

	remoteDesc, err := cacher.Get(ctx, imageRef)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("not authorized to retrieve image manifest: %s", err)
		}
		return nil, status.UnavailableErrorf("could not retrieve manifest from remote: %s", err)
	}

	gcrPlatform := gcr.Platform{
		Architecture: platform.GetArch(),
		OS:           platform.GetOs(),
		Variant:      platform.GetVariant(),
	}

	// For cached manifests, we need to handle platform resolution manually because
	// the cached remote.Descriptor doesn't have the internal state needed by Image().
	if useCache && remoteDesc.MediaType.IsIndex() {
		// Parse the index manifest
		indexManifest, err := gcr.ParseIndexManifest(bytes.NewReader(remoteDesc.Manifest))
		if err != nil {
			return nil, status.UnknownErrorf("error parsing index manifest: %s", err)
		}

		// Find the child manifest for this platform
		childDesc, err := ocimanifest.FindFirstImageManifest(*indexManifest, gcrPlatform)
		if err != nil {
			return nil, status.UnknownErrorf("could not find child image for platform in index: %s", err)
		}

		// Fetch the platform-specific child image using the same cacher to avoid extra /v2/ requests
		childRef := imageRef.Context().Digest(childDesc.Digest.String())
		childRemoteDesc, err := cacher.Get(ctx, childRef)
		if err != nil {
			if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
				return nil, status.PermissionDeniedErrorf("not authorized to retrieve child manifest: %s", err)
			}
			return nil, status.UnavailableErrorf("could not retrieve child manifest from remote: %s", err)
		}

		childImg, err := childRemoteDesc.Image()
		if err != nil {
			return nil, status.UnknownErrorf("could not get image from child manifest descriptor: %s", err)
		}

		// Wrap with caching support
		return &cachedImage{
			Image:  childImg,
			ctx:    ctx,
			repo:   imageRef.Context(),
			cacher: cacher,
		}, nil
	}

	// For non-cached descriptors or non-index manifests, use Image() which handles
	// platform resolution automatically.
	img, err := remoteDesc.Image()
	if err != nil {
		switch remoteDesc.MediaType {
		case types.OCIImageIndex, types.DockerManifestList:
			return nil, status.UnknownErrorf("could not get image in image index from descriptor: %s", err)
		case types.OCIManifestSchema1, types.DockerManifestSchema2:
			return nil, status.UnknownErrorf("could not get image from descriptor: %s", err)
		default:
			return nil, status.UnknownErrorf("descriptor has unknown media type %q, oci error: %s", remoteDesc.MediaType, err)
		}
	}

	// Wrap with caching support if enabled
	if useCache {
		return &cachedImage{
			Image:  img,
			ctx:    ctx,
			repo:   imageRef.Context(),
			cacher: cacher,
		}, nil
	}

	return img, nil
}

// cachedImage wraps a gcr.Image and intercepts layer operations to add caching support.
// It delegates platform resolution and manifest parsing to the underlying Image (which comes
// from remote.Descriptor.Image()), but overrides layer-fetching methods to use the BuildBuddy cache.
type cachedImage struct {
	gcr.Image // Embed the underlying image for delegation

	ctx    context.Context
	repo   gcrname.Repository
	cacher ocicache.OCITeeCacher
}

// Layers overrides the embedded Image's Layers() method to add caching support.
func (ci *cachedImage) Layers() ([]gcr.Layer, error) {
	manifest, err := ci.Image.Manifest()
	if err != nil {
		return nil, err
	}

	layers := make([]gcr.Layer, len(manifest.Layers))
	for i, layerDesc := range manifest.Layers {
		ref := ci.repo.Digest(layerDesc.Digest.String())
		layer, err := ci.cacher.LayerFromDescriptor(ci.ctx, ref, layerDesc, ci.Image)
		if err != nil {
			return nil, err
		}
		layers[i] = layer
	}
	return layers, nil
}

// LayerByDigest overrides the embedded Image's LayerByDigest() method to add caching support.
func (ci *cachedImage) LayerByDigest(digest gcr.Hash) (gcr.Layer, error) {
	// Get the layer descriptor from the manifest if available
	manifest, err := ci.Image.Manifest()
	if err != nil {
		return nil, err
	}

	// Find the descriptor for this digest
	var layerDesc *gcr.Descriptor
	for _, desc := range manifest.Layers {
		if desc.Digest == digest {
			layerDesc = &desc
			break
		}
	}

	ref := ci.repo.Digest(digest.String())
	if layerDesc != nil {
		// Use LayerFromDescriptor when we have the descriptor (avoids extra HTTP requests)
		return ci.cacher.LayerFromDescriptor(ci.ctx, ref, *layerDesc, ci.Image)
	}
	// Fall back to Layer() if descriptor not found in manifest
	return ci.cacher.Layer(ci.ctx, ref)
}

// LayerByDiffID overrides the embedded Image's LayerByDiffID() method to add caching support.
func (ci *cachedImage) LayerByDiffID(diffID gcr.Hash) (gcr.Layer, error) {
	// Convert DiffID to blob digest using the image config
	digest, err := partial.DiffIDToBlob(ci.Image, diffID)
	if err != nil {
		return nil, err
	}
	return ci.LayerByDigest(digest)
}

func (r *Resolver) getRemoteOpts(ctx context.Context, platform *rgpb.Platform, credentials Credentials) []remote.Option {
	remoteOpts := []remote.Option{
		remote.WithContext(ctx),
		remote.WithPlatform(
			gcr.Platform{
				Architecture: platform.GetArch(),
				OS:           platform.GetOs(),
				Variant:      platform.GetVariant(),
			},
		),
	}
	if !credentials.IsEmpty() {
		remoteOpts = append(remoteOpts, remote.WithAuth(&authn.Basic{
			Username: credentials.Username,
			Password: credentials.Password,
		}))
	}

	tr := httpclient.New(r.allowedPrivateIPs, "oci").Transport
	if len(*mirrors) > 0 {
		remoteOpts = append(remoteOpts, remote.WithTransport(newMirrorTransport(tr, *mirrors)))
	} else {
		remoteOpts = append(remoteOpts, remote.WithTransport(tr))
	}
	return remoteOpts
}

// RuntimePlatform returns the platform on which the program is being executed,
// as reported by the go runtime.
func RuntimePlatform() *rgpb.Platform {
	return &rgpb.Platform{
		Arch: runtime.GOARCH,
		Os:   runtime.GOOS,
	}
}

// verify that mirrorTransport implements the RoundTripper interface.
var _ http.RoundTripper = (*mirrorTransport)(nil)

type mirrorTransport struct {
	inner   http.RoundTripper
	mirrors []MirrorConfig
}

func newMirrorTransport(inner http.RoundTripper, mirrors []MirrorConfig) http.RoundTripper {
	return &mirrorTransport{
		inner:   inner,
		mirrors: mirrors,
	}
}

func (t *mirrorTransport) RoundTrip(in *http.Request) (out *http.Response, err error) {
	for _, mirror := range t.mirrors {
		if match, err := mirror.matches(in.URL); err == nil && match {
			mirroredRequest, err := mirror.rewriteRequest(in)
			if err != nil {
				log.Errorf("error mirroring request: %s", err)
				continue
			}
			out, err := t.inner.RoundTrip(mirroredRequest)
			if err != nil {
				log.Errorf("mirror err: %s", err)
				continue
			}
			if out.StatusCode < http.StatusOK || out.StatusCode >= 300 {
				fallbackRequest, err := mirror.rewriteFallbackRequest(in)
				if err != nil {
					log.Errorf("error rewriting fallback request: %s", err)
					continue
				}
				return t.inner.RoundTrip(fallbackRequest)
			}
			return out, nil // Return successful mirror response
		}
	}
	return t.inner.RoundTrip(in)
}
