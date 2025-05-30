package oci

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"runtime"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocicache"
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
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
)

var (
	registries             = flag.Slice("executor.container_registries", []Registry{}, "")
	mirrors                = flag.Slice("executor.container_registry_mirrors", []MirrorConfig{}, "")
	defaultKeychainEnabled = flag.Bool("executor.container_registry_default_keychain_enabled", false, "Enable the default container registry keychain, respecting both docker configs and podman configs.")
	allowedPrivateIPs      = flag.Slice("executor.container_registry_allowed_private_ips", []string{}, "Allowed private IP ranges for container registries. Private IPs are disallowed by default.")

	useCachePercent       = flag.Int("executor.container_registry.use_cache_percent", 0, "Percentage of image pulls to use the cache (individaul cache flags must also be enabled).")
	writeManifestsToCache = flag.Bool("executor.container_registry.write_manifests_to_cache", false, "Write resolved manifests to the cache.")

	defaultPlatform = gcr.Platform{
		Architecture: "amd64",
		OS:           "linux",
	}
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
	return &Resolver{env: env, allowedPrivateIPs: allowedPrivateIPNets}, nil
}

// AuthenticateWithRegistry makes a HEAD request to a remote registry with the input credentials.
// Any errors encountered are returned.
// Otherwise, the function returns nil and it is safe to assume the input credentials grant access
// to the image.
func (r *Resolver) AuthenticateWithRegistry(ctx context.Context, imageName string, platform *rgpb.Platform, credentials Credentials) error {
	imageRef, err := gcrname.ParseReference(imageName)
	if err != nil {
		return status.InvalidArgumentErrorf("invalid image %q", imageName)
	}
	log.CtxInfof(ctx, "Authenticating with registry %q", imageRef.Context().RegistryStr())

	remoteOpts := r.getRemoteOpts(ctx, platform, credentials)

	_, err = remote.Head(imageRef, remoteOpts...)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return status.PermissionDeniedErrorf("not authorized to access image manifest: %s", err)
		}
		return status.UnavailableErrorf("could not authorize to remote registry: %s", err)
	}
	return nil
}

func (r *Resolver) Resolve(ctx context.Context, imageName string, platform *rgpb.Platform, credentials Credentials) (gcr.Image, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	imageRef, err := gcrname.ParseReference(imageName)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image %q", imageName)
	}
	log.CtxInfof(ctx, "Resolving image in registry %q", imageRef.Context().RegistryStr())

	remoteOpts := r.getRemoteOpts(ctx, platform, credentials)

	useCache := false
	if *useCachePercent >= 100 {
		useCache = true
	} else if *useCachePercent > 0 && *useCachePercent < 100 {
		useCache = rand.Intn(100) < *useCachePercent
	}

	remoteDesc, err := remote.Get(imageRef, remoteOpts...)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("not authorized to retrieve image manifest: %s", err)
		}
		return nil, status.UnavailableErrorf("could not retrieve manifest from remote: %s", err)
	}

	// Reject Docker Schema 1 manifests.
	// Allow manifests and indexes.
	switch remoteDesc.MediaType {
	case types.DockerManifestSchema1, types.DockerManifestSchema1Signed:
		return nil, status.UnknownErrorf("unsupported MediaType %q", remoteDesc.MediaType)
	case types.OCIManifestSchema1, types.DockerManifestSchema2:
	case types.OCIImageIndex, types.DockerManifestList:
	default:
		log.CtxWarningf(ctx, "Unexpected media type %q", remoteDesc.MediaType)
	}

	if useCache {
		if *writeManifestsToCache {
			err := ocicache.WriteManifestToAC(
				ctx,
				remoteDesc.Manifest,
				r.env.GetActionCacheClient(),
				imageRef.Context(),
				remoteDesc.Descriptor.Digest,
				string(remoteDesc.MediaType),
			)
			if err != nil {
				log.CtxWarningf(ctx, "Could not write manifest to cache: %s", err)
			}
		}
		switch remoteDesc.MediaType {
		case types.OCIImageIndex, types.DockerManifestList:
			index := &indexFromRawManifest{
				repo:        imageRef.Context(),
				desc:        remoteDesc.Descriptor,
				rawManifest: remoteDesc.Manifest,
				remoteOpts:  remoteOpts,
			}
			gcrPlatform := gcr.Platform{
				Architecture: platform.GetArch(),
				OS:           platform.GetOs(),
				Variant:      platform.GetVariant(),
			}
			return index.imageByPlatform(gcrPlatform)
		default:
		}

		return &imageFromRawManifest{
			repo:        imageRef.Context(),
			desc:        remoteDesc.Descriptor,
			rawManifest: remoteDesc.Manifest,
			remoteOpts:  remoteOpts,
		}, nil
	}

	// Image() should resolve both images and image indices to an appropriate image
	img, err := remoteDesc.Image()
	if err != nil {
		switch remoteDesc.MediaType {
		// This is an "image index", a meta-manifest that contains a list of
		// {platform props, manifest hash} properties to allow client to decide
		// which manifest they want to use based on platform.
		case types.OCIImageIndex, types.DockerManifestList:
			return nil, status.UnknownErrorf("could not get image in image index from descriptor: %s", err)
		case types.OCIManifestSchema1, types.DockerManifestSchema2:
			return nil, status.UnknownErrorf("could not get image from descriptor: %s", err)
		default:
			return nil, status.UnknownErrorf("descriptor has unknown media type %q, oci error: %s", remoteDesc.MediaType, err)
		}
	}
	return img, nil
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

	tr := httpclient.NewWithAllowedPrivateIPs(r.allowedPrivateIPs).Transport
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

type indexFromRawManifest struct {
	repo        gcrname.Repository
	desc        gcr.Descriptor
	rawManifest []byte

	ctx        context.Context
	acClient   repb.ActionCacheClient
	remoteOpts []remote.Option
}

func (i *indexFromRawManifest) Digest() (gcr.Hash, error) {
	return i.desc.Digest, nil
}

func (i *indexFromRawManifest) MediaType() (types.MediaType, error) {
	return i.desc.MediaType, nil
}

func (i *indexFromRawManifest) Size() (int64, error) {
	return i.desc.Size, nil
}

func (i *indexFromRawManifest) RawManifest() ([]byte, error) {
	return i.rawManifest, nil
}

func (i *indexFromRawManifest) IndexManifest() (*gcr.IndexManifest, error) {
	return gcr.ParseIndexManifest(bytes.NewReader(i.rawManifest))
}

func (i *indexFromRawManifest) Image(h gcr.Hash) (gcr.Image, error) {
	desc, err := i.childByHash(h)
	if err != nil {
		return nil, err
	}

	switch desc.MediaType {
	case types.DockerManifestSchema1, types.DockerManifestSchema1Signed:
		return nil, status.UnknownErrorf("unsupported MediaType %q", desc.MediaType)
	case types.OCIImageIndex, types.DockerManifestList:
		return nil, status.UnknownErrorf("fetching image manifest from index, but encountered an index manifest")
	case types.OCIManifestSchema1, types.DockerManifestSchema2:
	default:
		log.Warningf("Unexpected media type in index manifest: %q", desc.MediaType)
	}

	ref := i.repo.Digest(h.String())
	remoteDesc, err := remote.Get(ref, i.remoteOpts...)
	if err != nil {
		return nil, err
	}

	if *writeManifestsToCache {
		err := ocicache.WriteManifestToAC(
			i.ctx,
			remoteDesc.Manifest,
			i.acClient,
			i.repo,
			h,
			string(remoteDesc.Descriptor.MediaType),
		)
		if err != nil {
			log.CtxWarningf(i.ctx, "Could not write manifest to cache: %s", err)
		}
	}

	image := &imageFromRawManifest{
		repo:        i.repo,
		desc:        *desc,
		rawManifest: remoteDesc.Manifest,
		remoteOpts:  i.remoteOpts,
	}

	return image, nil
}

func (i *indexFromRawManifest) ImageIndex(h gcr.Hash) (gcr.ImageIndex, error) {
	desc, err := i.childByHash(h)
	if err != nil {
		return nil, err
	}

	switch desc.MediaType {
	case types.DockerManifestSchema1, types.DockerManifestSchema1Signed:
		return nil, status.UnknownErrorf("unsupported MediaType %q", desc.MediaType)
	case types.OCIManifestSchema1, types.DockerManifestSchema2:
		return nil, status.UnknownErrorf("fetching index manifest but found image")
	case types.OCIImageIndex, types.DockerManifestList:
	default:
		log.Warningf("Unexpected media type in index manifest: %q", desc.MediaType)
	}

	ref := i.repo.Digest(h.String())
	remoteDesc, err := remote.Get(ref, i.remoteOpts...)
	if err != nil {
		return nil, err
	}

	if *writeManifestsToCache {
		err := ocicache.WriteManifestToAC(
			i.ctx,
			remoteDesc.Manifest,
			i.acClient,
			i.repo,
			h,
			string(remoteDesc.Descriptor.MediaType),
		)
		if err != nil {
			log.CtxWarningf(i.ctx, "Could not write manifest to cache: %s", err)
		}
	}

	index := &indexFromRawManifest{
		repo:        i.repo,
		desc:        *desc,
		rawManifest: remoteDesc.Manifest,
		remoteOpts:  i.remoteOpts,
	}

	return index, nil

}

func (i *indexFromRawManifest) imageByPlatform(platform gcr.Platform) (gcr.Image, error) {
	desc, err := i.childByPlatform(platform)
	if err != nil {
		return nil, err
	}
	return i.Image(desc.Digest)
}

func (i *indexFromRawManifest) childByPlatform(platform gcr.Platform) (*gcr.Descriptor, error) {
	index, err := i.IndexManifest()
	if err != nil {
		return nil, err
	}
	for _, childDesc := range index.Manifests {
		// If platform is missing from child descriptor, assume it's amd64/linux.
		p := defaultPlatform
		if childDesc.Platform != nil {
			p = *childDesc.Platform
		}

		if matchesPlatform(p, platform) {
			return &childDesc, nil
		}
	}
	return nil, status.NotFoundErrorf("no child with platform %+v in index", platform)
}

func matchesPlatform(given, required gcr.Platform) bool {
	// Required fields that must be identical.
	if given.Architecture != required.Architecture || given.OS != required.OS {
		return false
	}

	// Optional fields that may be empty, but must be identical if provided.
	if required.OSVersion != "" && given.OSVersion != required.OSVersion {
		return false
	}
	if required.Variant != "" && given.Variant != required.Variant {
		return false
	}

	// Verify required platform's features are a subset of given platform's features.
	if !isSubset(given.OSFeatures, required.OSFeatures) {
		return false
	}
	if !isSubset(given.Features, required.Features) {
		return false
	}

	return true
}

// isSubset checks if the required array of strings is a subset of the given lst.
func isSubset(lst, required []string) bool {
	set := make(map[string]bool)
	for _, value := range lst {
		set[value] = true
	}

	for _, value := range required {
		if _, ok := set[value]; !ok {
			return false
		}
	}

	return true
}

func (i *indexFromRawManifest) childByHash(h gcr.Hash) (*gcr.Descriptor, error) {
	index, err := i.IndexManifest()
	if err != nil {
		return nil, err
	}
	for _, childDesc := range index.Manifests {
		if h == childDesc.Digest {
			return &childDesc, nil
		}
	}
	return nil, status.NotFoundError("could not find child hash in index manifest")
}

type imageFromRawManifest struct {
	repo        gcrname.Repository
	desc        gcr.Descriptor
	rawManifest []byte

	remoteOpts    []remote.Option
	rawConfigFile []byte
}

func (i *imageFromRawManifest) Digest() (gcr.Hash, error) {
	return i.desc.Digest, nil
}

func (i *imageFromRawManifest) RawManifest() ([]byte, error) {
	return i.rawManifest, nil
}

func (i *imageFromRawManifest) Manifest() (*gcr.Manifest, error) {
	rawManifest, err := i.RawManifest()
	if err != nil {
		return nil, err
	}
	manifest, err := gcr.ParseManifest(bytes.NewReader(rawManifest))
	if err != nil {
		return nil, err
	}
	return manifest, nil
}

func (i *imageFromRawManifest) MediaType() (types.MediaType, error) {
	return i.desc.MediaType, nil
}

func (i *imageFromRawManifest) Size() (int64, error) {
	return i.desc.Size, nil
}

func (i *imageFromRawManifest) RawConfigFile() ([]byte, error) {
	if i.rawConfigFile != nil {
		return i.rawConfigFile, nil
	}

	manifest, err := i.Manifest()
	if err != nil {
		return nil, err
	}
	if manifest.Config.Data != nil {
		return manifest.Config.Data, nil
	}
	layer := layerFromDigest{
		digest:     manifest.Config.Digest,
		repo:       i.repo,
		image:      i,
		remoteOpts: i.remoteOpts,
	}

	rc, err := layer.Uncompressed()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	rawConfigFile, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	i.rawConfigFile = rawConfigFile
	return i.rawConfigFile, nil
}

func (i *imageFromRawManifest) ConfigFile() (*gcr.ConfigFile, error) {
	rawConfigFile, err := i.RawConfigFile()
	if err != nil {
		return nil, err
	}
	return gcr.ParseConfigFile(bytes.NewReader(rawConfigFile))
}

func (i *imageFromRawManifest) ConfigName() (gcr.Hash, error) {
	manifest, err := i.Manifest()
	if err != nil {
		return gcr.Hash{}, err
	}
	return manifest.Config.Digest, nil
}

func (i *imageFromRawManifest) Layers() ([]gcr.Layer, error) {
	m, err := i.Manifest()
	if err != nil {
		return nil, err
	}
	layers := make([]gcr.Layer, 0, len(m.Layers))
	for _, layerDesc := range m.Layers {
		layer := &layerFromDigest{
			digest:     layerDesc.Digest,
			repo:       i.repo,
			image:      i,
			remoteOpts: i.remoteOpts,
		}

		layers = append(layers, layer)
	}
	return layers, nil
}

func (i *imageFromRawManifest) LayerByDigest(digest gcr.Hash) (gcr.Layer, error) {
	return &layerFromDigest{
		digest:     digest,
		repo:       i.repo,
		image:      i,
		remoteOpts: i.remoteOpts,
	}, nil
}

func (i *imageFromRawManifest) LayerByDiffID(diffID gcr.Hash) (gcr.Layer, error) {
	digest, err := partial.DiffIDToBlob(i, diffID)
	if err != nil {
		return nil, err
	}
	return &layerFromDigest{
		digest:     digest,
		repo:       i.repo,
		image:      i,
		remoteOpts: i.remoteOpts,
	}, nil
}

type layerFromDigest struct {
	digest gcr.Hash
	repo   gcrname.Repository
	image  gcr.Image

	remoteOpts  []remote.Option
	remoteLayer gcr.Layer
}

func (l *layerFromDigest) Digest() (gcr.Hash, error) {
	return l.digest, nil
}

func (l *layerFromDigest) DiffID() (gcr.Hash, error) {
	return partial.BlobToDiffID(l.image, l.digest)
}

func (l *layerFromDigest) Compressed() (io.ReadCloser, error) {
	if l.remoteLayer != nil {
		return l.remoteLayer.Compressed()
	}
	ref := l.repo.Digest(l.digest.String())
	remoteLayer, err := remote.Layer(ref, l.remoteOpts...)
	if err != nil {
		return nil, err
	}
	l.remoteLayer = remoteLayer
	return l.remoteLayer.Compressed()
}

func (l *layerFromDigest) Uncompressed() (io.ReadCloser, error) {
	if l.remoteLayer != nil {
		return l.remoteLayer.Uncompressed()
	}
	ref := l.repo.Digest(l.digest.String())
	remoteLayer, err := remote.Layer(ref, l.remoteOpts...)
	if err != nil {
		return nil, err
	}
	l.remoteLayer = remoteLayer
	return l.remoteLayer.Uncompressed()
}

func (l *layerFromDigest) Size() (int64, error) {
	ref := l.repo.Digest(l.digest.String())
	desc, err := remote.Head(ref, l.remoteOpts...)
	if err != nil {
		return 0, err
	}
	return desc.Size, nil
}

func (l *layerFromDigest) MediaType() (types.MediaType, error) {
	ref := l.repo.Digest(l.digest.String())
	desc, err := remote.Head(ref, l.remoteOpts...)
	if err != nil {
		return "", err
	}
	return desc.MediaType, nil
}
