package oci

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
)

var (
	registries             = flag.Slice("executor.container_registries", []Registry{}, "")
	mirrors                = flag.Slice("executor.container_registry_mirrors", []MirrorConfig{}, "")
	defaultKeychainEnabled = flag.Bool("executor.container_registry_default_keychain_enabled", false, "Enable the default container registry keychain, respecting both docker configs and podman configs.")
	allowedPrivateIPs      = flag.Slice("executor.container_registry_allowed_private_ips", []string{}, "Allowed private IP ranges for container registries. Private IPs are disallowed by default.")

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

func (r *Resolver) Resolve(ctx context.Context, imageName string, platform *rgpb.Platform, credentials Credentials) (gcr.Image, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()
	imageRef, err := gcrname.ParseReference(imageName)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image %q", imageName)
	}

	gcrPlatform := gcr.Platform{
		Architecture: platform.GetArch(),
		OS:           platform.GetOs(),
		Variant:      platform.GetVariant(),
	}
	remoteOpts := []remote.Option{
		remote.WithContext(ctx),
		remote.WithPlatform(gcrPlatform),
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

	manifestHash, rawManifest, fromCache, err := r.fetchRawManifestFromCacheOrRemote(ctx, imageRef, remoteOpts)
	if err != nil {
		return nil, err
	}
	manifest, err := gcr.ParseManifest(bytes.NewReader(rawManifest))
	if err != nil {
		return nil, err
	}
	if !fromCache && *writeManifestsToCache {
		contentType := string(manifest.MediaType)
		err := ocicache.WriteManifestToAC(ctx, rawManifest, r.env.GetActionCacheClient(), imageRef, *manifestHash, contentType)
		if err != nil {
			log.CtxWarningf(ctx, "Could not write manifest for %q to the cache: %s", imageRef.Context(), err)
		}
	}

	if manifest.MediaType != types.OCIImageIndex && manifest.MediaType != types.DockerManifestList {
		image := &imageFromManifest{
			digest: imageRef.Context().Digest(manifestHash.String()),

			rawManifest: rawManifest,
			manifest:    manifest,

			remoteOpts: remoteOpts,
		}
		return partial.CompressedToImage(image)
	}

	index, err := gcr.ParseIndexManifest(bytes.NewReader(rawManifest))
	if err != nil {
		return nil, err
	}
	var child *gcr.Descriptor
	for _, childDesc := range index.Manifests {
		p := defaultPlatform
		if childDesc.Platform != nil {
			p = *childDesc.Platform
		}

		if matchesPlatform(p, gcrPlatform) {
			child = &childDesc
		}
	}
	if child == nil {
		return nil, status.UnavailableErrorf("no child with platform %+v for %s", platform, imageRef.Context())
	}

	childRef := imageRef.Context().Digest(child.Digest.String())
	childHash, childRawManifest, childFromCache, err := r.fetchRawManifestFromCacheOrRemote(ctx, childRef, remoteOpts)
	if err != nil {
		return nil, err
	}
	childManifest, err := gcr.ParseManifest(bytes.NewReader(childRawManifest))
	if err != nil {
		return nil, err
	}
	if !childFromCache && *writeManifestsToCache {
		childContentType := string(childManifest.MediaType)
		err := ocicache.WriteManifestToAC(ctx, childRawManifest, r.env.GetActionCacheClient(), childRef, *childHash, childContentType)
		if err != nil {
			log.CtxWarningf(ctx, "Could not write manifest for %q to the cache: %s", childRef.Context(), err)
		}
	}
	if childManifest.MediaType == types.OCIImageIndex || childManifest.MediaType == types.DockerManifestList {
		return nil, status.UnknownErrorf("child manifest for %q is itself an image index", childRef.Context())
	}

	image := &imageFromManifest{
		digest: childRef.Context().Digest(childHash.String()),

		rawManifest: childRawManifest,
		manifest:    childManifest,

		remoteOpts: remoteOpts,
	}
	return partial.CompressedToImage(image)
}

func (r *Resolver) fetchRawManifestFromCacheOrRemote(ctx context.Context, digestOrTagRef gcrname.Reference, remoteOpts []remote.Option) (*gcr.Hash, []byte, bool, error) {
	headDesc, err := remote.Head(digestOrTagRef, remoteOpts...)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, nil, false, status.PermissionDeniedErrorf("could not retrieve image manifest: %s", err)
		}
		return nil, nil, false, status.UnavailableErrorf("could not retrieve manifest from remote: %s", err)
	}

	mc, err := ocicache.FetchManifestFromAC(ctx, r.env.GetActionCacheClient(), digestOrTagRef, headDesc.Digest)
	if err == nil {
		return &headDesc.Digest, mc.Raw, true, nil
	}
	if !status.IsNotFoundError(err) {
		log.CtxErrorf(ctx, "error fetching manifest %s from the CAS: %s", digestOrTagRef.Context(), err)
	}

	manifestDigest := digestOrTagRef.Context().Digest(headDesc.Digest.String())
	getDesc, err := remote.Get(manifestDigest, remoteOpts...)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, nil, false, status.PermissionDeniedErrorf("could not retrieve image manifest: %s", err)
		}
		return nil, nil, false, status.UnavailableErrorf("could not retrieve manifest from remote: %s", err)
	}
	return &getDesc.Digest, getDesc.Manifest, false, nil
}

type imageFromManifest struct {
	digest gcrname.Digest

	rawManifest []byte
	manifest    *gcr.Manifest

	remoteOpts []remote.Option
}

func (i *imageFromManifest) RawManifest() ([]byte, error) {
	return i.rawManifest, nil
}

func (i *imageFromManifest) RawConfigFile() ([]byte, error) {
	if i.manifest.Config.Data != nil {
		return i.manifest.Config.Data, nil
	}

	configDigest := i.digest.Digest(i.manifest.Config.Digest.String())
	rl, err := remote.Layer(configDigest, i.remoteOpts...)
	if err != nil {
		return nil, err
	}
	rc, err := rl.Uncompressed()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	rawConfigFile, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return rawConfigFile, nil
}

func (i *imageFromManifest) MediaType() (types.MediaType, error) {
	return i.manifest.MediaType, nil
}

func (i *imageFromManifest) LayerByDigest(hash gcr.Hash) (partial.CompressedLayer, error) {
	for _, d := range i.manifest.Layers {
		if d.Digest == hash {
			rlref := i.digest.Context().Digest(hash.String())
			return remote.Layer(rlref, i.remoteOpts...)
		}
	}
	return nil, status.NotFoundErrorf("could not find layer in image %q", i.digest.Context())
}

var _ partial.CompressedImageCore = (*imageFromManifest)(nil)

// matchesPlatform is taken from https://github.com/google/go-containerregistry/blob/v0.17.0/pkg/v1/remote/index.go
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

// isSubset is taken from https://github.com/google/go-containerregistry/blob/v0.17.0/pkg/v1/remote/index.go
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
