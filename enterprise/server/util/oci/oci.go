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
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/distribution/reference"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/v1/partial"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"google.golang.org/protobuf/types/known/anypb"

	ocipb "github.com/buildbuddy-io/buildbuddy/proto/ociregistry"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
)

const (
	layerOutputFilePath         = "_bb_oci_layer_"
	layerMetadataOutputFilePath = "_bb_oci_layer_metadata_"
	actionResultInstanceName    = "_bb_oci_"
)

var (
	registries             = flag.Slice("executor.container_registries", []Registry{}, "")
	mirrors                = flag.Slice("executor.container_registry_mirrors", []MirrorConfig{}, "")
	defaultKeychainEnabled = flag.Bool("executor.container_registry_default_keychain_enabled", false, "Enable the default container registry keychain, respecting both docker configs and podman configs.")
	allowedPrivateIPs      = flag.Slice("executor.container_registry_allowed_private_ips", []string{}, "Allowed private IP ranges for container registries. Private IPs are disallowed by default.")
	cacheSalt              = flag.String("executor.container_registry_cache_salt", "", "Secret salt for caching OCI image manifests")
	useCachingResolve      = flag.Bool("executor.container_registry_use_cache", false, "Store image manifests and layers during image resolution")

	defaultPlatform = gcr.Platform{
		Architecture: "amd64",
		OS:           "linux",
	}
)

type Resolver struct {
	env               environment.Env
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

func (r *Resolver) hasGroupID(ctx context.Context) bool {
	_, err := r.env.GetAuthenticator().AuthenticatedUser(ctx)
	return err == nil
}

func (r *Resolver) Resolve(ctx context.Context, imgname string, platform *rgpb.Platform, credentials Credentials) (gcr.Image, error) {
	if *useCachingResolve {
		ctx, span := tracing.StartSpan(ctx)
		canUseCache := credentials.IsEmpty() || r.hasGroupID(ctx)
		defer span.End()
		gcrPlatform := gcr.Platform{
			Architecture: platform.GetArch(),
			OS:           platform.GetOs(),
			Variant:      platform.GetVariant(),
		}
		opts := r.makeRemoteOptions(ctx, gcrPlatform, credentials)
		imgref, err := gcrname.ParseReference(imgname)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("invalid image %q", imgname)
		}
		imgdigest, raw, fromCache, err := r.fetchRawManifestFromCacheOrRemote(ctx, imgref, opts)
		if err != nil {
			return nil, err
		}
		m, err := gcr.ParseManifest(bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		if !fromCache && canUseCache {
			err := r.UploadManifestToCache(
				ctx,
				*imgdigest,
				string(m.MediaType),
				int64(len(raw)),
				raw,
			)
			if err != nil {
				log.CtxErrorf(ctx, "error writing image manifest %s to the CAS: %s", imgref.Context(), err)
			}
		}

		if m.MediaType != types.OCIImageIndex && m.MediaType != types.DockerManifestList {
			img := &cacheAwareImage{
				digest:      *imgdigest,
				canUseCache: canUseCache,
				r:           r,
				ctx:         ctx,
				raw:         raw,
				manifest:    m,
				options:     opts,
			}
			return partial.CompressedToImage(img)
		}

		index, err := gcr.ParseIndexManifest(bytes.NewReader(raw))
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
			return nil, fmt.Errorf("no child with platform %+v for %s", platform, imgref.Context())
		}
		childRef := imgref.Context().Digest(child.Digest.String())
		childDigest, childraw, childFromCache, err := r.fetchRawManifestFromCacheOrRemote(ctx, childRef, opts)
		if err != nil {
			return nil, err
		}
		childm, err := gcr.ParseManifest(bytes.NewReader(childraw))
		if err != nil {
			return nil, err
		}
		if !childFromCache && canUseCache {
			err := r.UploadManifestToCache(
				ctx,
				*childDigest,
				string(childm.MediaType),
				int64(len(childraw)),
				childraw,
			)
			if err != nil {
				log.CtxErrorf(ctx, "error writing image %s to the CAS: %s", childDigest.Context(), err)
			}
		}
		if childm.MediaType == types.OCIImageIndex || childm.MediaType == types.DockerManifestList {
			return nil, fmt.Errorf("child manifest %s is itself an image index", childDigest.Context())
		}
		childimg := &cacheAwareImage{
			digest:      *childDigest,
			canUseCache: canUseCache,
			r:           r,
			ctx:         ctx,
			raw:         childraw,
			manifest:    childm,
			options:     opts,
		}
		return partial.CompressedToImage(childimg)
	}

	imageRef, err := gcrname.ParseReference(imgname)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image %q", imgname)
	}

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
	remoteDesc, err := remote.Get(imageRef, remoteOpts...)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("could not retrieve image manifest: %s", err)
		}
		return nil, status.UnavailableErrorf("could not retrieve manifest from remote: %s", err)
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

func (r *Resolver) makeRemoteOptions(ctx context.Context, platform gcr.Platform, credentials Credentials) []remote.Option {
	remoteOpts := []remote.Option{
		remote.WithContext(ctx),
		remote.WithPlatform(platform),
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

// RuntimePlatform returns the platform on which the program is being executed,
// as reported by the go runtime.
func RuntimePlatform() *rgpb.Platform {
	return &rgpb.Platform{
		Arch: runtime.GOARCH,
		Os:   runtime.GOOS,
	}
}

func (r *Resolver) fetchRawManifestFromCacheOrRemote(ctx context.Context, digestOrTagRef gcrname.Reference, remoteOpts []remote.Option) (*gcrname.Digest, []byte, bool, error) {
	desc, err := remote.Head(digestOrTagRef, remoteOpts...)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, nil, false, status.PermissionDeniedErrorf("could not retrieve image manifest: %s", err)
		}
		return nil, nil, false, status.UnavailableErrorf("could not retrieve manifest from remote: %s", err)
	}

	ocidigest := digestOrTagRef.Context().Digest(desc.Digest.String())
	raw, err := r.FetchManifestFromCache(ctx, ocidigest)
	if err == nil {
		return &ocidigest, raw, true, nil
	}
	if !status.IsNotFoundError(err) {
		log.CtxErrorf(ctx, "error fetching manifest %s from the CAS: %s", ocidigest.Context(), err)
	}

	remoteDesc, err := remote.Get(ocidigest, remoteOpts...)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, nil, false, status.PermissionDeniedErrorf("could not retrieve image manifest: %s", err)
		}
		return nil, nil, false, status.UnavailableErrorf("could not retrieve manifest from remote: %s", err)
	}
	return &ocidigest, remoteDesc.Manifest, false, nil
}

func (r *Resolver) fetchManifestContentFromCache(ctx context.Context, ocidigest gcrname.Digest) (*ocipb.OCIManifestContent, error) {
	arRN, err := ocidigestToACResourceName(ocidigest, ocipb.OCIResourceType_MANIFEST)
	if err != nil {
		return nil, err
	}
	ar, err := cachetools.GetActionResult(ctx, r.env.GetActionCacheClient(), arRN)
	if err != nil {
		return nil, err
	}
	meta := ar.GetExecutionMetadata()
	if meta == nil {
		return nil, fmt.Errorf("missing metadata for manifest in %s", ocidigest.Context())
	}
	aux := meta.GetAuxiliaryMetadata()
	if aux == nil || len(aux) != 1 {
		return nil, fmt.Errorf("missing metadata for manifest in %s", ocidigest.Context())
	}
	any := aux[0]
	var mc ocipb.OCIManifestContent
	err = any.UnmarshalTo(&mc)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal metadata for manifest in %s", ocidigest.Context())
	}
	return &mc, nil
}

func (r *Resolver) FetchManifestMetadataFromCache(ctx context.Context, ocidigest gcrname.Digest) (string, int64, error) {
	mc, err := r.fetchManifestContentFromCache(ctx, ocidigest)
	if err != nil {
		return "", 0, err
	}
	return mc.GetContentType(), mc.GetContentLength(), nil
}

func (r *Resolver) FetchManifestFromCache(ctx context.Context, ocidigest gcrname.Digest) ([]byte, error) {
	mc, err := r.fetchManifestContentFromCache(ctx, ocidigest)
	if err != nil {
		return nil, err
	}
	return mc.GetRaw(), nil
}

func (r *Resolver) FetchLayerMetadataFromCache(ctx context.Context, ocidigest gcrname.Digest) (string, int64, error) {
	arRN, err := ocidigestToACResourceName(ocidigest, ocipb.OCIResourceType_BLOB)
	if err != nil {
		return "", 0, err
	}
	ar, err := cachetools.GetActionResult(ctx, r.env.GetActionCacheClient(), arRN)
	if err != nil {
		return "", 0, err
	}

	var blobMetadataCASDigest *repb.Digest
	var blobCASDigest *repb.Digest
	for _, outputFile := range ar.GetOutputFiles() {
		switch outputFile.GetPath() {
		case layerMetadataOutputFilePath:
			blobMetadataCASDigest = outputFile.GetDigest()
		case layerOutputFilePath:
			blobCASDigest = outputFile.GetDigest()
		default:
			log.CtxErrorf(ctx, "unknown output file path '%s' in ActionResult for %s", outputFile.GetPath(), ocidigest)
		}
	}
	if blobMetadataCASDigest == nil || blobCASDigest == nil {
		return "", 0, fmt.Errorf("missing blob metadata digest or blob digest for %s", ocidigest)
	}
	blobMetadataRN := digest.NewCASResourceName(
		blobMetadataCASDigest,
		"",
		repb.DigestFunction_SHA256,
	)
	blobMetadata := &ocipb.OCIBlobMetadata{}
	err = cachetools.GetBlobAsProto(ctx, r.env.GetByteStreamClient(), blobMetadataRN, blobMetadata)
	if err != nil {
		return "", 0, err
	}
	return blobMetadata.GetContentType(), blobMetadata.GetContentLength(), nil
}

func (r *Resolver) FetchLayerFromCache(ctx context.Context, ocidigest gcrname.Digest, contentLength int64, w io.Writer) error {
	blobRN, err := ocidigestToCASResourceName(ocidigest, contentLength)
	if err != nil {
		return err
	}
	return cachetools.GetBlob(ctx, r.env.GetByteStreamClient(), blobRN, w)
}

func ocidigestToACResourceName(ocidigest gcrname.Digest, ociResourceType ocipb.OCIResourceType) (*digest.ACResourceName, error) {
	hash, err := gcr.NewHash(ocidigest.DigestStr())
	if err != nil {
		return nil, fmt.Errorf("could not parse hash in %s: %s", ocidigest.Context(), err)
	}
	arKey := &ocipb.OCIActionResultKey{
		Registry:      ocidigest.Context().RegistryStr(),
		Repository:    ocidigest.Context().RepositoryStr(),
		ResourceType:  ociResourceType,
		HashAlgorithm: hash.Algorithm,
		HashHex:       hash.Hex,
	}
	arKeyBytes, err := proto.Marshal(arKey)
	if err != nil {
		return nil, err
	}
	salted := append([]byte(*cacheSalt), arKeyBytes...)
	arDigest, err := digest.Compute(bytes.NewReader(salted), repb.DigestFunction_SHA256)
	if err != nil {
		return nil, err
	}
	arRN := digest.NewACResourceName(
		arDigest,
		actionResultInstanceName,
		repb.DigestFunction_SHA256,
	)
	return arRN, nil
}

func (r *Resolver) UploadManifestToCache(ctx context.Context, ocidigest gcrname.Digest, contentType string, contentLength int64, raw []byte) error {
	arRN, err := ocidigestToACResourceName(ocidigest, ocipb.OCIResourceType_MANIFEST)
	if err != nil {
		return err
	}
	m := &ocipb.OCIManifestContent{
		Raw:           raw,
		ContentLength: contentLength,
		ContentType:   contentType,
	}
	any, err := anypb.New(m)
	if err != nil {
		return err
	}
	ar := &repb.ActionResult{
		ExecutionMetadata: &repb.ExecutedActionMetadata{
			AuxiliaryMetadata: []*anypb.Any{
				any,
			},
		},
	}
	return cachetools.UploadActionResult(
		ctx,
		r.env.GetActionCacheClient(),
		arRN,
		ar,
	)
}

func (r *Resolver) uploadLayerMetadataToCache(ctx context.Context, ocidigest gcrname.Digest, contentType string, contentLength int64) error {
	blobRN, err := ocidigestToCASResourceName(ocidigest, contentLength)
	if err != nil {
		return err
	}
	blobMetadata := &ocipb.OCIBlobMetadata{
		ContentLength: contentLength,
		ContentType:   contentType,
	}
	blobMetadataCASDigest, err := cachetools.UploadProto(
		ctx,
		r.env.GetByteStreamClient(),
		"",
		repb.DigestFunction_SHA256,
		blobMetadata,
	)
	if err != nil {
		return err
	}

	arRN, err := ocidigestToACResourceName(ocidigest, ocipb.OCIResourceType_BLOB)
	if err != nil {
		return err
	}
	ar := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			{
				Path:   layerOutputFilePath,
				Digest: blobRN.GetDigest(),
			},
			{
				Path:   layerMetadataOutputFilePath,
				Digest: blobMetadataCASDigest,
			},
		},
	}
	return cachetools.UploadActionResult(
		ctx,
		r.env.GetActionCacheClient(),
		arRN,
		ar,
	)
}

func (r *Resolver) UploadLayerToCache(ctx context.Context, ocidigest gcrname.Digest, contentType string, contentLength int64, ior io.Reader) error {
	blobRN, err := ocidigestToCASResourceName(ocidigest, contentLength)
	if err != nil {
		return err
	}
	_, _, err = cachetools.UploadFromReader(
		ctx,
		r.env.GetByteStreamClient(),
		blobRN,
		ior,
	)
	if err != nil {
		return err
	}
	return r.uploadLayerMetadataToCache(ctx, ocidigest, contentType, contentLength)
}

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

type cachingLayerWriteCloser struct {
	ctx context.Context
	r   *Resolver

	ocidigest     gcrname.Digest
	contentType   string
	contentLength int64

	wc io.WriteCloser
}

func (clwc *cachingLayerWriteCloser) Write(p []byte) (int, error) {
	return clwc.wc.Write(p)
}

func (clwc *cachingLayerWriteCloser) Close() error {
	err := clwc.wc.Close()
	if err != nil {
		return err
	}

	return clwc.r.uploadLayerMetadataToCache(
		clwc.ctx,
		clwc.ocidigest,
		clwc.contentType,
		clwc.contentLength,
	)
}

func ocidigestToCASResourceName(ocidigest gcrname.Digest, contentLength int64) (*digest.CASResourceName, error) {
	hash, err := gcr.NewHash(ocidigest.DigestStr())
	if err != nil {
		return nil, fmt.Errorf("could not parse hash for layer in %s: %s", ocidigest.Context(), err)
	}
	var df repb.DigestFunction_Value
	switch hash.Algorithm {
	case "sha256":
		df = repb.DigestFunction_SHA256
	case "sha512":
		df = repb.DigestFunction_SHA512
	default:
		return nil, fmt.Errorf("unsupported hashing algorithm %s in %s", hash.Algorithm, ocidigest.Context())
	}
	casdigest := &repb.Digest{
		Hash:      hash.Hex,
		SizeBytes: contentLength,
	}
	rn := digest.NewCASResourceName(
		casdigest,
		"",
		df,
	)
	rn.SetCompressor(repb.Compressor_ZSTD)
	return rn, nil
}

func newCachingLayerWriteCloser(ctx context.Context, r *Resolver, ocidigest gcrname.Digest, contentType string, contentLength int64) (io.WriteCloser, error) {
	blobRN, err := ocidigestToCASResourceName(ocidigest, contentLength)
	if err != nil {
		return nil, err
	}
	wc, err := cachetools.NewUploadWriteCloser(ctx, r.env.GetByteStreamClient(), blobRN)
	if err != nil {
		return nil, err
	}
	return &cachingLayerWriteCloser{
		ctx:           ctx,
		r:             r,
		ocidigest:     ocidigest,
		contentType:   contentType,
		contentLength: contentLength,
		wc:            wc,
	}, nil
}

type cacheAwareImage struct {
	digest      gcrname.Digest
	r           *Resolver
	ctx         context.Context
	canUseCache bool

	raw      []byte
	manifest *gcr.Manifest
	options  []remote.Option
}

func (i *cacheAwareImage) RawManifest() ([]byte, error) {
	return i.raw, nil
}

func (i *cacheAwareImage) RawConfigFile() ([]byte, error) {
	if i.manifest.Config.Data != nil {
		return i.manifest.Config.Data, nil
	}

	_, contentLength, err := i.r.FetchLayerMetadataFromCache(
		i.ctx,
		i.digest,
	)
	if err == nil {
		raw := &bytes.Buffer{}
		err := i.r.FetchLayerFromCache(i.ctx, i.digest, contentLength, raw)
		if err == nil {
			return raw.Bytes(), nil
		}
		if !status.IsNotFoundError(err) {
			log.CtxErrorf(i.ctx, "error fetching config in %s from the CAS: %s", i.digest.Context(), err)
		}
	}

	ocidigest := i.digest.Digest(i.manifest.Config.Digest.String())
	rl, err := remote.Layer(ocidigest, i.options...)
	if err != nil {
		return nil, err
	}
	rc, err := rl.Uncompressed()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	remoteraw, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	if !i.canUseCache {
		return remoteraw, nil
	}
	err = i.r.UploadLayerToCache(
		i.ctx,
		ocidigest,
		string(i.manifest.Config.MediaType),
		int64(len(remoteraw)),
		bytes.NewReader(remoteraw),
	)
	if err != nil {
		log.CtxErrorf(i.ctx, "could not write config %s to cache", ocidigest)
	}
	return remoteraw, nil
}

func (i *cacheAwareImage) MediaType() (types.MediaType, error) {
	return i.manifest.MediaType, nil
}

func (i *cacheAwareImage) LayerByDigest(hash gcr.Hash) (partial.CompressedLayer, error) {
	for _, d := range i.manifest.Layers {
		if d.Digest == hash {
			rlref := i.digest.Context().Digest(hash.String())
			rl, err := remote.Layer(rlref, i.options...)
			if err != nil {
				return nil, fmt.Errorf("could not create remote layer in %s", rlref.Context())
			}
			l := &cacheAwareLayer{
				ocidigest:   i.digest.Context().Digest(hash.String()),
				r:           i.r,
				canUseCache: i.canUseCache,
				ctx:         i.ctx,
				descriptor:  &d,
				remoteLayer: rl,
			}
			return l, nil
		}
	}
	return nil, fmt.Errorf("could not find layer %s", hash)
}

var _ partial.CompressedImageCore = (*cacheAwareImage)(nil)

type cacheAwareLayer struct {
	r           *Resolver
	ctx         context.Context
	canUseCache bool

	ocidigest   gcrname.Digest
	descriptor  *gcr.Descriptor
	remoteLayer gcr.Layer
}

func (l *cacheAwareLayer) Digest() (gcr.Hash, error) {
	if l.descriptor != nil {
		return l.descriptor.Digest, nil
	}
	return l.remoteLayer.Digest()
}

func newTeeReadCloser(rc io.ReadCloser, wc io.WriteCloser) io.ReadCloser {
	return &teeReadCloser{rc, wc, true}
}

type teeReadCloser struct {
	rc            io.ReadCloser
	wc            io.WriteCloser
	attemptWrites bool
}

func (t *teeReadCloser) Read(p []byte) (int, error) {
	n, err := t.rc.Read(p)
	if n > 0 {
		_, werr := t.wc.Write(p[:n])
		if werr != nil {
			log.Warningf("teeReadCloser write error, swallowing: %s", werr)
			t.attemptWrites = false
		}
	}
	return n, err
}

func (t *teeReadCloser) Close() error {
	t.attemptWrites = false
	werr := t.wc.Close()
	rerr := t.rc.Close()
	if rerr != nil {
		return rerr
	}
	if werr != nil {
		return werr
	}
	return nil
}

func (l *cacheAwareLayer) Compressed() (io.ReadCloser, error) {
	_, contentLength, err := l.r.FetchLayerMetadataFromCache(l.ctx, l.ocidigest)
	if err != nil && !status.IsNotFoundError(err) {
		log.CtxErrorf(l.ctx, "error fetching CAS digest for layer in %s: %s", l.ocidigest.Context(), err)
	}
	if err == nil {
		r, w := io.Pipe()
		go func() {
			defer w.Close()
			err := l.r.FetchLayerFromCache(
				l.ctx,
				l.ocidigest,
				contentLength,
				w,
			)
			if err != nil {
				log.Warningf("error fetching layer from CAS in %s: %s", l.ocidigest.Context(), err)
				w.CloseWithError(err)
			}
		}()
		return r, nil
	}

	mediaType, err := l.remoteLayer.MediaType()
	if err != nil {
		return nil, err
	}
	size, err := l.remoteLayer.Size()
	if err != nil {
		return nil, err
	}
	uprc, err := l.remoteLayer.Compressed()
	if err != nil {
		return nil, err
	}
	if !l.canUseCache {
		return uprc, nil
	}
	caswc, err := newCachingLayerWriteCloser(
		l.ctx,
		l.r,
		l.ocidigest,
		string(mediaType),
		size,
	)
	if err != nil {
		// cannot cache, but we can still fetch the remote layer
		log.CtxErrorf(l.ctx, "cannot cache OCI image layer in %s: %s", l.ocidigest.Context(), err)
		return uprc, nil
	}
	return newTeeReadCloser(uprc, caswc), nil
}

func (l *cacheAwareLayer) Size() (int64, error) {
	if l.descriptor != nil {
		return l.descriptor.Size, nil
	}
	return l.remoteLayer.Size()
}

func (l *cacheAwareLayer) MediaType() (types.MediaType, error) {
	if l.descriptor != nil {
		return l.descriptor.MediaType, nil
	}
	return l.remoteLayer.MediaType()
}

var _ partial.CompressedLayer = (*cacheAwareLayer)(nil)
