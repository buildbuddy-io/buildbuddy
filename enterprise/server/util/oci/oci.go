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
	"time"

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
	"github.com/docker/distribution/reference"
	"github.com/google/go-containerregistry/pkg/authn"
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

var (
	registries             = flag.Slice("executor.container_registries", []Registry{}, "")
	mirrors                = flag.Slice("executor.container_registry_mirrors", []MirrorConfig{}, "")
	defaultKeychainEnabled = flag.Bool("executor.container_registry_default_keychain_enabled", false, "Enable the default container registry keychain, respecting both docker configs and podman configs.")
	allowedPrivateIPs      = flag.Slice("executor.container_registry_allowed_private_ips", []string{}, "Allowed private IP ranges for container registries. Private IPs are disallowed by default.")
	cacheSalt              = flag.String("executor.container_registry_cache_salt", "", "Secret salt for caching OCI image manifests")

	layerOutputFilePath         = "_bb_oci_layer_"
	layerMetadataOutputFilePath = "_bb_oci_layer_metadata_"
	actionResultInstanceName    = "_bb_oci_"
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

func (r *Resolver) Resolve(ctx context.Context, imageName string, platform *rgpb.Platform, credentials Credentials) (gcr.Image, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()
	imageRef, err := gcrname.ParseReference(imageName)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image %q", imageName)
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

	tr := httpclient.NewWithAllowedPrivateIPs(60*time.Minute, r.allowedPrivateIPs).Transport
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
