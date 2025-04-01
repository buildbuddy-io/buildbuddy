package oci

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
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
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/partial"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/google/go-containerregistry/pkg/v1/types"

	ocipb "github.com/buildbuddy-io/buildbuddy/proto/ociregistry"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	ctrname "github.com/google/go-containerregistry/pkg/name"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	blobOutputFilePath         = "_bb_ociregistry_blob_"
	blobMetadataOutputFilePath = "_bb_ociregistry_blob_metadata_"
	actionResultInstanceName   = "_bb_ociregistry_"
)

var (
	registries             = flag.Slice("executor.container_registries", []Registry{}, "")
	mirrors                = flag.Slice("executor.container_registry_mirrors", []MirrorConfig{}, "")
	defaultKeychainEnabled = flag.Bool("executor.container_registry_default_keychain_enabled", false, "Enable the default container registry keychain, respecting both docker configs and podman configs.")
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

func Resolve(ctx context.Context, imageName string, platform *rgpb.Platform, credentials Credentials) (v1.Image, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()
	imageRef, err := ctrname.ParseReference(imageName)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image %q", imageName)
	}

	remoteOpts := []remote.Option{
		remote.WithContext(ctx),
		remote.WithPlatform(
			v1.Platform{
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

	tr := httpclient.New().Transport
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
	remoteImg, err := remoteDesc.Image()
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
	img := &cacheAwareImage{remoteImg: remoteImg}
	return partial.CompressedToImage(img)
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

func FetchManifestFromCache(ctx context.Context, acClient repb.ActionCacheClient, bsClient bspb.ByteStreamClient, ref ctrname.Reference) ([]byte, error) {
	hash, err := v1.NewHash(ref.Identifier())
	if err != nil {
		return nil, err
	}
	arKey := &ocipb.OCIActionResultKey{
		Registry:      ref.Context().RegistryStr(),
		Repository:    ref.Context().RepositoryStr(),
		ResourceType:  ocipb.OCIResourceType_MANIFEST,
		HashAlgorithm: hash.Algorithm,
		HashHex:       hash.Hex,
	}
	arKeyBytes, err := proto.Marshal(arKey)
	if err != nil {
		return nil, err
	}
	arDigest, err := digest.Compute(bytes.NewReader(arKeyBytes), repb.DigestFunction_SHA256)
	if err != nil {
		return nil, err
	}
	arRN := digest.NewACResourceName(
		arDigest,
		actionResultInstanceName,
		repb.DigestFunction_SHA256,
	)
	ar, err := cachetools.GetActionResult(ctx, acClient, arRN)
	if err != nil {
		return nil, err
	}

	var blobMetadataCASDigest *repb.Digest
	var blobCASDigest *repb.Digest
	for _, outputFile := range ar.GetOutputFiles() {
		switch outputFile.GetPath() {
		case blobMetadataOutputFilePath:
			blobMetadataCASDigest = outputFile.GetDigest()
		case blobOutputFilePath:
			blobCASDigest = outputFile.GetDigest()
		default:
			log.CtxErrorf(ctx, "unknown output file path '%s' in ActionResult for %s", outputFile.GetPath(), ref)
		}
	}
	if blobMetadataCASDigest == nil || blobCASDigest == nil {
		return nil, fmt.Errorf("missing blob metadata digest or blob digest for %s", ref)
	}
	blobMetadataRN := digest.NewCASResourceName(
		blobMetadataCASDigest,
		"",
		repb.DigestFunction_SHA256,
	)
	blobMetadata := &ocipb.OCIBlobMetadata{}
	err = cachetools.GetBlobAsProto(ctx, bsClient, blobMetadataRN, blobMetadata)
	if err != nil {
		return nil, err
	}

	blobRN := digest.NewCASResourceName(
		blobCASDigest,
		"",
		repb.DigestFunction_SHA256,
	)
	blobRN.SetCompressor(repb.Compressor_ZSTD)
	var buf bytes.Buffer
	err = cachetools.GetBlob(ctx, bsClient, blobRN, &buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func WriteManifestToCache(ctx context.Context, acClient repb.ActionCacheClient, bsClient bspb.ByteStreamClient, ref ctrname.Reference, hash v1.Hash, contentType string, contentLength int64, upstream io.Reader) error {
	blobCASDigest := &repb.Digest{
		Hash:      hash.Hex,
		SizeBytes: contentLength,
	}
	blobRN := digest.NewCASResourceName(
		blobCASDigest,
		"",
		repb.DigestFunction_SHA256,
	)
	blobRN.SetCompressor(repb.Compressor_ZSTD)
	_, _, err := cachetools.UploadFromReader(ctx, bsClient, blobRN, upstream)
	if err != nil {
		return err
	}

	blobMetadata := &ocipb.OCIBlobMetadata{
		ContentLength: contentLength,
		ContentType:   contentType,
	}
	blobMetadataCASDigest, err := cachetools.UploadProto(ctx, bsClient, "", repb.DigestFunction_SHA256, blobMetadata)
	if err != nil {
		return err
	}

	arKey := &ocipb.OCIActionResultKey{
		Registry:      ref.Context().RegistryStr(),
		Repository:    ref.Context().RepositoryStr(),
		ResourceType:  ocipb.OCIResourceType_MANIFEST,
		HashAlgorithm: hash.Algorithm,
		HashHex:       hash.Hex,
	}
	ar := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			{
				Path:   blobOutputFilePath,
				Digest: blobCASDigest,
			},
			{
				Path:   blobMetadataOutputFilePath,
				Digest: blobMetadataCASDigest,
			},
		},
	}
	arKeyBytes, err := proto.Marshal(arKey)
	if err != nil {
		return err
	}
	arDigest, err := digest.Compute(bytes.NewReader(arKeyBytes), repb.DigestFunction_SHA256)
	if err != nil {
		return err
	}
	arRN := digest.NewACResourceName(
		arDigest,
		actionResultInstanceName,
		repb.DigestFunction_SHA256,
	)
	err = cachetools.UploadActionResult(ctx, acClient, arRN, ar)
	if err != nil {
		return err
	}
	return nil
}

type cacheAwareImage struct {
	remoteImg v1.Image
}

func (i *cacheAwareImage) RawManifest() ([]byte, error) {
	return i.remoteImg.RawManifest()
}

func (i *cacheAwareImage) RawConfigFile() ([]byte, error) {
	return i.remoteImg.RawConfigFile()
}

func (i *cacheAwareImage) MediaType() (types.MediaType, error) {
	return i.remoteImg.MediaType()
}

func (i *cacheAwareImage) LayerByDigest(hash v1.Hash) (partial.CompressedLayer, error) {
	return i.remoteImg.LayerByDigest(hash)
}

var _ partial.CompressedImageCore = (*cacheAwareImage)(nil)

type cacheAwareLayer struct {
	remoteLayer v1.Layer
}

func (l *cacheAwareLayer) Digest() (v1.Hash, error) {
	return l.remoteLayer.Digest()
}

func (l *cacheAwareLayer) Compressed() (io.ReadCloser, error) {
	return l.remoteLayer.Compressed()
}

func (l *cacheAwareLayer) Size() (int64, error) {
	return l.remoteLayer.Size()
}

func (l *cacheAwareLayer) MediaType() (types.MediaType, error) {
	return l.remoteLayer.MediaType()
}

var _ partial.CompressedLayer = (*cacheAwareLayer)(nil)
