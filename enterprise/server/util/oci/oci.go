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

	actionResultInstanceNameSalt = "_bb_oci_salt_" // STOPSHIP(dan): make this a secret
)

var (
	registries             = flag.Slice("executor.container_registries", []Registry{}, "")
	mirrors                = flag.Slice("executor.container_registry_mirrors", []MirrorConfig{}, "")
	defaultKeychainEnabled = flag.Bool("executor.container_registry_default_keychain_enabled", false, "Enable the default container registry keychain, respecting both docker configs and podman configs.")

	defaultPlatform = v1.Platform{
		Architecture: "amd64",
		OS:           "linux",
	}
)

func Resolve(ctx context.Context, acc repb.ActionCacheClient, bsc bspb.ByteStreamClient, imageName string, platform *rgpb.Platform, credentials Credentials) (v1.Image, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()
	imageRef, err := ctrname.ParseReference(imageName)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image %q", imageName)
	}
	gcrPlatform := v1.Platform{
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

	tr := httpclient.New().Transport
	if len(*mirrors) > 0 {
		remoteOpts = append(remoteOpts, remote.WithTransport(newMirrorTransport(tr, *mirrors)))
	} else {
		remoteOpts = append(remoteOpts, remote.WithTransport(tr))
	}

	cref, raw, fromCache, err := fetchRawManifestFromCacheOrRemote(ctx, acc, bsc, imageRef, remoteOpts)
	if err != nil {
		return nil, err
	}
	digest := imageRef.Context().Digest(cref.hash.String())
	m, err := v1.ParseManifest(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	if !fromCache {
		err := writeManifestToCache(
			ctx,
			acc,
			bsc,
			digest,
			string(m.MediaType),
			int64(len(raw)),
			raw,
		)
		if err != nil {
			log.CtxErrorf(ctx, "error writing image %s to the CAS: %s", imageRef, err)
		}
	}

	if m.MediaType != types.OCIImageIndex && m.MediaType != types.DockerManifestList {
		img := &cacheAwareImage{
			ref:      *cref,
			digest:   imageRef.Context().Digest(cref.hash.String()),
			acc:      acc,
			bsc:      bsc,
			raw:      raw,
			manifest: m,
			options:  remoteOpts,
		}
		return partial.CompressedToImage(img)
	}

	index, err := v1.ParseIndexManifest(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	var child *v1.Descriptor
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
		return nil, fmt.Errorf("no child with platform %+v in index %s/%s@%s", platform, cref.registry, cref.repository, cref.hash)
	}
	childRef := imageRef.Context().Digest(child.Digest.String())
	childcref, childraw, childFromCache, err := fetchRawManifestFromCacheOrRemote(ctx, acc, bsc, childRef, remoteOpts)
	if err != nil {
		return nil, err
	}
	childm, err := v1.ParseManifest(bytes.NewReader(childraw))
	if err != nil {
		return nil, err
	}
	if !childFromCache {
		err := writeManifestToCache(
			ctx,
			acc,
			bsc,
			childRef,
			string(childm.MediaType),
			int64(len(childraw)),
			childraw,
		)
		if err != nil {
			log.CtxErrorf(ctx, "error writing image %s to the CAS: %s", childRef, err)
		}
	}
	if childm.MediaType == types.OCIImageIndex || childm.MediaType == types.DockerManifestList {
		return nil, fmt.Errorf("child manifest %s is itself an image index", childRef)
	}
	childimg := &cacheAwareImage{
		ref:      *childcref,
		digest:   childRef,
		acc:      acc,
		bsc:      bsc,
		raw:      childraw,
		manifest: childm,
		options:  remoteOpts,
	}
	return partial.CompressedToImage(childimg)
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

func fetchRawManifestFromCacheOrRemote(ctx context.Context, acc repb.ActionCacheClient, bsc bspb.ByteStreamClient, digestOrTagRef ctrname.Reference, remoteOpts []remote.Option) (*cacheableRef, []byte, bool, error) {
	desc, err := remote.Head(digestOrTagRef, remoteOpts...)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, nil, false, status.PermissionDeniedErrorf("could not retrieve image manifest: %s", err)
		}
		return nil, nil, false, status.UnavailableErrorf("could not retrieve manifest from remote: %s", err)
	}
	cref := &cacheableRef{
		registry:   digestOrTagRef.Context().RegistryStr(),
		repository: digestOrTagRef.Context().RepositoryStr(),
		hash:       desc.Digest,
	}

	digest := digestOrTagRef.Context().Digest(desc.Digest.String())
	casDigest, _, _, _, err := FetchBlobOrManifestMetadataFromCache(ctx, acc, bsc, digest, ocipb.OCIResourceType_MANIFEST)
	if err == nil {
		raw := &bytes.Buffer{}
		err := FetchBlobOrManifestFromCache(ctx, bsc, casDigest, raw)
		if err == nil {
			return cref, raw.Bytes(), true, nil
		}
		if !status.IsNotFoundError(err) {
			log.CtxErrorf(ctx, "error fetching manifest %s from the CAS: %s", digest.Context(), err)
			return nil, nil, false, err
		}
	}

	remoteDesc, err := remote.Get(digestOrTagRef, remoteOpts...)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, nil, false, status.PermissionDeniedErrorf("could not retrieve image manifest: %s", err)
		}
		return nil, nil, false, status.UnavailableErrorf("could not retrieve manifest from remote: %s", err)
	}
	return cref, remoteDesc.Manifest, false, nil
}

func FetchBlobOrManifestMetadataFromCache(ctx context.Context, acc repb.ActionCacheClient, bsc bspb.ByteStreamClient, ref ctrname.Reference, ociResourceType ocipb.OCIResourceType) (*repb.Digest, *v1.Hash, string, int64, error) {
	hash, err := v1.NewHash(ref.Identifier())
	if err != nil {
		return nil, nil, "", 0, err
	}
	arKey := &ocipb.OCIActionResultKey{
		Registry:      ref.Context().RegistryStr(),
		Repository:    ref.Context().RepositoryStr(),
		ResourceType:  ociResourceType,
		HashAlgorithm: hash.Algorithm,
		HashHex:       hash.Hex,
	}
	arKeyBytes, err := proto.Marshal(arKey)
	if err != nil {
		return nil, nil, "", 0, err
	}
	arDigest, err := digest.Compute(bytes.NewReader(arKeyBytes), repb.DigestFunction_SHA256)
	if err != nil {
		return nil, nil, "", 0, err
	}
	arRN := digest.NewACResourceName(
		arDigest,
		arInstanceName(ref.Context().RegistryStr(), ref.Context().RepositoryStr()),
		repb.DigestFunction_SHA256,
	)
	ar, err := cachetools.GetActionResult(ctx, acc, arRN)
	if err != nil {
		return nil, nil, "", 0, err
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
		return nil, nil, "", 0, fmt.Errorf("missing blob metadata digest or blob digest for %s", ref)
	}
	blobMetadataRN := digest.NewCASResourceName(
		blobMetadataCASDigest,
		"",
		repb.DigestFunction_SHA256,
	)
	blobMetadata := &ocipb.OCIBlobMetadata{}
	err = cachetools.GetBlobAsProto(ctx, bsc, blobMetadataRN, blobMetadata)
	if err != nil {
		return nil, nil, "", 0, err
	}
	return blobCASDigest, &hash, blobMetadata.GetContentType(), blobMetadata.GetContentLength(), nil
}

func FetchBlobOrManifestFromCache(ctx context.Context, bsc bspb.ByteStreamClient, casDigest *repb.Digest, w io.Writer) error {
	blobRN := digest.NewCASResourceName(
		casDigest,
		"",
		repb.DigestFunction_SHA256,
	)
	blobRN.SetCompressor(repb.Compressor_ZSTD)
	return cachetools.GetBlob(ctx, bsc, blobRN, w)
}

func UploadBlobOrManifestToCache(ctx context.Context, acc repb.ActionCacheClient, bsc bspb.ByteStreamClient, ref ctrname.Reference, ociResourceType ocipb.OCIResourceType, hash v1.Hash, contentType string, contentLength int64, r io.Reader) error {
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
	_, _, err := cachetools.UploadFromReader(ctx, bsc, blobRN, r)
	if err != nil {
		return err
	}

	blobMetadata := &ocipb.OCIBlobMetadata{
		ContentLength: contentLength,
		ContentType:   contentType,
	}
	blobMetadataCASDigest, err := cachetools.UploadProto(ctx, bsc, "", repb.DigestFunction_SHA256, blobMetadata)
	if err != nil {
		return err
	}

	arKey := &ocipb.OCIActionResultKey{
		Registry:      ref.Context().RegistryStr(),
		Repository:    ref.Context().RepositoryStr(),
		ResourceType:  ociResourceType,
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
		arInstanceName(ref.Context().RegistryStr(), ref.Context().RepositoryStr()),
		repb.DigestFunction_SHA256,
	)
	err = cachetools.UploadActionResult(ctx, acc, arRN, ar)
	if err != nil {
		return err
	}
	return nil
}

func writeManifestToCache(ctx context.Context, acc repb.ActionCacheClient, bsc bspb.ByteStreamClient, digest ctrname.Digest, contentType string, contentLength int64, raw []byte) error {
	hash, err := v1.NewHash(digest.DigestStr())
	if err != nil {
		return fmt.Errorf("could not parse hash from %s: %s", digest.Context(), err)
	}
	r := bytes.NewReader(raw)
	return UploadBlobOrManifestToCache(
		ctx,
		acc,
		bsc,
		digest,
		ocipb.OCIResourceType_MANIFEST,
		hash,
		contentType,
		contentLength,
		r,
	)
}

type cacheableRef struct {
	registry   string
	repository string
	hash       v1.Hash
}

// matchesPlatform checks if the given platform matches the required platforms.
// The given platform matches the required platform if
// - architecture and OS are identical.
// - OS version and variant are identical if provided.
// - features and OS features of the required platform are subsets of those of the given platform.
func matchesPlatform(given, required v1.Platform) bool {
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

func arInstanceName(registry, repository string) string {
	return registry + "|" + repository + "|" + actionResultInstanceNameSalt
}

func WriteConfigToCache(ctx context.Context, acClient repb.ActionCacheClient, bsClient bspb.ByteStreamClient, registry, repository string, hash v1.Hash, contentType string, contentLength int64, raw []byte) error {
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
	buf := bytes.NewBuffer(raw)
	_, _, err := cachetools.UploadFromReader(ctx, bsClient, blobRN, buf)
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
		Registry:      registry,
		Repository:    repository,
		ResourceType:  ocipb.OCIResourceType_BLOB,
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
		arInstanceName(registry, repository),
		repb.DigestFunction_SHA256,
	)
	err = cachetools.UploadActionResult(ctx, acClient, arRN, ar)
	if err != nil {
		return err
	}
	return nil
}

func fetchLayerCASDigest(ctx context.Context, acClient repb.ActionCacheClient, registry, repository string, hash v1.Hash) (*repb.Digest, error) {
	arKey := &ocipb.OCIActionResultKey{
		Registry:      registry,
		Repository:    repository,
		ResourceType:  ocipb.OCIResourceType_BLOB,
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
		arInstanceName(registry, repository),
		repb.DigestFunction_SHA256,
	)
	ar, err := cachetools.GetActionResult(ctx, acClient, arRN)
	if err != nil {
		return nil, err
	}

	var blobCASDigest *repb.Digest
	for _, outputFile := range ar.GetOutputFiles() {
		switch outputFile.GetPath() {
		case blobMetadataOutputFilePath:
			continue
		case blobOutputFilePath:
			blobCASDigest = outputFile.GetDigest()
		default:
			log.CtxErrorf(ctx, "unknown output file path '%s' in ActionResult for %s/%s:%s", outputFile.GetPath(), registry, repository, hash)
		}
	}
	if blobCASDigest == nil {
		return nil, fmt.Errorf("missing blob metadata digest or blob digest for %s/%s:%s", registry, repository, hash)
	}
	return blobCASDigest, nil
}

func FetchLayerFromCAS(ctx context.Context, bsClient bspb.ByteStreamClient, blobCASDigest *repb.Digest, w io.Writer) error {
	blobRN := digest.NewCASResourceName(
		blobCASDigest,
		"",
		repb.DigestFunction_SHA256,
	)
	blobRN.SetCompressor(repb.Compressor_ZSTD)
	return cachetools.GetBlob(ctx, bsClient, blobRN, w)
}

func FetchConfigFromCache(ctx context.Context, acClient repb.ActionCacheClient, bsClient bspb.ByteStreamClient, registry, repository string, hash v1.Hash) ([]byte, error) {
	arKey := &ocipb.OCIActionResultKey{
		Registry:      registry,
		Repository:    repository,
		ResourceType:  ocipb.OCIResourceType_BLOB,
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
		arInstanceName(registry, repository),
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
			log.CtxErrorf(ctx, "unknown output file path '%s' in ActionResult for %s/%s:%s", outputFile.GetPath(), registry, repository, hash)
		}
	}
	if blobMetadataCASDigest == nil || blobCASDigest == nil {
		return nil, fmt.Errorf("missing blob metadata digest or blob digest for %s/%s:%s", registry, repository, hash)
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
	w := &bytes.Buffer{}
	if err := cachetools.GetBlob(ctx, bsClient, blobRN, w); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

type cachingLayerWriteCloser struct {
	ctx           context.Context
	acClient      repb.ActionCacheClient
	bsClient      bspb.ByteStreamClient
	blobCASDigest *repb.Digest
	registry      string
	repository    string
	hash          v1.Hash
	contentType   string
	contentLength int64
	wc            io.WriteCloser
}

func (clwc *cachingLayerWriteCloser) Write(p []byte) (int, error) {
	n, err := clwc.wc.Write(p)
	return n, err
}

func (clwc *cachingLayerWriteCloser) Close() error {
	err := clwc.wc.Close()
	if err != nil {
		return err
	}

	blobMetadata := &ocipb.OCIBlobMetadata{
		ContentLength: clwc.contentLength,
		ContentType:   clwc.contentType,
	}
	blobMetadataCASDigest, err := cachetools.UploadProto(clwc.ctx, clwc.bsClient, "", repb.DigestFunction_SHA256, blobMetadata)
	if err != nil {
		return err
	}

	arKey := &ocipb.OCIActionResultKey{
		Registry:      clwc.registry,
		Repository:    clwc.repository,
		ResourceType:  ocipb.OCIResourceType_BLOB,
		HashAlgorithm: clwc.hash.Algorithm,
		HashHex:       clwc.hash.Hex,
	}
	ar := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			{
				Path:   blobOutputFilePath,
				Digest: clwc.blobCASDigest,
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
		arInstanceName(clwc.registry, clwc.repository),
		repb.DigestFunction_SHA256,
	)
	err = cachetools.UploadActionResult(clwc.ctx, clwc.acClient, arRN, ar)
	if err != nil {
		return err
	}
	return nil
}

func newCachingLayerWriteCloser(ctx context.Context, acClient repb.ActionCacheClient, bsClient bspb.ByteStreamClient, registry, repository string, hash v1.Hash, contentType string, contentLength int64) (io.WriteCloser, error) {
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
	wc, err := cachetools.NewUploadWriteCloser(ctx, bsClient, blobRN)
	if err != nil {
		return nil, err
	}
	return &cachingLayerWriteCloser{
		ctx:           ctx,
		acClient:      acClient,
		bsClient:      bsClient,
		blobCASDigest: blobCASDigest,
		registry:      registry,
		repository:    repository,
		hash:          hash,
		contentType:   contentType,
		contentLength: contentLength,
		wc:            wc,
	}, nil
}

func WriteLayerToCache(ctx context.Context, acClient repb.ActionCacheClient, bsClient bspb.ByteStreamClient, registry, repository string, hash v1.Hash, contentType string, contentLength int64, r io.Reader) error {
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
	_, _, err := cachetools.UploadFromReader(ctx, bsClient, blobRN, r)
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
		Registry:      registry,
		Repository:    repository,
		ResourceType:  ocipb.OCIResourceType_BLOB,
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
		arInstanceName(registry, repository),
		repb.DigestFunction_SHA256,
	)
	err = cachetools.UploadActionResult(ctx, acClient, arRN, ar)
	if err != nil {
		return err
	}
	return nil
}

type cacheAwareImage struct {
	ref    cacheableRef
	digest ctrname.Digest
	acc    repb.ActionCacheClient
	bsc    bspb.ByteStreamClient

	raw      []byte
	manifest *v1.Manifest
	options  []remote.Option
}

func (i *cacheAwareImage) RawManifest() ([]byte, error) {
	log.Infof("RawManifest() on %s", i.digest)
	return i.raw, nil
}

func (i *cacheAwareImage) RawConfigFile() ([]byte, error) {
	log.Infof("RawConfigFile() on %s", i.digest)
	if i.manifest.Config.Data != nil {
		return i.manifest.Config.Data, nil
	}
	ctx := context.Background()
	raw, err := FetchConfigFromCache(ctx, i.acc, i.bsc, i.ref.registry, i.ref.repository, i.manifest.Config.Digest)
	if err == nil {
		log.Infof("RawConfigFile from cache '%s'", string(raw))
		return raw, nil
	} else if !status.IsNotFoundError(err) {
		log.Errorf("error reading config from cache for %s", i.digest)
		return nil, err
	}
	digest := i.digest.Digest(i.manifest.Config.Digest.String())

	rl, err := remote.Layer(digest, i.options...)
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
	err = WriteConfigToCache(
		ctx,
		i.acc,
		i.bsc,
		i.ref.registry,
		i.ref.repository,
		i.manifest.Config.Digest,
		string(i.manifest.Config.MediaType),
		int64(len(remoteraw)),
		remoteraw,
	)
	if err != nil {
		log.CtxErrorf(ctx, "could not write config %s to cache", digest)
	}
	log.Infof("RawConfigFile from remote '%s'", string(remoteraw))
	return remoteraw, nil
}

func (i *cacheAwareImage) MediaType() (types.MediaType, error) {
	return i.manifest.MediaType, nil
}

func (i *cacheAwareImage) LayerByDigest(hash v1.Hash) (partial.CompressedLayer, error) {
	lcref := cacheableRef{
		registry:   i.ref.registry,
		repository: i.ref.repository,
		hash:       hash,
	}
	for _, d := range i.manifest.Layers {
		if d.Digest == hash {
			rlref, err := ctrname.NewDigest(lcref.registry + "/" + lcref.repository + "@" + hash.String())
			if err != nil {
				return nil, fmt.Errorf("could not construct ref for layer %s", d.Digest)
			}
			rl, err := remote.Layer(rlref, i.options...)
			if err != nil {
				return nil, fmt.Errorf("could not create remote layer for %s", rlref)
			}
			l := &cacheAwareLayer{
				digest:      i.digest.Context().Digest(hash.String()),
				ref:         lcref,
				acc:         i.acc,
				bsc:         i.bsc,
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
	ref cacheableRef
	acc repb.ActionCacheClient
	bsc bspb.ByteStreamClient

	digest      ctrname.Digest
	descriptor  *v1.Descriptor
	remoteLayer v1.Layer
}

func (l *cacheAwareLayer) Digest() (v1.Hash, error) {
	if l.descriptor != nil {
		return l.descriptor.Digest, nil
	}
	return l.remoteLayer.Digest()
}

func newTeeReadCloser(rc io.ReadCloser, wc io.WriteCloser) io.ReadCloser {
	return &teeReadCloser{rc, wc}
}

type teeReadCloser struct {
	rc io.ReadCloser
	wc io.WriteCloser
}

func (t *teeReadCloser) Read(p []byte) (int, error) {
	n, err := t.rc.Read(p)
	if n > 0 {
		_, werr := t.wc.Write(p[:n])
		if werr != nil && werr == io.EOF {
			return n, io.EOF
		}
		if werr != nil {
			return n, err
		}
	}
	return n, err
}

func (t *teeReadCloser) Close() error {
	t.wc.Close()
	return t.rc.Close()
}

func (l *cacheAwareLayer) Compressed() (io.ReadCloser, error) {
	ctx := context.Background()
	casDigest, _, _, _, err := FetchBlobOrManifestMetadataFromCache(
		ctx,
		l.acc,
		l.bsc,
		l.digest,
		ocipb.OCIResourceType_BLOB,
	)
	if err != nil && !status.IsNotFoundError(err) {
		log.CtxErrorf(ctx, "error fetching CAS digest for layer in %s: %s", l.digest.Context(), err)
	}
	if err == nil && casDigest != nil {
		r, w := io.Pipe()
		go func() {
			defer w.Close()
			err := FetchBlobOrManifestFromCache(
				ctx,
				l.bsc,
				casDigest,
				w,
			)
			if err != nil {
				log.Errorf("error fetching layer from CAS in %s: %s", l.digest.Context(), err)
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
	caswc, err := newCachingLayerWriteCloser(
		ctx,
		l.acc,
		l.bsc,
		l.ref.registry,
		l.ref.repository,
		l.ref.hash,
		string(mediaType),
		size,
	)
	if err != nil {
		// cannot cache, but we can still fetch the remote layer
		log.CtxErrorf(ctx, "cannot cache OCI image layer for %s/%s@%s: %s", l.ref.registry, l.ref.repository, l.ref.hash, err)
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
