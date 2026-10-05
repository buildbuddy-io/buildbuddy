package oci

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetcher"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocimanifest"
	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
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
	"github.com/google/go-containerregistry/pkg/v1/partial"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

const (
	// resolveImageDigestLRUMaxEntries limits the number of entries in the image-tag-to-digest cache.
	resolveImageDigestLRUMaxEntries = 1000
	resolveImageDigestLRUDuration   = 15 * time.Minute

	fetchLocationExecutor = "executor"
	fetchLocationRemote   = "remote"
)

var (
	registries             = flag.Slice("executor.container_registries", []Registry{}, "")
	defaultKeychainEnabled = flag.Bool("executor.container_registry_default_keychain_enabled", false, "Enable the default container registry keychain, respecting both docker configs and podman configs.")
	useOCIFetcherEnabled   = flag.Bool("executor.use_oci_fetcher", false, "Whether to use the OCI fetcher service for pulling container images when the use-oci-fetcher platform property is set.")
	fetchLocation          = flag.String("executor.oci_fetch_location", fetchLocationExecutor, "Where container images are fetched from remote registries: 'executor' fetches them in the executor process, and 'remote' fetches them with the OCI fetcher service at the executor's cache target (the app or a cache proxy).")

	cacheEnabledPercent = flag.Int("executor.container_registry.use_cache_percent", 0, "Percentage of image pulls that should use the BuildBuddy remote cache for manifests and layers, when images are fetched in the executor process.")
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

type Resolver struct {
	env environment.Env

	// cachedFetcher fetches images in-process, caching manifests and layers
	// in the BuildBuddy remote cache. It is nil if the environment has no
	// cache clients.
	cachedFetcher ofpb.OCIFetcherClient
	// uncachedFetcher fetches images in-process, directly from the remote
	// registry.
	uncachedFetcher ofpb.OCIFetcherClient

	imageTagToDigestLRU lru.LRU[string]
}

func NewResolver(env environment.Env) (*Resolver, error) {
	if *fetchLocation != fetchLocationExecutor && *fetchLocation != fetchLocationRemote {
		return nil, status.InvalidArgumentErrorf("invalid value %q for executor.oci_fetch_location: must be %q or %q", *fetchLocation, fetchLocationExecutor, fetchLocationRemote)
	}
	uncachedServer, err := ocifetcher.NewInProcessServer(nil, nil)
	if err != nil {
		return nil, err
	}
	var cachedFetcher ofpb.OCIFetcherClient
	if env.GetByteStreamClient() != nil && env.GetActionCacheClient() != nil {
		cachedServer, err := ocifetcher.NewInProcessServer(env.GetByteStreamClient(), env.GetActionCacheClient())
		if err != nil {
			return nil, err
		}
		cachedFetcher = ocifetcher.NewInProcessClient(cachedServer)
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
		cachedFetcher:       cachedFetcher,
		uncachedFetcher:     ocifetcher.NewInProcessClient(uncachedServer),
		imageTagToDigestLRU: imageTagToDigestLRU,
	}, nil
}

// fetcher returns the OCIFetcherClient to fetch images with.
//
// Images are fetched by the remote OCI fetcher service if
// --executor.oci_fetch_location=remote, or if useOCIFetcher is set (from the
// use-oci-fetcher platform property) and --executor.use_oci_fetcher is
// enabled. Otherwise they are fetched in the executor process.
func (r *Resolver) fetcher(ctx context.Context, imageRef ctrname.Reference, useOCIFetcher bool) (ofpb.OCIFetcherClient, error) {
	if *fetchLocation == fetchLocationRemote || (useOCIFetcher && *useOCIFetcherEnabled) {
		if r.env.GetOCIFetcherClient() == nil {
			return nil, status.FailedPreconditionError("an OCIFetcherClient is required to fetch images remotely")
		}
		return r.env.GetOCIFetcherClient(), nil
	}

	cacheEnabled := false
	if *cacheEnabledPercent >= 100 {
		cacheEnabled = true
	} else if *cacheEnabledPercent > 0 && *cacheEnabledPercent < 100 {
		cacheEnabled = rand.Intn(100) < *cacheEnabledPercent
	}
	if !cacheEnabled || r.cachedFetcher == nil {
		return r.uncachedFetcher, nil
	}
	if isAnonymousUser(ctx) {
		log.CtxInfof(ctx, "Anonymous user request, skipping manifest and layer cache for %q", imageRef)
		return r.uncachedFetcher, nil
	}
	return r.cachedFetcher, nil
}

// AuthenticateWithRegistry makes a HEAD request to a remote registry with the input credentials.
// Any errors encountered are returned.
// Otherwise, the function returns nil and it is safe to assume the input credentials grant access
// to the image.
func (r *Resolver) AuthenticateWithRegistry(ctx context.Context, imageName string, credentials Credentials, useOCIFetcher bool) error {
	if credentials.bypassRegistry {
		return nil
	}

	log.CtxDebugf(ctx, "Authenticating with registry for %q", imageName)

	imageRef, err := ctrname.ParseReference(imageName)
	if err != nil {
		return status.InvalidArgumentErrorf("invalid image reference %q: %s", imageName, err)
	}
	fetcher, err := r.fetcher(ctx, imageRef, useOCIFetcher)
	if err != nil {
		return err
	}
	_, err = fetcher.FetchManifestMetadata(ctx, &ofpb.FetchManifestMetadataRequest{
		Ref:         imageRef.String(),
		Credentials: credentials.ToProto(),
	})
	return err
}

// ResolveImageDigest takes an image name and returns an image name with a digest.
// If the input image name includes a digest, a canonicalized version of the name is returned.
// If the input image name refers to a tag (either explictly or implicity), ResolveImageDigest
// will make a HEAD request to the remote registry.
// ResolveImageDigest keeps an LRU cache that maps between canonical image names with tags
// to image names with digests, to reduce the number of HEAD requests.
func (r *Resolver) ResolveImageDigest(ctx context.Context, imageName string, credentials Credentials, useOCIFetcher bool) (string, error) {
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

	fetcher, err := r.fetcher(ctx, tagRef, useOCIFetcher)
	if err != nil {
		return "", err
	}
	resp, err := fetcher.FetchManifestMetadata(ctx, &ofpb.FetchManifestMetadataRequest{
		Ref:         tagRef.String(),
		Credentials: credentials.ToProto(),
	})
	if err != nil {
		return "", err
	}
	imageNameWithDigest := tagRef.Context().Digest(resp.GetDigest()).String()
	r.imageTagToDigestLRU.Add(tagRef.String(), imageNameWithDigest)
	return imageNameWithDigest, nil
}

func (r *Resolver) Resolve(ctx context.Context, imageName string, platform *rgpb.Platform, credentials Credentials, useOCIFetcher bool) (ctr.Image, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	imageRef, err := ctrname.ParseReference(imageName)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image %q", imageName)
	}
	log.CtxDebugf(ctx, "Resolving image %q", imageRef)

	fetcher, err := r.fetcher(ctx, imageRef, useOCIFetcher)
	if err != nil {
		return nil, err
	}
	return fetchImage(
		ctx,
		imageRef,
		ctr.Platform{
			Architecture: platform.GetArch(),
			OS:           platform.GetOs(),
			Variant:      platform.GetVariant(),
		},
		fetcher,
		credentials,
	)
}

// fetchImage fetches the manifest for the given image reference.
// If the referenced manifest is actually an image index, fetchImage will recur at most once
// to fetch a child image matching the given platform.
func fetchImage(ctx context.Context, digestOrTagRef ctrname.Reference, platform ctr.Platform, fetcher ofpb.OCIFetcherClient, credentials Credentials) (ctr.Image, error) {
	resp, err := fetcher.FetchManifest(ctx, &ofpb.FetchManifestRequest{
		Ref:            digestOrTagRef.String(),
		Credentials:    credentials.ToProto(),
		BypassRegistry: credentials.bypassRegistry,
	})
	if err != nil {
		return nil, err
	}
	digest, err := ctr.NewHash(resp.GetDigest())
	if err != nil {
		return nil, status.InternalErrorf("invalid digest %q from OCI fetcher: %s", resp.GetDigest(), err)
	}
	desc := ctr.Descriptor{
		Digest:    digest,
		Size:      resp.GetSize(),
		MediaType: types.MediaType(resp.GetMediaType()),
	}
	return imageFromDescriptorAndManifest(ctx, digestOrTagRef.Context(), desc, resp.GetManifest(), platform, fetcher, credentials)
}

// imageFromDescriptorAndManifest returns an Image from the given manifest (if the manifest is an image manifest),
// finds a child image matching the given platform (and fetches a manifest for it) if the given manifest is an index,
// and otherwise returns an error.
func imageFromDescriptorAndManifest(ctx context.Context, repo ctrname.Repository, desc ctr.Descriptor, rawManifest []byte, platform ctr.Platform, fetcher ofpb.OCIFetcherClient, credentials Credentials) (ctr.Image, error) {
	if desc.MediaType.IsSchema1() {
		return nil, status.UnknownErrorf("unsupported MediaType %q", desc.MediaType)
	}

	if desc.MediaType.IsIndex() {
		indexManifest, err := ctr.ParseIndexManifest(bytes.NewReader(rawManifest))
		if err != nil {
			return nil, status.UnknownErrorf("error parsing index manifest: %s", err)
		}

		desc, err := ocimanifest.FindFirstImageManifest(*indexManifest, platform)
		if err != nil {
			return nil, status.UnknownErrorf("Could not find child image for platform in index: %s", err)
		}
		ref := repo.Digest(desc.Digest.String())
		return fetchImage(ctx, ref, platform, fetcher, credentials)
	}

	return newImageFromRawManifest(ctx, repo, desc, rawManifest, fetcher, credentials), nil
}

// RuntimePlatform returns the platform on which the program is being executed,
// as reported by the go runtime.
func RuntimePlatform() *rgpb.Platform {
	return &rgpb.Platform{
		Arch: runtime.GOARCH,
		Os:   runtime.GOOS,
	}
}

func newImageFromRawManifest(ctx context.Context, repo ctrname.Repository, desc ctr.Descriptor, rawManifest []byte, fetcher ofpb.OCIFetcherClient, credentials Credentials) *imageFromRawManifest {
	i := &imageFromRawManifest{
		repo:        repo,
		desc:        desc,
		rawManifest: rawManifest,
		ctx:         ctx,
		fetcher:     fetcher,
		credentials: credentials,
	}
	i.fetchRawConfigOnce = sync.OnceValues(func() ([]byte, error) {
		manifest, err := i.Manifest()
		if err != nil {
			return nil, err
		}
		if manifest.Config.Data != nil {
			return manifest.Config.Data, nil
		}
		layer := newLayerFromDigest(i.repo, manifest.Config.Digest, i, nil)

		rc, err := layer.Uncompressed()
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		return io.ReadAll(rc)
	})
	return i
}

var _ ctr.Image = (*imageFromRawManifest)(nil)

// imageFromRawManifest implements the go-containerregistry Image interface.
// It allows us to construct an Image from a raw manifest returned by an
// OCIFetcher, and to fetch its layers through that OCIFetcher.
type imageFromRawManifest struct {
	repo        ctrname.Repository
	desc        ctr.Descriptor
	rawManifest []byte

	ctx         context.Context
	fetcher     ofpb.OCIFetcherClient
	credentials Credentials

	fetchRawConfigOnce func() ([]byte, error)
}

func (i *imageFromRawManifest) Digest() (ctr.Hash, error) {
	return i.desc.Digest, nil
}

func (i *imageFromRawManifest) RawManifest() ([]byte, error) {
	return i.rawManifest, nil
}

func (i *imageFromRawManifest) Manifest() (*ctr.Manifest, error) {
	rawManifest, err := i.RawManifest()
	if err != nil {
		return nil, err
	}
	manifest, err := ctr.ParseManifest(bytes.NewReader(rawManifest))
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

// RawConfigFile looks for the raw config file bytes
// in the rawConfigFile field, then in the manifest's Config section,
// then from the OCIFetcher.
func (i *imageFromRawManifest) RawConfigFile() ([]byte, error) {
	return i.fetchRawConfigOnce()
}

func (i *imageFromRawManifest) ConfigFile() (*ctr.ConfigFile, error) {
	rawConfigFile, err := i.RawConfigFile()
	if err != nil {
		return nil, err
	}
	return ctr.ParseConfigFile(bytes.NewReader(rawConfigFile))
}

func (i *imageFromRawManifest) ConfigName() (ctr.Hash, error) {
	manifest, err := i.Manifest()
	if err != nil {
		return ctr.Hash{}, err
	}
	return manifest.Config.Digest, nil
}

func (i *imageFromRawManifest) Layers() ([]ctr.Layer, error) {
	m, err := i.Manifest()
	if err != nil {
		return nil, err
	}
	layers := make([]ctr.Layer, 0, len(m.Layers))
	for _, layerDesc := range m.Layers {
		layers = append(layers, newLayerFromDigest(i.repo, layerDesc.Digest, i, &layerDesc))
	}
	return layers, nil
}

func (i *imageFromRawManifest) LayerByDigest(digest ctr.Hash) (ctr.Layer, error) {
	return newLayerFromDigest(i.repo, digest, i, nil), nil
}

func (i *imageFromRawManifest) LayerByDiffID(diffID ctr.Hash) (ctr.Layer, error) {
	digest, err := partial.DiffIDToBlob(i, diffID)
	if err != nil {
		return nil, err
	}
	return newLayerFromDigest(i.repo, digest, i, nil), nil
}

func newLayerFromDigest(repo ctrname.Repository, digest ctr.Hash, image *imageFromRawManifest, desc *ctr.Descriptor) *layerFromDigest {
	return &layerFromDigest{
		repo:   repo,
		digest: digest,
		image:  image,
		desc:   desc,
	}
}

var _ ctr.Layer = (*layerFromDigest)(nil)

// layerFromDigest implements the go-containerregistry Layer interface.
// It fetches the layer through the image's OCIFetcher.
type layerFromDigest struct {
	repo   ctrname.Repository
	digest ctr.Hash
	image  *imageFromRawManifest

	desc *ctr.Descriptor
}

func (l *layerFromDigest) Digest() (ctr.Hash, error) {
	return l.digest, nil
}

func (l *layerFromDigest) DiffID() (ctr.Hash, error) {
	return partial.BlobToDiffID(l.image, l.digest)
}

func (l *layerFromDigest) Compressed() (io.ReadCloser, error) {
	ref := l.repo.Digest(l.digest.String())
	// Create a cancellable context so that Close() can abort the stream
	// if the caller doesn't read to EOF.
	ctx, cancel := context.WithCancel(l.image.ctx)
	stream, err := l.image.fetcher.FetchBlob(ctx, &ofpb.FetchBlobRequest{
		Ref:            ref.String(),
		Credentials:    l.image.credentials.ToProto(),
		BypassRegistry: l.image.credentials.bypassRegistry,
	})
	if err != nil {
		cancel()
		return nil, err
	}
	return newStreamReader(stream, cancel), nil
}

// Uncompressed fetches the compressed bytes from the upstream server
// and returns a ReadCloser that decompresses as it reads.
func (l *layerFromDigest) Uncompressed() (io.ReadCloser, error) {
	cl, err := partial.CompressedToLayer(l)
	if err != nil {
		return nil, err
	}
	return cl.Uncompressed()
}

func (l *layerFromDigest) Size() (int64, error) {
	if l.desc != nil {
		return l.desc.Size, nil
	}
	ref := l.repo.Digest(l.digest.String())
	resp, err := l.image.fetcher.FetchBlobMetadata(l.image.ctx, &ofpb.FetchBlobMetadataRequest{
		Ref:            ref.String(),
		Credentials:    l.image.credentials.ToProto(),
		BypassRegistry: l.image.credentials.bypassRegistry,
	})
	if err != nil {
		return 0, err
	}
	return resp.GetSize(), nil
}

func (l *layerFromDigest) MediaType() (types.MediaType, error) {
	return types.DockerLayer, nil
}

// streamReader wraps a FetchBlob stream as an io.ReadCloser.
type streamReader struct {
	stream ofpb.OCIFetcher_FetchBlobClient
	cancel context.CancelFunc
	buf    []byte
}

func newStreamReader(stream ofpb.OCIFetcher_FetchBlobClient, cancel context.CancelFunc) *streamReader {
	return &streamReader{stream: stream, cancel: cancel}
}

func (r *streamReader) Read(p []byte) (int, error) {
	if len(r.buf) == 0 {
		resp, err := r.stream.Recv()
		if err != nil {
			return 0, err
		}
		r.buf = resp.GetData()
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

// Close cancels the underlying gRPC stream context to release resources.
// This is safe to call even if the stream has been fully read (cancel is a no-op
// after the context is already done).
func (r *streamReader) Close() error {
	r.cancel()
	return nil
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
