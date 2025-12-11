package oci

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetcher"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocimanifest"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/distribution/reference"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/v1/partial"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/jonboulle/clockwork"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
)

const (
	// resolveImageDigestLRUMaxEntries limits the number of entries in the image-tag-to-digest cache.
	resolveImageDigestLRUMaxEntries = 1000
	resolveImageDigestLRUDuration   = 15 * time.Minute
)

var (
	registries             = flag.Slice("executor.container_registries", []Registry{}, "")
	defaultKeychainEnabled = flag.Bool("executor.container_registry_default_keychain_enabled", false, "Enable the default container registry keychain, respecting both docker configs and podman configs.")
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

type tagToDigestEntry struct {
	nameWithDigest string
	expiration     time.Time
}

type Resolver struct {
	env environment.Env

	mu                  sync.Mutex
	imageTagToDigestLRU *lru.LRU[tagToDigestEntry]
	clock               clockwork.Clock
}

func NewResolver(env environment.Env) (*Resolver, error) {
	if env.GetOCIFetcherClient() == nil {
		return nil, status.FailedPreconditionError("OCIFetcherClient is required")
	}
	imageTagToDigestLRU, err := lru.NewLRU[tagToDigestEntry](&lru.Config[tagToDigestEntry]{
		SizeFn:  func(_ tagToDigestEntry) int64 { return 1 },
		MaxSize: int64(resolveImageDigestLRUMaxEntries),
	})
	if err != nil {
		return nil, err
	}
	return &Resolver{
		env:                 env,
		imageTagToDigestLRU: imageTagToDigestLRU,
		clock:               env.GetClock(),
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

	log.CtxInfof(ctx, "Authenticating with registry for %q", imageName)
	_, err := r.env.GetOCIFetcherClient().FetchManifestMetadata(ctx, &ofpb.FetchManifestMetadataRequest{
		Ref: imageName,
		Credentials: &rgpb.Credentials{
			Username: credentials.Username,
			Password: credentials.Password,
		},
	})
	if err != nil {
		return err
	}
	return nil
}

// ResolveImageDigest takes an image name and returns an image name with a digest.
// If the input image name includes a digest, a canonicalized version of the name is returned.
// If the input image name refers to a tag (either explictly or implicity), ResolveImageDigest
// will make a HEAD request to the remote registry.
// ResolveImageDigest keeps an LRU cache that maps between canonical image names with tags
// to image names with digests, to reduce the number of HEAD requests.
func (r *Resolver) ResolveImageDigest(ctx context.Context, imageName string, platform *rgpb.Platform, credentials Credentials) (string, error) {
	if imageRefWithDigest, err := gcrname.NewDigest(imageName); err == nil {
		return imageRefWithDigest.String(), nil
	}
	tagRef, err := gcrname.ParseReference(imageName)
	if err != nil {
		return "", status.InvalidArgumentErrorf("invalid image name %q", imageName)
	}

	r.mu.Lock()
	entry, ok := r.imageTagToDigestLRU.Get(tagRef.String())
	r.mu.Unlock()
	if ok {
		if entry.expiration.After(r.clock.Now()) {
			return entry.nameWithDigest, nil
		}
		// The entry has expired. Evict it!
		r.mu.Lock()
		r.imageTagToDigestLRU.Remove(tagRef.String())
		r.mu.Unlock()
	}

	resp, err := r.env.GetOCIFetcherClient().FetchManifestMetadata(ctx, &ofpb.FetchManifestMetadataRequest{
		Ref: imageName,
		Credentials: &rgpb.Credentials{
			Username: credentials.Username,
			Password: credentials.Password,
		},
	})
	if err != nil {
		if status.IsPermissionDeniedError(err) {
			return "", err
		}
		return "", status.UnavailableErrorf("could not authorize to remote registry: %s", err)
	}
	imageNameWithDigest := tagRef.Context().Digest(resp.GetDigest()).String()
	entryToAdd := tagToDigestEntry{
		nameWithDigest: imageNameWithDigest,
		expiration:     r.clock.Now().Add(resolveImageDigestLRUDuration),
	}
	r.mu.Lock()
	r.imageTagToDigestLRU.Add(tagRef.String(), entryToAdd)
	r.mu.Unlock()
	return imageNameWithDigest, nil
}

func (r *Resolver) Resolve(ctx context.Context, imageName string, platform *rgpb.Platform, credentials Credentials) (gcr.Image, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	imageRef, err := gcrname.ParseReference(imageName)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image %q", imageName)
	}
	log.CtxInfof(ctx, "Resolving image %q", imageRef)

	return fetchImageFromRemote(
		ctx,
		imageRef,
		gcr.Platform{
			Architecture: platform.GetArch(),
			OS:           platform.GetOs(),
			Variant:      platform.GetVariant(),
		},
		r.env.GetOCIFetcherClient(),
		credentials,
	)
}

// fetchImageFromRemote fetches the manifest for the given image reference.
// Caching is handled by OCIFetcherClient.
// If the referenced manifest is actually an image index, fetchImageFromRemote will recur at most once
// to fetch a child image matching the given platform.
func fetchImageFromRemote(ctx context.Context, digestOrTagRef gcrname.Reference, platform gcr.Platform, client ofpb.OCIFetcherClient, creds Credentials) (gcr.Image, error) {
	manifestResp, err := client.FetchManifest(ctx, &ofpb.FetchManifestRequest{
		Ref:            digestOrTagRef.String(),
		Credentials:    creds.ToProto(),
		BypassRegistry: creds.bypassRegistry,
	})
	if err != nil {
		if status.IsPermissionDeniedError(err) {
			return nil, err
		}
		return nil, status.UnavailableErrorf("could not retrieve manifest from remote: %s", err)
	}

	manifestDigest, err := gcr.NewHash(manifestResp.GetDigest())
	if err != nil {
		return nil, status.InternalErrorf("invalid digest from manifest: %s", err)
	}
	desc := gcr.Descriptor{
		Digest:    manifestDigest,
		Size:      manifestResp.GetSize(),
		MediaType: types.MediaType(manifestResp.GetMediaType()),
	}

	return imageFromDescriptorAndManifest(
		ctx,
		digestOrTagRef.Context(),
		desc,
		manifestResp.GetManifest(),
		platform,
		client,
		creds,
	)
}

// imageFromDescriptorAndManifest returns an Image from the given manifest (if the manifest is an image manifest),
// finds a child image matching the given platform (and fetches a manifest for it) if the given manifest is an index,
// and otherwise returns an error.
func imageFromDescriptorAndManifest(ctx context.Context, repo gcrname.Repository, desc gcr.Descriptor, rawManifest []byte, platform gcr.Platform, client ofpb.OCIFetcherClient, creds Credentials) (gcr.Image, error) {
	if desc.MediaType.IsSchema1() {
		return nil, status.UnknownErrorf("unsupported MediaType %q", desc.MediaType)
	}

	if desc.MediaType.IsIndex() {
		indexManifest, err := gcr.ParseIndexManifest(bytes.NewReader(rawManifest))
		if err != nil {
			return nil, status.UnknownErrorf("error parsing index manifest: %s", err)
		}

		desc, err := ocimanifest.FindFirstImageManifest(*indexManifest, platform)
		if err != nil {
			return nil, status.UnknownErrorf("Could not find child image for platform in index: %s", err)
		}
		ref := repo.Digest(desc.Digest.String())
		return fetchImageFromRemote(
			ctx,
			ref,
			platform,
			client,
			creds,
		)
	}

	return newImageFromRawManifest(
		ctx,
		repo,
		desc,
		rawManifest,
		client,
		creds,
	), nil
}

// RuntimePlatform returns the platform on which the program is being executed,
// as reported by the go runtime.
func RuntimePlatform() *rgpb.Platform {
	return &rgpb.Platform{
		Arch: runtime.GOARCH,
		Os:   runtime.GOOS,
	}
}

func newImageFromRawManifest(ctx context.Context, repo gcrname.Repository, desc gcr.Descriptor, rawManifest []byte, client ofpb.OCIFetcherClient, creds Credentials) *imageFromRawManifest {
	i := &imageFromRawManifest{
		repo:        repo,
		desc:        desc,
		rawManifest: rawManifest,
		ctx:         ctx,
		client:      client,
		creds:       creds,
	}
	i.fetchRawConfigOnce = sync.OnceValues(func() ([]byte, error) {
		manifest, err := i.Manifest()
		if err != nil {
			return nil, err
		}
		if manifest.Config.Data != nil {
			return manifest.Config.Data, nil
		}
		layer := newLayerFromDescriptor(
			i.repo,
			i,
			i.client,
			i.creds,
			manifest.Config,
		)

		rc, err := layer.Uncompressed()
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		return io.ReadAll(rc)
	})
	return i
}

var _ gcr.Image = (*imageFromRawManifest)(nil)

// imageFromRawManifest implements the go-containerregistry Image interface.
// It allows us to construct an Image from a raw manifest from either the cache
// or an upstream remote registry.
type imageFromRawManifest struct {
	repo        gcrname.Repository
	desc        gcr.Descriptor
	rawManifest []byte

	ctx    context.Context
	client ofpb.OCIFetcherClient
	creds  Credentials

	fetchRawConfigOnce func() ([]byte, error)
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

// RawConfigFile looks for the raw config file bytes
// in the rawConfigFile field, then in the manifest's Config section,
// then from the upstream registry.
func (i *imageFromRawManifest) RawConfigFile() ([]byte, error) {
	return i.fetchRawConfigOnce()
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
		layer := newLayerFromDescriptor(
			i.repo,
			i,
			i.client,
			i.creds,
			layerDesc,
		)

		layers = append(layers, layer)
	}
	return layers, nil
}

func (i *imageFromRawManifest) LayerByDigest(digest gcr.Hash) (gcr.Layer, error) {
	m, err := i.Manifest()
	if err != nil {
		return nil, err
	}
	for _, layerDesc := range m.Layers {
		if layerDesc.Digest == digest {
			return newLayerFromDescriptor(
				i.repo,
				i,
				i.client,
				i.creds,
				layerDesc,
			), nil
		}
	}
	return nil, status.NotFoundErrorf("layer with digest %s not found in manifest", digest)
}

func (i *imageFromRawManifest) LayerByDiffID(diffID gcr.Hash) (gcr.Layer, error) {
	digest, err := partial.DiffIDToBlob(i, diffID)
	if err != nil {
		return nil, err
	}
	return i.LayerByDigest(digest)
}

func newLayerFromDescriptor(repo gcrname.Repository, image *imageFromRawManifest, client ofpb.OCIFetcherClient, creds Credentials, desc gcr.Descriptor) *layerFromDescriptor {
	return &layerFromDescriptor{
		repo:   repo,
		digest: desc.Digest,
		image:  image,
		client: client,
		creds:  creds,
		desc:   desc,
	}
}

var _ gcr.Layer = (*layerFromDescriptor)(nil)

// layerFromDescriptor implements the go-containerregistry Layer interface.
// It allows us to read layers from and write layers to the cache.
type layerFromDescriptor struct {
	repo   gcrname.Repository
	digest gcr.Hash
	image  *imageFromRawManifest

	client ofpb.OCIFetcherClient
	creds  Credentials
	desc   gcr.Descriptor
}

func (l *layerFromDescriptor) Digest() (gcr.Hash, error) {
	return l.digest, nil
}

func (l *layerFromDescriptor) DiffID() (gcr.Hash, error) {
	return partial.BlobToDiffID(l.image, l.digest)
}

func (l *layerFromDescriptor) Compressed() (io.ReadCloser, error) {
	// Caching is now handled by OCIFetcherClient
	ref := l.repo.Digest(l.digest.String())
	return ocifetcher.ReadBlob(l.image.ctx, l.client, ref.String(), l.creds.ToProto(), l.creds.bypassRegistry)
}

// Uncompressed fetches the compressed bytes from the upstream server
// and returns a ReadCloser that decompresses as it reads.
func (l *layerFromDescriptor) Uncompressed() (io.ReadCloser, error) {
	cl, err := partial.CompressedToLayer(l)
	if err != nil {
		return nil, err
	}
	return cl.Uncompressed()
}

func (l *layerFromDescriptor) Size() (int64, error) {
	return l.desc.Size, nil
}

func (l *layerFromDescriptor) MediaType() (types.MediaType, error) {
	return types.DockerLayer, nil
}
