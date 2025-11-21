package fetcher

import (
	"context"
	"io"
	"net"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-containerregistry/pkg/authn"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	ocifetcherpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
)

const (
	// Maximum chunk size for streaming blob responses (1MB)
	maxBlobChunkSize = 1024 * 1024
)

var (
	allowedPrivateIPs = flag.Slice("executor.container_registry_allowed_private_ips", []string{}, "Allowed private IP ranges for container registries. Private IPs are disallowed by default.")
)

type OCIFetcherServer struct {
	env               environment.Env
	allowedPrivateIPs []*net.IPNet
}

func Register(env *real_environment.RealEnv) error {
	allowedPrivateIPNets := make([]*net.IPNet, 0, len(*allowedPrivateIPs))
	for _, r := range *allowedPrivateIPs {
		_, ipNet, err := net.ParseCIDR(r)
		if err != nil {
			return status.InvalidArgumentErrorf("invalid value %q for executor.container_registry_allowed_private_ips flag: %s", r, err)
		}
		allowedPrivateIPNets = append(allowedPrivateIPNets, ipNet)
	}

	server := &OCIFetcherServer{
		env:               env,
		allowedPrivateIPs: allowedPrivateIPNets,
	}
	env.SetOCIFetcherServer(server)
	return nil
}

func (s *OCIFetcherServer) getRemoteOpts(ctx context.Context, credentials *rgpb.Credentials) []remote.Option {
	remoteOpts := []remote.Option{
		remote.WithContext(ctx),
	}

	if credentials != nil && credentials.GetUsername() != "" {
		remoteOpts = append(remoteOpts, remote.WithAuth(&authn.Basic{
			Username: credentials.GetUsername(),
			Password: credentials.GetPassword(),
		}))
	}

	tr := httpclient.New(s.allowedPrivateIPs, "oci_fetcher").Transport
	remoteOpts = append(remoteOpts, remote.WithTransport(tr))

	return remoteOpts
}

func (s *OCIFetcherServer) CanAccess(ctx context.Context, req *ocifetcherpb.CanAccessRequest) (*ocifetcherpb.CanAccessResponse, error) {
	if req.GetReference() == "" {
		return nil, status.InvalidArgumentError("reference is required")
	}

	imageRef, err := gcrname.ParseReference(req.GetReference())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image reference %q: %s", req.GetReference(), err)
	}

	log.CtxInfof(ctx, "Checking access to registry %q", imageRef.Context().RegistryStr())

	remoteOpts := s.getRemoteOpts(ctx, req.GetCredentials())
	_, err = remote.Head(imageRef, remoteOpts...)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return &ocifetcherpb.CanAccessResponse{CanAccess: false}, nil
		}
		return nil, status.UnavailableErrorf("could not check access to remote registry: %s", err)
	}

	return &ocifetcherpb.CanAccessResponse{CanAccess: true}, nil
}

func (s *OCIFetcherServer) FetchManifest(ctx context.Context, req *ocifetcherpb.FetchManifestRequest) (*ocifetcherpb.FetchManifestResponse, error) {
	if req.GetRef() == "" {
		return nil, status.InvalidArgumentError("ref is required")
	}

	imageRef, err := gcrname.ParseReference(req.GetRef())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image reference %q: %s", req.GetRef(), err)
	}

	log.CtxInfof(ctx, "Fetching manifest for %q", imageRef)

	remoteOpts := s.getRemoteOpts(ctx, req.GetCredentials())
	puller, err := remote.NewPuller(remoteOpts...)
	if err != nil {
		return nil, status.InternalErrorf("error creating puller: %s", err)
	}

	remoteDesc, err := puller.Get(ctx, imageRef)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("not authorized to retrieve image manifest: %s", err)
		}
		return nil, status.UnavailableErrorf("could not retrieve manifest from remote: %s", err)
	}

	manifestBytes, err := remoteDesc.RawManifest()
	if err != nil {
		return nil, status.InternalErrorf("could not read manifest: %s", err)
	}

	return &ocifetcherpb.FetchManifestResponse{
		Manifest: manifestBytes,
	}, nil
}

func (s *OCIFetcherServer) FetchManifestMetadata(ctx context.Context, req *ocifetcherpb.FetchManifestMetadataRequest) (*ocifetcherpb.FetchManifestMetadataResponse, error) {
	if req.GetRef() == "" {
		return nil, status.InvalidArgumentError("ref is required")
	}

	imageRef, err := gcrname.ParseReference(req.GetRef())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image reference %q: %s", req.GetRef(), err)
	}

	log.CtxInfof(ctx, "Fetching manifest metadata for %q", imageRef)

	remoteOpts := s.getRemoteOpts(ctx, req.GetCredentials())
	puller, err := remote.NewPuller(remoteOpts...)
	if err != nil {
		return nil, status.InternalErrorf("error creating puller: %s", err)
	}

	desc, err := puller.Head(ctx, imageRef)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("not authorized to access image manifest: %s", err)
		}
		return nil, status.UnavailableErrorf("could not get manifest metadata from remote: %s", err)
	}

	return &ocifetcherpb.FetchManifestMetadataResponse{
		Digest:    desc.Digest.String(),
		Size:      desc.Size,
		MediaType: string(desc.MediaType),
	}, nil
}

func (s *OCIFetcherServer) FetchBlob(req *ocifetcherpb.FetchBlobRequest, stream ocifetcherpb.OCIFetcher_FetchBlobServer) error {
	ctx := stream.Context()

	if req.GetRef() == "" {
		return status.InvalidArgumentError("ref is required")
	}

	ref, err := gcrname.ParseReference(req.GetRef())
	if err != nil {
		return status.InvalidArgumentErrorf("invalid blob reference %q: %s", req.GetRef(), err)
	}

	blobRef, ok := ref.(gcrname.Digest)
	if !ok {
		return status.InvalidArgumentErrorf("blob reference must be a digest, got %q", req.GetRef())
	}

	log.CtxInfof(ctx, "Fetching blob for %q", blobRef)

	remoteOpts := s.getRemoteOpts(ctx, req.GetCredentials())
	puller, err := remote.NewPuller(remoteOpts...)
	if err != nil {
		return status.InternalErrorf("error creating puller: %s", err)
	}

	// For blobs, we need to fetch them as layers
	layer, err := puller.Layer(ctx, blobRef)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return status.PermissionDeniedErrorf("not authorized to retrieve blob: %s", err)
		}
		return status.UnavailableErrorf("could not retrieve blob from remote: %s", err)
	}

	// Get the compressed layer data
	rc, err := layer.Compressed()
	if err != nil {
		return status.InternalErrorf("could not get compressed layer: %s", err)
	}
	defer rc.Close()

	// Stream the blob data in chunks
	buf := make([]byte, maxBlobChunkSize)
	for {
		n, err := rc.Read(buf)
		if err != nil && err != io.EOF {
			return status.InternalErrorf("error reading blob data: %s", err)
		}
		if n == 0 {
			break
		}

		resp := &ocifetcherpb.FetchBlobResponse{
			Data: buf[:n],
		}
		if err := stream.Send(resp); err != nil {
			return status.InternalErrorf("error streaming blob data: %s", err)
		}

		if err == io.EOF {
			break
		}
	}

	return nil
}

func (s *OCIFetcherServer) FetchBlobMetadata(ctx context.Context, req *ocifetcherpb.FetchBlobMetadataRequest) (*ocifetcherpb.FetchBlobMetadataResponse, error) {
	if req.GetRef() == "" {
		return nil, status.InvalidArgumentError("ref is required")
	}

	ref, err := gcrname.ParseReference(req.GetRef())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid blob reference %q: %s", req.GetRef(), err)
	}

	blobRef, ok := ref.(gcrname.Digest)
	if !ok {
		return nil, status.InvalidArgumentErrorf("blob reference must be a digest, got %q", req.GetRef())
	}

	log.CtxInfof(ctx, "Fetching blob metadata for %q", blobRef)

	remoteOpts := s.getRemoteOpts(ctx, req.GetCredentials())
	puller, err := remote.NewPuller(remoteOpts...)
	if err != nil {
		return nil, status.InternalErrorf("error creating puller: %s", err)
	}

	// Fetch the layer to get metadata
	layer, err := puller.Layer(ctx, blobRef)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("not authorized to retrieve blob: %s", err)
		}
		return nil, status.UnavailableErrorf("could not retrieve blob from remote: %s", err)
	}

	size, err := layer.Size()
	if err != nil {
		return nil, status.InternalErrorf("could not get blob size: %s", err)
	}

	mediaType, err := layer.MediaType()
	if err != nil {
		return nil, status.InternalErrorf("could not get blob media type: %s", err)
	}

	return &ocifetcherpb.FetchBlobMetadataResponse{
		SizeBytes: size,
		MediaType: string(mediaType),
	}, nil
}
