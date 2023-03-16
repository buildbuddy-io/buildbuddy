package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_client"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/buildbuddy-io/buildbuddy/server/util/dsingleflight"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/containerd/stargz-snapshotter/estargz"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	ctrname "github.com/google/go-containerregistry/pkg/name"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	registryInstanceName = "registry_artifacts"
)

var (
	port = flag.Int("port", 8080, "The port to listen for HTTP traffic on")

	serverType            = flag.String("server_type", "image-converter", "The server type to match on health checks")
	casBackend            = flag.String("image_converter.cas_backend", "", "gRPC endpoint of the CAS used to store converted artifacts")
	imageConverterBackend = flag.String("image_converter.self_target", "", "gRPC endpoint pointing to this service (used to load balance layer conversion requests)")
)

type imageConverter struct {
	casClient repb.ContentAddressableStorageClient
	bsClient  bspb.ByteStreamClient

	deduper *dsingleflight.Coordinator
}

func getImage(ctx context.Context, image string, credentials *rgpb.Credentials) (v1.Image, error) {
	imageRef, err := ctrname.ParseReference(image)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image %q", image)
	}

	remoteOpts := []remote.Option{remote.WithContext(ctx)}
	if credentials.GetUsername() != "" || credentials.GetPassword() != "" {
		authenticator := &authn.Basic{
			Username: credentials.GetUsername(),
			Password: credentials.GetPassword(),
		}
		remoteOpts = append(remoteOpts, remote.WithAuth(authenticator))
	}

	remoteDesc, err := remote.Get(imageRef, remoteOpts...)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("could not retrieve image manifest: %s", err)
		}
		return nil, status.UnavailableErrorf("could not retrieve manifest from remote: %s", err)
	}

	switch remoteDesc.MediaType {
	case types.OCIImageIndex, types.DockerManifestList:
		return nil, status.InvalidArgumentErrorf("image index not expected in conversion request")
	case types.OCIManifestSchema1, types.DockerManifestSchema2:
		img, err := remoteDesc.Image()
		if err != nil {
			return nil, status.UnknownErrorf("could not get image from descriptor: %s", err)
		}
		return img, nil
	default:
		return nil, status.UnknownErrorf("descriptor has unknown media type %q", remoteDesc.MediaType)
	}
}

func manifestProto(img v1.Image, casDependencies []*repb.Digest) (*rgpb.Manifest, error) {
	manifestBytes, err := img.RawManifest()
	if err != nil {
		return nil, status.UnknownErrorf("could not get manifest: %s", err)
	}
	manifestMediaType, err := img.MediaType()
	if err != nil {
		return nil, status.UnknownErrorf("could not get media type: %s", err)
	}

	manifestDigest, err := img.Digest()
	if err != nil {
		return nil, status.UnknownErrorf("could not get digest: %s", err)
	}

	return &rgpb.Manifest{
		Digest:          manifestDigest.String(),
		Data:            manifestBytes,
		ContentType:     string(manifestMediaType),
		CasDependencies: casDependencies,
	}, nil
}

type convertedLayer struct {
	rsp *rgpb.ConvertLayerResponse
}

func (c *convertedLayer) Descriptor() (*v1.Descriptor, error) {
	d, err := c.Digest()
	if err != nil {
		return nil, err
	}
	as := make(map[string]string)
	for _, a := range c.rsp.GetAnnotations() {
		as[a.GetKey()] = a.GetValue()
	}
	return &v1.Descriptor{
		Size:        c.rsp.GetSize(),
		Annotations: as,
		Digest:      d,
		MediaType:   types.MediaType(c.rsp.GetMediaType()),
	}, nil
}

func (c *convertedLayer) Digest() (v1.Hash, error) {
	return v1.NewHash(c.rsp.GetDigest())
}

func (c *convertedLayer) DiffID() (v1.Hash, error) {
	return v1.NewHash(c.rsp.GetDiffId())
}

func (c *convertedLayer) Compressed() (io.ReadCloser, error) {
	return nil, status.UnimplementedError("Compressed() not supported")
}

func (c *convertedLayer) Uncompressed() (io.ReadCloser, error) {
	return nil, status.UnimplementedError("Uncompressed() not supported")
}

func (c *convertedLayer) Size() (int64, error) {
	return c.rsp.GetSize(), nil
}

func (c *convertedLayer) MediaType() (types.MediaType, error) {
	return types.MediaType(c.rsp.GetMediaType()), nil
}

func (c *imageConverter) convertImage(ctx context.Context, req *rgpb.ConvertImageRequest) (*rgpb.ConvertImageResponse, error) {
	img, err := getImage(ctx, req.GetImage(), req.GetCredentials())
	if err != nil {
		return nil, err
	}

	manifest, err := img.Manifest()
	if err != nil {
		return nil, status.UnknownErrorf("could not get image manifest: %s", err)
	}
	cnf, err := img.ConfigFile()
	if err != nil {
		return nil, status.UnknownErrorf("could not get image config; %s", err)
	}

	// This field will be populated with new layers as we do the conversion.
	// We need to clear this to remove references to the old layers.
	cnf.RootFS.DiffIDs = nil

	newImg, err := mutate.ConfigFile(empty.Image, cnf)
	if err != nil {
		return nil, status.UnknownErrorf("could not add config to image: %s", err)
	}

	eg, egCtx := errgroup.WithContext(ctx)

	var mu sync.Mutex
	convertedLayers := make([]*rgpb.ConvertLayerResponse, len(manifest.Layers))

	for layerIdx, layerDesc := range manifest.Layers {
		layerIdx := layerIdx
		layerDesc := layerDesc

		eg.Go(func() error {
			convConn, err := grpc_client.DialTarget(*imageConverterBackend)
			if err != nil {
				return status.UnavailableErrorf("could not connect to image converter: %s", err)
			}
			defer convConn.Close()

			imageConverterClient := rgpb.NewImageConverterClient(convConn)
			newLayer, err := imageConverterClient.ConvertLayer(egCtx, &rgpb.ConvertLayerRequest{
				Image:       req.GetImage(),
				Credentials: req.GetCredentials(),
				LayerDigest: layerDesc.Digest.String(),
			})
			if err != nil {
				return status.UnknownErrorf("could not convert layer %q: %s", layerDesc.Digest, err)
			}
			if newLayer.GetSkip() {
				return nil
			}

			mu.Lock()
			convertedLayers[layerIdx] = newLayer
			mu.Unlock()
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	var casDependencies []*repb.Digest
	for _, l := range convertedLayers {
		// Layer should be omitted.
		if l == nil {
			continue
		}
		newImg, err = mutate.AppendLayers(newImg, &convertedLayer{l})
		if err != nil {
			return nil, err
		}
		casDependencies = append(casDependencies, l.GetCasDigest())
	}

	newConfigBytes, err := newImg.RawConfigFile()
	if err != nil {
		return nil, status.UnknownErrorf("could not get new config file: %s", err)
	}
	configCASDigest, err := cachetools.UploadBlob(ctx, c.bsClient, registryInstanceName, repb.DigestFunction_SHA256, bytes.NewReader(newConfigBytes))
	if err != nil {
		return nil, status.UnknownErrorf("could not upload converted image config %s", err)
	}
	casDependencies = append(casDependencies, configCASDigest)

	mfProto, err := manifestProto(newImg, casDependencies)
	if err != nil {
		return nil, err
	}

	return &rgpb.ConvertImageResponse{Manifest: mfProto}, nil
}

// dedupeConvertImage waits for an existing conversion or starts a new one if no
// conversion is in progress for the given image.
func (c *imageConverter) dedupeConvertImage(ctx context.Context, req *rgpb.ConvertImageRequest) (*rgpb.ConvertImageResponse, error) {
	workKey := fmt.Sprintf("image-converter-image-%s", req.GetImage())
	rspBytes, err := c.deduper.Do(ctx, workKey, func() ([]byte, error) {
		r, err := c.convertImage(ctx, req)
		if err != nil {
			return nil, err
		}
		p, err := proto.Marshal(r)
		if err != nil {
			return nil, err
		}
		return p, nil
	})
	if err != nil {
		return nil, err
	}
	var rsp rgpb.ConvertImageResponse
	if err := proto.Unmarshal(rspBytes, &rsp); err != nil {
		return nil, err
	}
	return &rsp, nil
}

func (c *imageConverter) ConvertImage(ctx context.Context, req *rgpb.ConvertImageRequest) (*rgpb.ConvertImageResponse, error) {
	log.CtxInfof(ctx, "ConvertImage %s", req.GetImage())
	return c.dedupeConvertImage(ctx, req)
}

// isEmptyLayer checks whether the given layer does not contain any files.
func isEmptyLayer(layer v1.Layer) (bool, error) {
	d, err := layer.Digest()
	if err != nil {
		return false, status.UnknownErrorf("could not get layer digest: %s", err)
	}

	size, err := layer.Size()
	if err != nil {
		return false, status.UnknownErrorf("could not determine layer size for layer %q: %s", d, err)
	}
	// An empty tar.gz archive contains an "end of file" record, so it's not 0
	// bytes.
	if size > 100 {
		return false, nil
	}
	lr, err := layer.Uncompressed()
	if err != nil {
		return false, status.UnknownErrorf("could not create uncompressed reader for layer %q: %s", d, err)
	}
	defer lr.Close()
	r := tar.NewReader(lr)
	// If the archive is empty, r.Next should immediately return an EOF error
	// to indicate there are no files contained inside.
	_, err = r.Next()
	if err == io.EOF {
		return true, nil
	}
	if err != nil {
		return false, status.UnknownErrorf("error reading tar layer %q: %s", d, err)
	}
	return false, nil
}

func (c *imageConverter) convertLayer(ctx context.Context, req *rgpb.ConvertLayerRequest) (*rgpb.ConvertLayerResponse, error) {
	layerDigest, err := v1.NewHash(req.GetLayerDigest())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid layer digest %q: %s", req.GetLayerDigest(), err)
	}

	img, err := getImage(ctx, req.GetImage(), req.GetCredentials())
	if err != nil {
		return nil, err
	}

	l, err := img.LayerByDigest(layerDigest)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("could not get layer %q: %s", req.GetLayerDigest(), err)
	}

	// Lovely hack.
	// podman + stargzstore does not properly handle duplicate layers
	// in the same image. You would think this wouldn't happen in
	// practice, but it seems that some image generation libraries
	// can include multiple empty layers (why???) causing container
	// mounting to fail.
	// To get around this, we don't include any empty layers in the
	// converted image.
	emptyLayer, err := isEmptyLayer(l)
	if err != nil {
		return nil, err
	}
	if emptyLayer {
		return &rgpb.ConvertLayerResponse{Skip: true}, nil
	}

	remoteLayerReader, err := l.Uncompressed()
	if err != nil {
		return nil, status.UnknownErrorf("could not create uncompressed reader: %s", err)
	}

	// The estargz library requires a seekable reader so we suck it up and read
	// the layer into memory for performance.
	layerData, err := io.ReadAll(remoteLayerReader)
	if err != nil {
		return nil, status.UnknownErrorf("could not read original layer data: %s", err)
	}

	layerReader := io.NewSectionReader(bytes.NewReader(layerData), 0, int64(len(layerData)))
	newLayer, err := estargz.Build(layerReader, estargz.WithCompressionLevel(gzip.BestSpeed))
	if err != nil {
		return nil, status.UnknownErrorf("could not convert layer to estargz: %s", err)
	}
	// Read the estargz layer into memory so that we can compute the digest
	// and upload without reading from disk.
	// The original layer data should not be needed at this point, so hopefully
	// it's eligible for garbage collection.
	newLayerData, err := io.ReadAll(newLayer.ReadCloser)
	if err != nil {
		return nil, status.UnknownErrorf("could not read converted layer into memory: %s", err)
	}
	if err := newLayer.Close(); err != nil {
		return nil, status.UnknownErrorf("could not close new layer reader: %s", err)
	}
	newLayerDigest, err := digest.Compute(bytes.NewReader(newLayerData), repb.DigestFunction_SHA256)
	if err != nil {
		return nil, status.UnknownErrorf("could not compute digest of new layer: %s", err)
	}

	rn := digest.NewResourceName(newLayerDigest, registryInstanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
	casDigest, err := cachetools.UploadFromReader(ctx, c.bsClient, rn, bytes.NewReader(newLayerData))
	if err != nil {
		return nil, status.UnknownErrorf("could not upload converted layer %q: %s", newLayerDigest, err)
	}

	return &rgpb.ConvertLayerResponse{
		CasDigest: casDigest,
		Digest:    "sha256:" + newLayerDigest.GetHash(),
		DiffId:    newLayer.DiffID().String(),
		Size:      int64(len(newLayerData)),
		MediaType: string(types.DockerLayer),
		Annotations: []*rgpb.ConvertLayerResponse_Annotation{
			{Key: estargz.TOCJSONDigestAnnotation, Value: newLayer.TOCDigest().String()},
		},
	}, nil
}

// dedupeConvertLayer waits for an existing conversion or starts a new one if no
// conversion is in progress for the given layer.
func (c *imageConverter) dedupeConvertLayer(ctx context.Context, req *rgpb.ConvertLayerRequest) (*rgpb.ConvertLayerResponse, error) {
	workKey := fmt.Sprintf("image-converter-layer-%s", req.GetLayerDigest())
	rspBytes, err := c.deduper.Do(ctx, workKey, func() ([]byte, error) {
		log.CtxInfof(ctx, "Converting layer %q", req.GetLayerDigest())
		start := time.Now()
		r, err := c.convertLayer(ctx, req)
		if err != nil {
			log.CtxWarningf(ctx, "Converting layer %q failed: %s", req.GetLayerDigest(), err)
			return nil, err
		}
		if r.GetSkip() {
			log.CtxInfof(ctx, "Skipped conversion for layer %q", req.GetLayerDigest())
		} else {
			log.CtxInfof(ctx, "Converted layer %q to %q, CAS digest %q in %s", req.GetLayerDigest(), r.GetDigest(), r.GetCasDigest(), time.Since(start))
		}
		p, err := proto.Marshal(r)
		if err != nil {
			return nil, err
		}
		return p, nil
	})
	if err != nil {
		return nil, err
	}
	var rsp rgpb.ConvertLayerResponse
	if err := proto.Unmarshal(rspBytes, &rsp); err != nil {
		return nil, err
	}
	return &rsp, err
}

func (c *imageConverter) ConvertLayer(ctx context.Context, req *rgpb.ConvertLayerRequest) (*rgpb.ConvertLayerResponse, error) {
	log.CtxInfof(ctx, "ConvertLayer %s %s", req.GetImage(), req.GetLayerDigest())
	return c.dedupeConvertLayer(ctx, req)
}

func (c *imageConverter) Start(hc interfaces.HealthChecker, env environment.Env) {
	mux := http.NewServeMux()
	mux.Handle("/readyz", hc.ReadinessHandler())
	mux.Handle("/healthz", hc.LivenessHandler())

	regRegistryServices := func(server *grpc.Server, env environment.Env) {
		rgpb.RegisterImageConverterServer(server, c)
	}

	if err := grpc_server.RegisterGRPCServer(env, regRegistryServices); err != nil {
		log.Fatalf("Could not setup GRPC server: %s", err)
	}
	if err := grpc_server.RegisterGRPCSServer(env, regRegistryServices); err != nil {
		log.Fatalf("Could not setup GRPCS server: %s", err)
	}

	log.Infof("Starting HTTP server on port %d", *port)
	srv := &http.Server{
		Handler: mux,
	}
	hc.RegisterShutdownFunction(func(ctx context.Context) error {
		log.Infof("Shutting down server...")
		return srv.Shutdown(ctx)
	})

	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		log.Fatalf("could not listen on port %d: %s", *port, err)
	}

	_ = srv.Serve(lis)
}

func main() {
	if err := config.Load(); err != nil {
		log.Fatalf("Error loading config from file: %s", err)
	}

	config.ReloadOnSIGHUP()

	if err := log.Configure(); err != nil {
		fmt.Printf("Error configuring logging: %s", err)
		os.Exit(1)
	}

	healthChecker := healthcheck.NewHealthChecker(*serverType)
	env := real_environment.NewRealEnv(healthChecker)

	if err := ssl.Register(env); err != nil {
		log.Fatalf("could not configure SSL: %s", err)
	}

	conn, err := grpc_client.DialTarget(*casBackend)
	if err != nil {
		log.Fatalf("could not connect to CAS: %s", err)
	}
	casClient := repb.NewContentAddressableStorageClient(conn)
	bsClient := bspb.NewByteStreamClient(conn)

	if err := redis_client.RegisterDefault(env); err != nil {
		log.Fatalf("could not initialize redis client: %s", err)
	}

	c := &imageConverter{
		casClient: casClient,
		bsClient:  bsClient,
		deduper:   dsingleflight.New(env.GetDefaultRedisClient()),
	}
	c.Start(env.GetHealthChecker(), env)
}
