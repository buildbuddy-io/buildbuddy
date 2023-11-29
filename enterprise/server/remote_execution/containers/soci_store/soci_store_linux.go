//go:build linux && !android
// +build linux,!android

package soci_store

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"

	sspb "github.com/awslabs/soci-snapshotter/proto"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	socipb "github.com/buildbuddy-io/buildbuddy/proto/soci"
	godigest "github.com/opencontainers/go-digest"
)

var (
	imageStreamingEnabled = flag.Bool("executor.podman.enable_image_streaming", false, "If set, all public (non-authenticated) podman images are streamed using soci artifacts generated and stored in the apps.")

	// TODO(https://github.com/buildbuddy-io/buildbuddy-internal/issues/2570): remove this flag
	binary   = flag.String("executor.podman.soci_store_binary", "soci-store", "The name of the soci-store binary to run. If empty, soci-store is not started even if it's needed (for local development).")
	logLevel = flag.String("executor.podman.soci_store_log_level", "", "The level at which the soci-store should log. Should be one of the standard log levels, all lowercase.")

	artifactStoreTarget = flag.String("executor.podman.soci_artifact_store_target", "", "The GRPC url to use to access the SociArtifactStore GRPC service.")
	keychainPort        = flag.Int("executor.podman.soci_store_keychain_port", 1989, "The port on which the soci-store local keychain service is exposed, for sharing credentials for streaming private container images.")
)

const (
	// soci-store data root
	sociDataRootDirectory = "/var/lib/containers/soci"

	// The paths where soci artifact blobs are stored
	sociContentDirectory = "/var/lib/containers/soci/content"
	sociBlobDirectory    = "/var/lib/containers/soci/content/blobs/sha256/"

	// The path where soci indexes are stored
	sociIndexDirectory = "/var/lib/containers/soci/indexes/"

	// The path where the soci-store is mounted
	sociStorePath = "/var/lib/soci-store/store"

	// How long to cache credentials in the store.
	sociStoreCredCacheTtl = 1 * time.Hour
)

// A SociStore implementation that references an externally running soci-store.
// This is useful for local development if you want to do things like make the
// store crash while running, though it should go away eventually in favor of
// a patched dependency.
// TODO(iain): delete this.
type DevStore struct {
	path                string
	artifactStoreClient socipb.SociArtifactStoreClient
	keychainClient      sspb.LocalKeychainClient
}

func (s *DevStore) Ready(ctx context.Context) error {
	_, err := os.Stat(s.path)
	return err
}

func (s *DevStore) WaitUntilReady() error {
	if err := disk.WaitUntilExists(context.Background(), s.path, disk.WaitOpts{}); err != nil {
		return status.UnavailableErrorf("soci-store unavailable at %s: %s", s.path, err)
	}
	return nil
}

func (s *DevStore) GetPodmanArgs() []string {
	return []string{streamingStoreArg(s.path)}
}

func (s *DevStore) GetArtifacts(ctx context.Context, env environment.Env, image string, creds oci.Credentials) error {
	return getArtifacts(ctx, s.artifactStoreClient, env, image, creds)
}

func (s *DevStore) PutCredentials(ctx context.Context, image string, credentials oci.Credentials) error {
	return putCredentials(ctx, s.keychainClient, image, credentials)
}

// A SociStore implementation that runs a soci-store in another process.
type SociStore struct {
	path                string
	artifactStoreClient socipb.SociArtifactStoreClient
	keychainClient      sspb.LocalKeychainClient
}

func (s *SociStore) Ready(ctx context.Context) error {
	_, err := os.Stat(s.path)
	return err
}

func (s *SociStore) WaitUntilReady() error {
	if err := disk.WaitUntilExists(context.Background(), s.path, disk.WaitOpts{}); err != nil {
		return status.UnavailableErrorf("soci-store unavailable at %s: %s", s.path, err)
	}
	return nil
}

func (s *SociStore) GetPodmanArgs() []string {
	return []string{fmt.Sprintf("--storage-opt=additionallayerstore=%s:ref", s.path)}
}

func (s *SociStore) GetArtifacts(ctx context.Context, env environment.Env, image string, creds oci.Credentials) error {
	return getArtifacts(ctx, s.artifactStoreClient, env, image, creds)
}

func (s *SociStore) PutCredentials(ctx context.Context, image string, credentials oci.Credentials) error {
	return putCredentials(ctx, s.keychainClient, image, credentials)
}

// TODO(iain): write this as a temp file and pass it to the store as an arg.
func writeSociStoreConf() error {
	sociStoreConf := fmt.Sprintf(`
root_path = "%s"
content_store_path = "%s"
index_store_path = "%s"
`, sociDataRootDirectory, sociContentDirectory, sociIndexDirectory)
	if err := os.MkdirAll("/etc/soci-store", 0644); err != nil {
		return err
	}
	if err := os.WriteFile("/etc/soci-store/config.toml", []byte(sociStoreConf), 0644); err != nil {
		return status.UnavailableErrorf("could not write soci-store config: %s", err)
	}
	return nil
}

// TODO(iain): select the correct location for this based on the user.
func writeStorageConf() error {
	storageConf := `
[storage]
driver = "overlay"
runroot = "/run/containers/storage"
graphroot = "/var/lib/containers/storage"
`
	if err := os.WriteFile("/etc/containers/storage.conf", []byte(storageConf), 0644); err != nil {
		return status.UnavailableErrorf("could not write storage config: %s", err)
	}
	return nil
}

func (s *SociStore) init(ctx context.Context) error {
	if err := writeSociStoreConf(); err != nil {
		return err
	}
	return writeStorageConf()
}

func (s *SociStore) runWithRetries(ctx context.Context) {
	for {
		log.Infof("Starting soci store")
		args := []string{fmt.Sprintf("--local_keychain_port=%d", *keychainPort)}
		if *logLevel != "" {
			args = append(args, fmt.Sprintf("--log-level=%s", *logLevel))
		}
		args = append(args, s.path)
		cmd := exec.CommandContext(ctx, *binary, args...)
		logWriter := log.Writer("[socistore] ")
		cmd.Stderr = logWriter
		cmd.Stdout = logWriter
		log.Infof("mounting soci-store at %s", s.path)
		cmd.Run()

		log.Errorf("Detected soci store crash, restarting")
		metrics.PodmanSociStoreCrashes.Inc()

		// If the store crashed, the path must be unmounted to recover.
		if err := syscall.Unmount(s.path, 0); err == nil {
			log.Infof("successfully unmounted dead soci-store path, re-mounting shortly")
		} else {
			log.Errorf("error unmounting dead soci-store path, re-mounting in new location: %s", err)
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func Init(env environment.Env) (Store, error) {
	if *imageStreamingEnabled {
		artifactStoreClient, err := intializeSociArtifactStoreClient(env, *artifactStoreTarget)
		if err != nil {
			return nil, err
		}
		keychainClient, err := initializeSociStoreKeychainClient(env, fmt.Sprintf("grpc://localhost:%d", *keychainPort))
		if err != nil {
			return nil, err
		}
		var store Store
		if *binary == "" {
			store = &DevStore{
				path:                sociStorePath,
				artifactStoreClient: artifactStoreClient,
				keychainClient:      keychainClient,
			}
		} else {
			sociStore := &SociStore{
				path:                sociStorePath,
				artifactStoreClient: artifactStoreClient,
				keychainClient:      keychainClient,
			}
			if err = sociStore.init(env.GetServerContext()); err != nil {
				return nil, err
			}
			go sociStore.runWithRetries(env.GetServerContext())

			if err := sociStore.WaitUntilReady(); err != nil {
				return nil, status.UnavailableErrorf("soci-store failed to start: %s", err)
			}
			store = sociStore
		}

		// TODO(github.com/buildbuddy-io/buildbuddy-internal/issues/2282):
		// there's a bug in soci-store that causes it to panic occasionally.
		// Report the executor as unhealthy if the soci-store directory doesn'
		// exist for any reason.
		env.GetHealthChecker().AddHealthCheck(
			"soci_store", interfaces.CheckerFunc(
				func(ctx context.Context) error {
					err := store.Ready(ctx)
					if err != nil {
						return fmt.Errorf("soci-store died (stat returned: %s)", err)
					}
					return nil
				},
			),
		)

		return store, nil
	} else {
		return NoStore{}, nil
	}
}

func intializeSociArtifactStoreClient(env environment.Env, target string) (socipb.SociArtifactStoreClient, error) {
	conn, err := grpc_client.DialSimple(target)
	if err != nil {
		return nil, err
	}
	log.Infof("Connecting to app internal target: %s", target)
	env.GetHealthChecker().AddHealthCheck("grpc_soci_artifact_store_connection", conn)
	return socipb.NewSociArtifactStoreClient(conn), nil
}

func initializeSociStoreKeychainClient(env environment.Env, target string) (sspb.LocalKeychainClient, error) {
	conn, err := grpc_client.DialSimple(target)
	if err != nil {
		return nil, err
	}
	log.Infof("Connecting to soci store local keychain target: %s", target)
	env.GetHealthChecker().AddHealthCheck("grpc_soci_store_keychain_connection", conn)
	return sspb.NewLocalKeychainClient(conn), nil
}

func streamingStoreArg(path string) string {
	return fmt.Sprintf("--storage-opt=additionallayerstore=%s:ref", path)
}

func getArtifacts(ctx context.Context, client socipb.SociArtifactStoreClient, env environment.Env, image string, credentials oci.Credentials) error {
	startTime := time.Now()
	defer metrics.PodmanGetSociArtifactsLatencyUsec.
		With(prometheus.Labels{metrics.ContainerImageTag: image}).
		Observe(float64(time.Since(startTime).Microseconds()))

	ctx, err := prefix.AttachUserPrefixToContext(ctx, env)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(sociBlobDirectory, 0644); err != nil {
		return err
	}
	req := socipb.GetArtifactsRequest{
		Image:       image,
		Credentials: credentials.ToProto(),
		Platform: &rgpb.Platform{
			Arch: runtime.GOARCH,
			Os:   runtime.GOOS,
			// TODO: support CPU variants. Details here:
			// https://github.com/opencontainers/image-spec/blob/v1.0.0/image-index.md
		},
	}
	resp, err := client.GetArtifacts(ctx, &req)
	if err != nil {
		log.Infof("Error fetching soci artifacts %v", err)
		// Fall back to pulling the image without streaming.
		return nil
	}
	for _, artifact := range resp.Artifacts {
		if artifact.Type == socipb.Type_UNKNOWN_TYPE {
			return status.InternalErrorf("SociArtifactStore returned unknown artifact with hash %s" + artifact.Digest.Hash)
		} else if artifact.Type == socipb.Type_SOCI_INDEX {
			// Write the index file, this is what the snapshotter uses to
			// associate the requested image with its soci index.
			if err = os.MkdirAll(sociIndexDirectory, 0644); err != nil {
				return err
			}
			sociIndexDigest := godigest.NewDigestFromEncoded(godigest.SHA256, artifact.Digest.Hash)
			imageDigest := godigest.NewDigestFromEncoded(godigest.SHA256, resp.ImageId)
			if err = os.WriteFile(sociIndex(imageDigest), []byte(sociIndexDigest.String()), 0644); err != nil {
				return err
			}
		}
		blobFile, err := os.Create(sociBlob(artifact.Digest))
		defer blobFile.Close()
		if err != nil {
			return err
		}
		resourceName := digest.NewResourceName(artifact.Digest, "" /*=instanceName -- not used */, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
		if err = cachetools.GetBlob(ctx, env.GetByteStreamClient(), resourceName, blobFile); err != nil {
			return err
		}
	}
	return nil
}

func putCredentials(ctx context.Context, keychainClient sspb.LocalKeychainClient, image string, credentials oci.Credentials) error {
	if credentials.IsEmpty() {
		return nil
	}

	putCredsReq := sspb.PutCredentialsRequest{
		ImageName: image,
		Credentials: &sspb.Credentials{
			Username: credentials.Username,
			Password: credentials.Password,
		},
		ExpiresInSeconds: int64(sociStoreCredCacheTtl.Seconds()),
	}
	_, err := keychainClient.PutCredentials(ctx, &putCredsReq)
	return err
}

func sociIndex(digest godigest.Digest) string {
	return sociIndexDirectory + digest.Encoded()
}

func sociBlob(digest *repb.Digest) string {
	return sociBlobDirectory + digest.Hash
}
