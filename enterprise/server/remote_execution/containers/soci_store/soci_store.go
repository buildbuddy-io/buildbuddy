package soci_store

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/awslabs/soci-snapshotter/fs/config"
	"github.com/awslabs/soci-snapshotter/fs/layer"
	"github.com/awslabs/soci-snapshotter/fs/source"
	"github.com/awslabs/soci-snapshotter/metadata"
	"github.com/awslabs/soci-snapshotter/service/keychain/local_keychain"
	"github.com/awslabs/soci-snapshotter/service/resolver"
	"github.com/awslabs/soci-snapshotter/store"
	"github.com/awslabs/soci-snapshotter/ztoc"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	socifs "github.com/awslabs/soci-snapshotter/fs"
	sspb "github.com/awslabs/soci-snapshotter/proto"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	socipb "github.com/buildbuddy-io/buildbuddy/proto/soci"
	sddaemon "github.com/coreos/go-systemd/v22/daemon"
	godigest "github.com/opencontainers/go-digest"
	bolt "go.etcd.io/bbolt"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	sociStoreRootPath = flag.String("executor.podman.soci_store_root_path", "/tmp/soci", "The root path under which to store soci-store artifacts and mount the soci-store.")
)

const (
	// Local filesystem locations for soci-store
	storeSubPath   = "store"
	indexSubPath   = "indexes"
	contentSubPath = "content"
	blobsSubPath   = "content/blobs/sha256"

	sociStoreCredCacheTtl = 1 * time.Hour
)

type Config struct {
	config.Config

	// KubeconfigKeychainConfig is config for kubeconfig-based keychain.
	KubeconfigKeychainConfig `toml:"kubeconfig_keychain"`

	// ResolverConfig is config for resolving registries.
	ResolverConfig `toml:"resolver"`
}

type KubeconfigKeychainConfig struct {
	EnableKeychain bool   `toml:"enable_keychain"`
	KubeconfigPath string `toml:"kubeconfig_path"`
}

type ResolverConfig resolver.Config

// Command-line argument to podman to enable the use of the soci store for streaming
func EnableStreamingStoreArg() string {
	return fmt.Sprintf("--storage-opt=additionallayerstore=%s:ref", StorePath())
}

func RunSociStoreWithRetries(ctx context.Context, port int) {
	for {
		log.Infof("Starting soci-store")
		err := RunSociStore(ctx, port)
		if err != nil {
			log.Errorf("soci-store returned error: %s", err)
		}
		log.Infof("Detected-soci store crash, restarting")
		metrics.PodmanSociStoreCrashes.Inc()
		// If the store crashed, the path must be unmounted to recover.
		syscall.Unmount(StorePath(), 0)

		time.Sleep(500 * time.Millisecond)
	}
}

func RunSociStore(ctx context.Context, port int) error {
	var config Config
	config.Config.RootPath = *sociStoreRootPath
	config.Config.IndexStorePath = filepath.Join(*sociStoreRootPath, indexSubPath)
	config.Config.ContentStorePath = filepath.Join(*sociStoreRootPath, contentSubPath)

	if err := disk.EnsureDirectoryExists(*sociStoreRootPath); err != nil {
		return err
	}

	credsFuncs := []resolver.Credential{local_keychain.Init(ctx, port).GetCredentials}

	// Use RegistryHosts based on ResolverConfig and keychain
	hosts := resolver.RegistryHostsFromConfig(resolver.Config(config.ResolverConfig), credsFuncs...)

	// Configure and mount filesystem
	if _, err := os.Stat(StorePath()); err != nil {
		if err2 := os.MkdirAll(StorePath(), 0755); err2 != nil && !os.IsExist(err2) {
			log.Fatalf("soci-store failed to prepare mountpoint %s: %s / %s", StorePath(), err, err2)
		}
	}
	if !config.Config.DisableVerification {
		log.Warning("soci-store content verification is not supported; switching to non-verification mode")
		config.Config.DisableVerification = true
	}
	mt, err := getMetadataStore(*sociStoreRootPath)
	if err != nil {
		log.Fatalf("soci-store failed to configure metadata store: %s", err)
	}

	// Configure filesystem and snapshotter
	var fsOpts []socifs.Option
	opq := layer.OverlayOpaqueTrusted
	fsOpts = append(fsOpts, socifs.WithGetSources(
		source.FromDefaultLabels(hosts), // provides source info based on default labels
	), socifs.WithOverlayOpaqueType(opq))
	fs, err := socifs.NewFilesystem(ctx, *sociStoreRootPath, config.Config, fsOpts...)
	if err != nil {
		log.Fatalf("failed to prepare soci-store fs: %s", err)
	}

	layerManager, err := store.NewLayerManager(ctx, *sociStoreRootPath, hosts, mt, fs, config.Config)
	if err != nil {
		log.Fatalf("failed to prepare soci-store pool: %s", err)
	}
	server, err := store.Mount(ctx, StorePath(), layerManager, config.Config.Debug)
	if err != nil {
		log.Fatalf("failed to mount soci store fs at %q: %s", StorePath(), err)
	}
	defer func() {
		if err = server.Unmount(); err != nil {
			log.Warningf("error unmounting soci-store: %s", err)
		}
		log.Info("soci-store exiting")
	}()

	if os.Getenv("NOTIFY_SOCKET") != "" {
		notified, notifyErr := sddaemon.SdNotify(false, sddaemon.SdNotifyReady)
		log.Debugf("SdNotifyReady notified=%v, err=%v", notified, notifyErr)
	}
	defer func() {
		if os.Getenv("NOTIFY_SOCKET") != "" {
			notified, notifyErr := sddaemon.SdNotify(false, sddaemon.SdNotifyStopping)
			log.Debugf("SdNotifyStopping notified=%v, err=%v", notified, notifyErr)
		}
	}()

	waitForSIGINT()
	return nil
}

func waitForSIGINT() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

func getMetadataStore(rootDir string) (metadata.Store, error) {
	bOpts := bolt.Options{
		NoFreelistSync:  true,
		InitialMmapSize: 64 * 1024 * 1024,
		FreelistType:    bolt.FreelistMapType,
	}
	db, err := bolt.Open(filepath.Join(rootDir, "metadata.db"), 0600, &bOpts)
	if err != nil {
		return nil, err
	}
	return func(sr *io.SectionReader, ztoc ztoc.TOC, opts ...metadata.Option) (metadata.Reader, error) {
		return metadata.NewReader(db, sr, ztoc, opts...)
	}, nil
}

func InitializeSociStoreKeychainClient(env environment.Env, target string) (sspb.LocalKeychainClient, error) {
	conn, err := grpc_client.DialTarget(target)
	if err != nil {
		return nil, err
	}
	log.Infof("Connecting to soci-store local keychain target: %s", target)
	env.GetHealthChecker().AddHealthCheck(
		"grpc_soci_store_keychain_connection", healthcheck.NewGRPCHealthCheck(conn))
	return sspb.NewLocalKeychainClient(conn), nil
}

func PutCredentials(ctx context.Context, image string, creds container.PullCredentials, client sspb.LocalKeychainClient) error {
	if creds.IsEmpty() {
		return nil
	}

	// The soci-store, which streams container images from the remote
	// repository and makes them available to the executor via a FUSE runs as a
	// separate process and thus does not have access to the user-provided
	// container access credentials. To address this, send the credentials to
	// the store via this gRPC service so it can cache and use them.
	//
	// TODO(iain): this runs in-process, send credentials via function call
	putCredsReq := sspb.PutCredentialsRequest{
		ImageName: image,
		Credentials: &sspb.Credentials{
			Username: creds.Username,
			Password: creds.Password,
		},
		ExpiresInSeconds: int64(sociStoreCredCacheTtl.Seconds()),
	}
	if _, err := client.PutCredentials(ctx, &putCredsReq); err != nil {
		return err
	}
	return nil
}

// Stores the SOCI artifacts referenced in the provided GetArtifactsResponse on
// the local filesystem in the place the soci-store expects to find them.
func WriteArtifacts(ctx context.Context, bsc bspb.ByteStreamClient, resp *socipb.GetArtifactsResponse) error {
	if err := disk.EnsureDirectoryExists(filepath.Join(*sociStoreRootPath, blobsSubPath)); err != nil {
		return err
	}
	if err := disk.EnsureDirectoryExists(filepath.Join(*sociStoreRootPath, indexSubPath)); err != nil {
		return err
	}

	for _, artifact := range resp.Artifacts {
		if artifact.Type == socipb.Type_UNKNOWN_TYPE {
			return status.InternalErrorf("SociArtifactStore returned unknown artifact with hash %s" + artifact.Digest.Hash)
		} else if artifact.Type == socipb.Type_SOCI_INDEX {
			// Write the index file, this is what the snapshotter uses to
			// associate the requested image with its soci index.
			sociIndexDigest := godigest.NewDigestFromEncoded(godigest.SHA256, artifact.Digest.Hash)
			imageDigest := godigest.NewDigestFromEncoded(godigest.SHA256, resp.ImageId)
			if err := os.WriteFile(getIndexPath(imageDigest), []byte(sociIndexDigest.String()), 0644); err != nil {
				return err
			}
		}
		blobFile, err := os.Create(getContentPath(artifact.Digest))
		defer blobFile.Close()
		if err != nil {
			return err
		}
		resourceName := digest.NewResourceName(artifact.Digest, "" /*=instanceName -- not used */, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
		if err = cachetools.GetBlob(ctx, bsc, resourceName, blobFile); err != nil {
			return err
		}
	}
	return nil
}

func StorePath() string {
	return filepath.Join(*sociStoreRootPath, storeSubPath)
}

func getIndexPath(digest godigest.Digest) string {
	return filepath.Join(*sociStoreRootPath, indexSubPath, digest.Encoded())
}

func getContentPath(digest *repb.Digest) string {
	return filepath.Join(*sociStoreRootPath, blobsSubPath, digest.Hash)
}
