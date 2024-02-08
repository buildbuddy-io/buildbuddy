package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaploader"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

// This tool should be run on the executor with the desired local snapshot in
// its filecache. It will upload the local snapshot to the remote cache,
// so it can be more easily inspected and run with vmstart, for example.

var (
	cacheTarget  = flag.String("cache_target", "grpcs://remote.buildbuddy.dev", "The remote cache target to upload the snapshot to.")
	filecacheDir = flag.String("file_cache_dir", "/tmp/buildbuddy/filecache", "The local filecache dir on the executor")
	apiKey       = flag.String("api_key", "", "The API key to use to interact with the remote cache.")
	// Note: this key can be copied and pasted from logs:
	//
	//  "INFO: Found snapshot for key {...}" // copy this JSON
	//
	// At the shell, paste it in single quotes: -local_snapshot_key='<paste>'
	localSnapshotKeyJSON = flag.String("local_snapshot_key", "", "JSON struct containing a local snapshot key that should be uploaded to the remote cache.")
)

type LocalSnapshotKey struct {
	GroupID      string `json:"group_id"`
	InstanceName string `json:"instance_name"`
	KeyDigest    string `json:"key_digest"`
	Key          any    `json:"key"`
}

func main() {
	flag.Parse()

	if *localSnapshotKeyJSON == "" {
		log.Fatalf("You must pass in a snapshot key")
	}
	inputKey, key, err := parseSnapshotKeyJSON(*localSnapshotKeyJSON)
	if err != nil {
		log.Fatalf("Failed to parse snapshot key: %s", err)
	}
	snapshotGroupID := inputKey.GroupID
	snapshotInstanceName := inputKey.InstanceName

	ctx := context.Background()
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	}
	env := getToolEnv()
	loader, err := snaploader.New(env)
	if err != nil {
		log.Fatalf("Failed to init snaploader: %s", err)
	}

	localManifestKey, err := snaploader.LocalManifestKey(snapshotGroupID, key)
	if err != nil {
		log.Fatalf("Failed to generate local manifest key: %s", err)
	}
	localACResult, err := loader.GetLocalManifestACResult(ctx, localManifestKey)
	if err != nil {
		log.Fatalf("Failed to get local manifest ac result: %s", err)
	}

	allSnapshotDigests := getAllDigestsFromSnapshotManifest(ctx, loader, snapshotInstanceName, localACResult)
	missingDigestsRemoteCache := getMissingDigestsRemoteCache(ctx, env, snapshotInstanceName, allSnapshotDigests)
	uploadDigestsRemoteCache(ctx, env, snapshotInstanceName, missingDigestsRemoteCache)
	uploadManifestRemoteCache(ctx, env, key, localACResult, snapshotInstanceName)

	// Sanity check that snapshot exists in remote cache
	flagutil.SetValueForFlagName("executor.enable_remote_snapshot_sharing", true, nil, false)
	_, err = loader.GetSnapshot(ctx, &fcpb.SnapshotKeySet{BranchKey: key}, true)
	if err != nil {
		log.Fatalf("Snapshot upload failed. Snapshot is not in the remote cache after it should've been uploaded.")
	}

	log.Infof("Snapshot successfully uploaded!")
}

func getToolEnv() *real_environment.RealEnv {
	healthChecker := healthcheck.NewHealthChecker("upload-local-snapshot")
	re := real_environment.NewRealEnv(healthChecker)

	// Set a large file cache size so this tool doesn't evict anything from
	// the executor's filecache
	fcSize := int64(1e18)
	fc, err := filecache.NewFileCache(*filecacheDir, fcSize, false)
	if err != nil {
		log.Fatalf("Unable to setup filecache %s", err)
	}
	fc.WaitForDirectoryScanToComplete()
	re.SetFileCache(fc)

	conn, err := grpc_client.DialSimple(*cacheTarget)
	if err != nil {
		log.Fatalf("Unable to connect to cache '%s': %s", *cacheTarget, err)
	}
	re.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	re.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	re.SetActionCacheClient(repb.NewActionCacheClient(conn))
	re.SetImageCacheAuthenticator(container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{}))
	re.SetAuthenticator(&nullauth.NullAuthenticator{})
	return re
}

func parseSnapshotKeyJSON(in string) (*LocalSnapshotKey, *fcpb.SnapshotKey, error) {
	k := &LocalSnapshotKey{}
	if err := json.Unmarshal([]byte(in), k); err != nil {
		return nil, nil, status.WrapError(err, "unmarshal snapshot key JSON")
	}
	b, err := json.Marshal(k.Key)
	if err != nil {
		return nil, nil, status.WrapError(err, "marshal SnapshotKey")
	}
	pk := &fcpb.SnapshotKey{}
	if err := protojson.Unmarshal(b, pk); err != nil {
		return nil, nil, status.WrapError(err, "unmarshal SnapshotKey")
	}
	return k, pk, nil
}

func getAllDigestsFromSnapshotManifest(ctx context.Context, loader *snaploader.FileCacheLoader, remoteInstanceName string, localACResult *repb.ActionResult) []*repb.Digest {
	digests := make([]*repb.Digest, 0)

	tmpDir, err := os.MkdirTemp("", "upload-local-ss-*")
	if err != nil {
		log.Fatalf("Error making temp dir: %s", err.Error())
	}
	defer os.RemoveAll(tmpDir)

	// Add digests for regular snapshot files
	for _, file := range localACResult.OutputFiles {
		digests = append(digests, file.GetDigest())
	}

	// Add digests for chunked snapshot files
	for _, chunkedFileMetadata := range localACResult.OutputDirectories {
		// Add digests for chunked file metadata objects
		digests = append(digests, chunkedFileMetadata.GetTreeDigest())

		// Add digests for chunked file chunks
		tree, err := loader.ChunkedFileTree(ctx, remoteInstanceName, chunkedFileMetadata, tmpDir, false /*remoteEnabled*/)
		if err != nil {
			log.Fatalf("Failed to process chunked file tree: %s", err)
		}
		for _, chunk := range tree.GetRoot().GetFiles() {
			digests = append(digests, chunk.GetDigest())
		}
	}
	return digests
}

func getMissingDigestsRemoteCache(ctx context.Context, env environment.Env, remoteInstanceName string, digests []*repb.Digest) []*repb.Digest {
	req := &repb.FindMissingBlobsRequest{
		InstanceName:   remoteInstanceName,
		BlobDigests:    digests,
		DigestFunction: repb.DigestFunction_BLAKE3,
	}
	res, err := env.GetContentAddressableStorageClient().FindMissingBlobs(ctx, req)
	if err != nil {
		log.Fatalf("Error FindMissingBlobs: %s", err.Error())
	}
	return res.MissingBlobDigests
}

func uploadDigestsRemoteCache(ctx context.Context, env environment.Env, remoteInstanceName string, digests []*repb.Digest) {
	tmpDir, err := os.MkdirTemp("", "upload-local-ss-*")
	if err != nil {
		log.Fatalf("Error making temp dir: %s", err.Error())
	}
	defer os.RemoveAll(tmpDir)

	for _, d := range digests {
		upload := func() {
			path := filepath.Join(tmpDir, d.GetHash())
			node := &repb.FileNode{
				Digest: d,
			}
			inFC := env.GetFileCache().FastLinkFile(ctx, node, path)
			if !inFC {
				log.Fatalf("Digest %s does not exist in the local file cache", d)
			}

			rn := digest.NewResourceName(d, remoteInstanceName, rspb.CacheType_CAS, repb.DigestFunction_BLAKE3)
			rn.SetCompressor(repb.Compressor_ZSTD)
			file, err := os.Open(path)
			defer file.Close()
			if err != nil {
				log.Fatalf("Error opening file %s: %s", path, err)
			}
			_, err = cachetools.UploadFromReader(ctx, env.GetByteStreamClient(), rn, file)
			if err != nil {
				log.Fatalf("Error uploading file to remote cache %s: %s", path, err)
			}
		}

		upload()
	}
}

func uploadManifestRemoteCache(ctx context.Context, env environment.Env, key *fcpb.SnapshotKey, localACResult *repb.ActionResult, remoteInstanceName string) {
	remoteManifestKey, err := snaploader.RemoteManifestKey(key)
	if err != nil {
		log.Fatalf("Error generating digest for snapshot key: %s", err)
	}
	acDigest := digest.NewResourceName(remoteManifestKey, remoteInstanceName, rspb.CacheType_AC, repb.DigestFunction_BLAKE3)
	if err := cachetools.UploadActionResult(ctx, env.GetActionCacheClient(), acDigest, localACResult); err != nil {
		log.Fatalf("Error uploading manifest to remote cache: %s", err)
	}
}
