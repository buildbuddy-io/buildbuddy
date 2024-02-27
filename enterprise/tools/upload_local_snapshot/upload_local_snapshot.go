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
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/protobuf/encoding/protojson"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

// This tool should be run on the executor pod with the desired local snapshot in
// its filecache. It will upload the local snapshot to the remote cache,
// so it can be more easily inspected and run with vmstart, for example.
//
// Steps:
// 1. SSH into the executor pod the local snapshot is on.
//		a. From the action details page of the invocation link, find the executor host ID
// 		b. Search for the executor host ID in the `Executors` tab
//			- Likely will be under the "Shared Executors" group
//		c. Grab the IP address of the executor
// 		d. Find the pod name associated with that IP address
//			- kubectl --namespace executor-prod get pods -o wide
//			- Search for the IP
// 		e. SSH into the pod
//			-  kubectl --namespace=executor-prod exec -it executor-XXX /bin/bash
//
// 2. Clone the buildbuddy repo on the executor so you can use this tool
//		a. git clone https://github.com/buildbuddy-io/buildbuddy.git
//
// 3. Download bazel to run this tool
//		a. curl -Lo /usr/local/bin/bazelisk https://github.com/bazelbuild/bazelisk/releases/download/v1.15.0/bazelisk-linux-amd64
//		b. chmod +x /usr/local/bin/bazelisk
//
// 4. Find the snapshot key JSON
//		a. From the action  details page of the invocation link, find the snapshot ID
//		b. Search for that in the logs. The logs should include the snapshot key JSON
//
// 5. Run this tool with bazel
//		* You may need to build it with remote execution if the executor does not
//		  have all the tools needed to build it
//
// Ex. bazelisk run //enterprise/tools/upload_local_snapshot \
//		--config=remote \
//		--remote_header=x-buildbuddy-api-key=XXX -- \    # This api key is for remote execution of the build
//		--file_cache_dir="/buildbuddy/filecache/<executor_host_id>" \
//		--api_key=YYY \		# This api key should correspond to the group that owns the snapshot
//		--local_snapshot_key='XXX'
//
// ** Another option to run this tool (that reduces workload on the executor)
// is to build the tool on a Linux machine with `--config=static` and use
// `kubectl cp --namespace=executor-prod \
// bazel-bin/enterprise/tools/upload_local_snapshot/upload_local_snapshot_/upload_local_snapshot \
// executor-XXX:/tmp/upload_local_snapshot` to copy it to the executor

var (
	cacheTarget  = flag.String("cache_target", "grpcs://remote.buildbuddy.io", "The remote cache target to upload the snapshot to.")
	filecacheDir = flag.String("file_cache_dir", "/buildbuddy/filecache", "The local filecache dir on the executor")
	apiKey       = flag.String("api_key", "", "The API key of the owner of the snapshot. The snapshot will also be uploaded under this API key.")
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

	env := getToolEnv()
	loader, err := snaploader.New(env)
	if err != nil {
		log.Fatalf("Failed to init snaploader: %s", err)
	}
	ctx := context.Background()
	ctxWithHackyClaims := ctx
	if *apiKey != "" {
		ctx = context.WithValue(ctx, "x-buildbuddy-api-key", *apiKey)

		// The filecache reads the groupID from the claims on the context,
		// so set that here. However this isn't a valid Claims object, so for
		// API calls to the remote cache, you will not be able to use this context
		ctxWithHackyClaims = claims.AuthContextFromClaims(ctx, &claims.Claims{
			GroupID: snapshotGroupID,
		}, nil)
	}

	localManifestKey, err := snaploader.LocalManifestKey(snapshotGroupID, key)
	if err != nil {
		log.Fatalf("Failed to generate local manifest key: %s", err)
	}
	localACResult, err := loader.GetLocalManifestACResult(ctxWithHackyClaims, localManifestKey)
	if err != nil {
		log.Fatalf("Failed to get local manifest ac result: %s", err)
	}

	allSnapshotDigests := getAllDigestsFromSnapshotManifest(ctxWithHackyClaims, env, loader, snapshotInstanceName, localACResult)
	missingDigestsRemoteCache := getMissingDigestsRemoteCache(ctx, env, snapshotInstanceName, allSnapshotDigests)
	uploadDigestsRemoteCache(ctx, ctxWithHackyClaims, env, snapshotInstanceName, missingDigestsRemoteCache)
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

func getAllDigestsFromSnapshotManifest(ctxWithClaims context.Context, env environment.Env, loader *snaploader.FileCacheLoader, remoteInstanceName string, localACResult *repb.ActionResult) []*repb.Digest {
	tmpDir := env.GetFileCache().TempDir()

	digests := make([]*repb.Digest, 0)
	// Add digests for regular snapshot files
	for _, file := range localACResult.OutputFiles {
		digests = append(digests, file.GetDigest())
	}

	// Add digests for chunked snapshot files
	for _, chunkedFileMetadata := range localACResult.OutputDirectories {
		// Add digests for chunked file metadata objects
		digests = append(digests, chunkedFileMetadata.GetTreeDigest())

		// Add digests for chunked file chunks
		tree, err := loader.ChunkedFileTree(ctxWithClaims, remoteInstanceName, chunkedFileMetadata, tmpDir, false /*remoteEnabled*/)
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

func uploadDigestsRemoteCache(ctx context.Context, ctxWithClaims context.Context, env environment.Env, remoteInstanceName string, digests []*repb.Digest) {
	tmpDir := env.GetFileCache().TempDir()

	for _, d := range digests {
		upload := func() {
			path := filepath.Join(tmpDir, d.GetHash())
			node := &repb.FileNode{
				Digest: d,
			}
			inFC := env.GetFileCache().FastLinkFile(ctxWithClaims, node, path)
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
