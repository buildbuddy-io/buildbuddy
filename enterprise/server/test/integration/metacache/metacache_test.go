package metacache_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/metacache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/config"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	mdserver "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/metadata"
	"github.com/buildbuddy-io/buildbuddy/server/cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/capabilities_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/mockgcs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	cspb "github.com/buildbuddy-io/buildbuddy/proto/cache_service"
	mdspb "github.com/buildbuddy-io/buildbuddy/proto/metadata_service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	instanceName      = "prod-like-metacache-test"
	maxInlineFileSize = int64(65_537)
)

type metadataCluster struct {
	nodes []*mdserver.Server
	conn  *grpc.ClientConn
}

func localAddr(t testing.TB) string {
	return fmt.Sprintf("127.0.0.1:%d", testport.FindFree(t))
}

func newMetadataConfig(t testing.TB, fs filestore.Store) *config.ServerConfig {
	id, err := uuid.NewRandom()
	require.NoError(t, err)
	httpPort := testport.FindFree(t)
	grpcPort := testport.FindFree(t)
	return &config.ServerConfig{
		NHID:              id.String(),
		RootDir:           testfs.MakeTempDir(t),
		RaftAddr:          fmt.Sprintf("127.0.0.1:%d", httpPort),
		GRPCAddr:          fmt.Sprintf("127.0.0.1:%d", grpcPort),
		GRPCListeningAddr: fmt.Sprintf("127.0.0.1:%d", grpcPort),
		LogDBConfigType:   config.SmallMemLogDBConfigType,
		FileStorer:        fs,
		Partitions: []disk.Partition{
			{
				ID:           constants.DefaultPartitionID,
				MaxSizeBytes: 256_000_000,
				NumRanges:    2,
			},
			{
				ID:           "anon",
				MaxSizeBytes: 32_000_000,
				NumRanges:    1,
			},
		},
	}
}

func waitForHealthy(t testing.TB, nodes ...*mdserver.Server) {
	require.Eventually(t, func() bool {
		for _, node := range nodes {
			if err := node.Check(context.Background()); err != nil {
				return false
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond)
}

func startMetadataCluster(t testing.TB, fs filestore.Store) *metadataCluster {
	t.Helper()
	const nodeCount = 3
	envs := make([]*testenv.TestEnv, nodeCount)
	configs := make([]*config.ServerConfig, nodeCount)
	joinList := make([]string, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		envs[i] = testenv.GetTestEnv(t)
		configs[i] = newMetadataConfig(t, fs)
		joinList = append(joinList, localAddr(t))
	}

	nodes := make([]*mdserver.Server, nodeCount)
	eg := errgroup.Group{}
	for i := 0; i < nodeCount; i++ {
		i := i
		gs, err := gossip.NewWithArgs(configs[i].NHID, joinList[i], joinList)
		require.NoError(t, err)
		configs[i].GossipManager = gs
		eg.Go(func() error {
			node, err := mdserver.New(envs[i], configs[i])
			if err != nil {
				return err
			}
			nodes[i] = node
			return nil
		})
	}
	require.NoError(t, eg.Wait())
	waitForHealthy(t, nodes...)

	mdGRPCServer, runMDServer, lis := testenv.RegisterLocalGRPCServer(t, envs[0])
	mdspb.RegisterMetadataServiceServer(mdGRPCServer, nodes[0])
	go runMDServer()
	conn, err := testenv.LocalGRPCConn(envs[0].GetServerContext(), lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	t.Cleanup(func() {
		eg := errgroup.Group{}
		for _, node := range nodes {
			node := node
			eg.Go(func() error {
				return node.Stop(context.Background())
			})
		}
		require.NoError(t, eg.Wait())
	})
	return &metadataCluster{nodes: nodes, conn: conn}
}

type remoteCacheClients struct {
	ac    repb.ActionCacheClient
	cas   repb.ContentAddressableStorageClient
	bs    bspb.ByteStreamClient
	cache cspb.CacheClient
}

func startRemoteCacheAPIs(t testing.TB, cache interfaces.Cache) remoteCacheClients {
	t.Helper()
	env := testenv.GetTestEnv(t)
	env.SetCache(cache)

	acServer, err := action_cache_server.NewActionCacheServer(env)
	require.NoError(t, err)
	bsServer, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)
	casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	require.NoError(t, err)
	capsServer := capabilities_server.NewCapabilitiesServer(env, true /*=cas*/, true /*=rbe*/, true /*=zstd*/)
	cacheServer := cache_server.New(env)

	grpcServer, runServer, lis := testenv.RegisterLocalGRPCServer(t, env)
	repb.RegisterActionCacheServer(grpcServer, acServer)
	bspb.RegisterByteStreamServer(grpcServer, bsServer)
	repb.RegisterContentAddressableStorageServer(grpcServer, casServer)
	repb.RegisterCapabilitiesServer(grpcServer, capsServer)
	cspb.RegisterCacheServer(grpcServer, cacheServer)
	go runServer()

	conn, err := testenv.LocalGRPCConn(env.GetServerContext(), lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return remoteCacheClients{
		ac:    repb.NewActionCacheClient(conn),
		cas:   repb.NewContentAddressableStorageClient(conn),
		bs:    bspb.NewByteStreamClient(conn),
		cache: cspb.NewCacheClient(conn),
	}
}

func TestRemoteCacheAPIs_Metacache_ProdLike(t *testing.T) {
	flags.Set(t, "app.log_level", "error")
	flags.Set(t, "gossip.log_level", "error")
	require.NoError(t, log.Configure())

	ctx := context.Background()
	clock := clockwork.NewRealClock()
	mockGCS := mockgcs.New(clock)
	require.NoError(t, mockGCS.SetBucketCustomTimeTTL(ctx, 30))
	fs := filestore.New(filestore.WithGCSBlobstore(mockGCS, "metacache-api-test"), filestore.WithClock(clock))
	mdCluster := startMetadataCluster(t, fs)

	mc, err := metacache.New(testenv.GetTestEnv(t), metacache.Options{
		Name:                        "prod_like_meta_cache",
		Clock:                       clock,
		MetadataClient:              mdspb.NewMetadataServiceClient(mdCluster.conn),
		FileStorer:                  fs,
		GCSTTLDays:                  30,
		MaxInlineFileSizeBytes:      maxInlineFileSize,
		MinBytesAutoZstdCompression: 100,
		PartitionMappings: []disk.PartitionMapping{
			{GroupID: interfaces.AuthAnonymousUser, Prefix: "anon/", PartitionID: "anon"},
		},
	})
	require.NoError(t, err)
	clients := startRemoteCacheAPIs(t, mc)

	smallBlob := []byte("small cache payload")
	smallDigest, err := cachetools.UploadBlob(ctx, clients.bs, instanceName, repb.DigestFunction_SHA256, bytes.NewReader(smallBlob))
	require.NoError(t, err)
	smallRN := digest.NewCASResourceName(smallDigest, instanceName, repb.DigestFunction_SHA256)
	var smallOut bytes.Buffer
	require.NoError(t, cachetools.GetBlob(ctx, clients.bs, smallRN, &smallOut))
	require.Equal(t, smallBlob, smallOut.Bytes())

	largeBlob := bytes.Repeat([]byte("larger cache payload "), 5000)
	largeDigest, err := cachetools.UploadBlob(ctx, clients.bs, instanceName, repb.DigestFunction_SHA256, bytes.NewReader(largeBlob))
	require.NoError(t, err)
	largeRN := digest.NewCASResourceName(largeDigest, instanceName, repb.DigestFunction_SHA256)
	var largeOut bytes.Buffer
	require.NoError(t, cachetools.GetBlob(ctx, clients.bs, largeRN, &largeOut))
	require.Equal(t, largeBlob, largeOut.Bytes())

	missingRsp, err := clients.cas.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		InstanceName:   instanceName,
		DigestFunction: repb.DigestFunction_SHA256,
		BlobDigests:    []*repb.Digest{smallDigest, largeDigest},
	})
	require.NoError(t, err)
	require.Empty(t, missingRsp.GetMissingBlobDigests())

	batchReadRsp, err := clients.cas.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{
		InstanceName:   instanceName,
		DigestFunction: repb.DigestFunction_SHA256,
		Digests:        []*repb.Digest{smallDigest, largeDigest},
	})
	require.NoError(t, err)
	require.Len(t, batchReadRsp.GetResponses(), 2)
	gotByHash := make(map[string][]byte)
	for _, rsp := range batchReadRsp.GetResponses() {
		require.Equal(t, int32(0), rsp.GetStatus().GetCode())
		gotByHash[rsp.GetDigest().GetHash()] = rsp.GetData()
	}
	require.Equal(t, smallBlob, gotByHash[smallDigest.GetHash()])
	require.Equal(t, largeBlob, gotByHash[largeDigest.GetHash()])

	dir := &repb.Directory{
		Files: []*repb.FileNode{{
			Name:         "large.txt",
			Digest:       largeDigest,
			IsExecutable: true,
		}},
	}
	dirDigest, err := cachetools.UploadProto(ctx, clients.bs, instanceName, repb.DigestFunction_SHA256, dir)
	require.NoError(t, err)
	treeStream, err := clients.cas.GetTree(ctx, &repb.GetTreeRequest{
		InstanceName:   instanceName,
		DigestFunction: repb.DigestFunction_SHA256,
		RootDigest:     dirDigest,
	})
	require.NoError(t, err)
	treeRsp, err := treeStream.Recv()
	require.NoError(t, err)
	require.Len(t, treeRsp.GetDirectories(), 1)
	require.True(t, proto.Equal(dir, treeRsp.GetDirectories()[0]))
	_, err = treeStream.Recv()
	require.Equal(t, io.EOF, err)

	actionDigest, err := digest.Compute(bytes.NewReader([]byte("action digest")), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	actionRN := digest.NewACResourceName(actionDigest, instanceName, repb.DigestFunction_SHA256)
	actionResult := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{{
			Path:         "large.txt",
			Digest:       largeDigest,
			IsExecutable: true,
		}},
	}
	require.NoError(t, cachetools.UploadActionResult(ctx, clients.ac, actionRN, actionResult))
	gotActionResult, err := cachetools.GetActionResult(ctx, clients.ac, actionRN)
	require.NoError(t, err)
	require.Len(t, gotActionResult.GetOutputFiles(), 1)
	require.Equal(t, largeDigest.GetHash(), gotActionResult.GetOutputFiles()[0].GetDigest().GetHash())

	cacheMetadata, err := clients.cache.GetMetadata(ctx, &capb.GetCacheMetadataRequest{
		ResourceName: digest.NewResourceName(largeDigest, instanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256).ToProto(),
	})
	require.NoError(t, err)
	require.Equal(t, largeDigest.GetSizeBytes(), cacheMetadata.GetDigestSizeBytes())
	require.Greater(t, cacheMetadata.GetStoredSizeBytes(), int64(0))

	absentDigest, err := digest.Compute(bytes.NewReader([]byte("not uploaded")), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	missingRsp, err = clients.cas.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		InstanceName:   instanceName,
		DigestFunction: repb.DigestFunction_SHA256,
		BlobDigests:    []*repb.Digest{absentDigest},
	})
	require.NoError(t, err)
	require.Len(t, missingRsp.GetMissingBlobDigests(), 1)
	require.Equal(t, absentDigest.GetHash(), missingRsp.GetMissingBlobDigests()[0].GetHash())

	absentRN := digest.NewCASResourceName(absentDigest, instanceName, repb.DigestFunction_SHA256)
	err = cachetools.GetBlob(ctx, clients.bs, absentRN, &bytes.Buffer{})
	require.True(t, status.IsFailedPreconditionError(err), "expected FailedPrecondition, got %v", err)
}
