package cas

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func MakeTree(ctx context.Context, t testing.TB, bsClient bspb.ByteStreamClient, instanceName string, depth, branchingFactor int) (*repb.Digest, []string) {
	numFiles := int(math.Pow(float64(branchingFactor), float64(depth)))
	fileNames := make([]string, 0, numFiles)
	var leafNodes []*repb.DirectoryNode

	for d := depth; d > 0; d-- {
		numNodes := int(math.Pow(float64(branchingFactor), float64(d)))
		nextLeafNodes := make([]*repb.DirectoryNode, 0, numNodes)
		for n := 0; n < numNodes; n++ {
			subdir := &repb.Directory{}
			if d == depth {
				rn, buf := testdigest.RandomCASResourceBuf(t, 100)
				_, err := cachetools.UploadBlob(ctx, bsClient, instanceName, repb.DigestFunction_SHA256, bytes.NewReader(buf))
				require.NoError(t, err)
				fileName := fmt.Sprintf("leaf-file-%s-%d", rn.GetDigest().GetHash(), n)
				fileNames = append(fileNames, fileName)
				subdir.Files = append(subdir.Files, &repb.FileNode{
					Name:   fileName,
					Digest: rn.GetDigest(),
				})
			} else {
				start := n * branchingFactor
				end := branchingFactor + start
				subdir.Directories = append(subdir.Directories, leafNodes[start:end]...)
			}

			subdirDigest, err := cachetools.UploadProto(ctx, bsClient, instanceName, repb.DigestFunction_SHA256, subdir)
			require.NoError(t, err)
			dirName := fmt.Sprintf("node-%s-depth-%d-node-%d", subdirDigest.GetHash(), d, n)
			fileNames = append(fileNames, dirName)
			nextLeafNodes = append(nextLeafNodes, &repb.DirectoryNode{
				Name:   dirName,
				Digest: subdirDigest,
			})
		}
		leafNodes = nextLeafNodes
	}

	parentDir := &repb.Directory{
		Directories: leafNodes,
	}
	rootDigest, err := cachetools.UploadProto(ctx, bsClient, instanceName, repb.DigestFunction_SHA256, parentDir)
	require.NoError(t, err)
	return rootDigest, fileNames
}

func ReadTree(ctx context.Context, t testing.TB, casClient repb.ContentAddressableStorageClient, instanceName string, rootDigest *repb.Digest) []string {
	// Fetch the tree, and return contents.
	stream, err := casClient.GetTree(ctx, &repb.GetTreeRequest{
		InstanceName: instanceName,
		RootDigest:   rootDigest,
	})
	assert.Nil(t, err)

	names := make([]string, 0)

	for {
		rsp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		for _, dir := range rsp.GetDirectories() {
			for _, file := range dir.GetFiles() {
				names = append(names, file.GetName())
			}
			for _, subdir := range dir.GetDirectories() {
				names = append(names, subdir.GetName())
			}
		}
	}
	return names
}
