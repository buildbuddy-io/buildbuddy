package directory_size

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	directorySizesEnabled = flag.Bool("cache.directory_sizes_enabled", true, "If true, enable user-owned API keys.")
)

// A helper object to tally up the size total size (including contents) of each
// directory in a tree using a streamed GetTree response.  Simply add the
// individual directories via Add() and then call GetOutput().
type directorySizeCounter struct {
	// A map from digest to digests that are waiting for this digest to be done.
	// When this digest is removed from waitingOn, it should bubble up and
	// compute sizes for all entries in this set.  If a parent technically
	// contains the same child digest twice (e.g., two empty directories), it
	// will appear in this list twice so that we properly count its total size.
	parents map[string][]string
	// A map from digest to its pending children.  When this set is empty, we
	// know the real size of the directory and we should bubble up the directory
	// size to the digests in parents.
	pendingChildren map[string]map[string]struct{}
	// The pending size of the directory node with the specified digest.  When the
	// set in childrenPending is empty, this value will be the computed total size
	// of the directory and be moved over to totalSize
	pendingSize map[string]int64
	// The total size of the directory node with the specified digest.  When the
	// set in childrenPending is empty, this value will be the computed total size
	// of the directory.
	totalSize map[string]int64

	digestFunction repb.DigestFunction_Value
}

func NewDirectorySizeCounter(digestFunction repb.DigestFunction_Value) *directorySizeCounter {
	return &directorySizeCounter{
		parents:         make(map[string][]string),
		pendingChildren: make(map[string]map[string]struct{}),
		pendingSize:     make(map[string]int64),
		totalSize:       make(map[string]int64),
		digestFunction:  digestFunction,
	}
}

func (dsc *directorySizeCounter) Add(dir *repb.Directory) error {
	dirDigest, err := digest.ComputeForMessage(dir, dsc.digestFunction)
	if err != nil {
		return err
	}
	digestString := digest.String(dirDigest)
	if _, ok := dsc.totalSize[digestString]; ok {
		// We're already in the process of computing this directory's size.
		return nil
	}
	if _, ok := dsc.pendingSize[digestString]; ok {
		// We're already in the process of computing this directory's size.
		return nil
	}

	// Create..
	dsc.pendingSize[digestString] = dirDigest.GetSizeBytes()
	for _, f := range dir.GetFiles() {
		dsc.pendingSize[digestString] += f.GetDigest().GetSizeBytes()
	}
	if len(dir.GetDirectories()) == 0 {
		dsc.finish(digestString)
		return nil
	}

	dsc.pendingChildren[digestString] = make(map[string]struct{})

	for _, d := range dir.GetDirectories() {
		dh := digest.String(d.GetDigest())
		if subDirTotal, ok := dsc.totalSize[dh]; ok {
			dsc.pendingSize[digestString] += subDirTotal
			continue
		}
		if _, ok := dsc.parents[dh]; !ok {
			dsc.parents[dh] = make([]string, 0)
		}
		dsc.pendingChildren[digestString][dh] = struct{}{}
		dsc.parents[dh] = append(dsc.parents[dh], digestString)
	}

	if len(dsc.pendingChildren[digestString]) == 0 {
		dsc.finish(digestString)
	}
	return nil
}

// Completes the computation of a subtree's size and recursively computes the
// size of any parent directories that are now also fully known.
func (dsc *directorySizeCounter) finish(digest string) {
	if _, ok := dsc.totalSize[digest]; ok {
		return
	}

	total := dsc.pendingSize[digest]
	dsc.totalSize[digest] = total
	for _, parent := range dsc.parents[digest] {
		dsc.pendingSize[parent] += total
	}
	for _, parent := range dsc.parents[digest] {
		delete(dsc.pendingChildren[parent], digest)
		if len(dsc.pendingChildren[parent]) == 0 {
			dsc.finish(parent)
		}
	}
	delete(dsc.parents, digest)
	delete(dsc.pendingChildren, digest)
	delete(dsc.pendingSize, digest)
}

// Outputs only completed branches--if for some reason we couldn't find one of
// the directories in the directory tree, we don't output a total size for any
// of its parents because the size would be incorrect.
func (dsc *directorySizeCounter) GetOutput() map[string]int64 {
	return dsc.totalSize
}

func GetTreeDirectorySizes(ctx context.Context, env environment.Env, req *capb.GetTreeDirectorySizesRequest) (*capb.GetTreeDirectorySizesResponse, error) {
	if !*directorySizesEnabled {
		return &capb.GetTreeDirectorySizesResponse{}, nil
	}
	// XXX: Validate access? Is group ID getting added to cache request implicitly good enough?
	r := req.GetResourceName()

	conn, err := grpc_client.DialTarget(fmt.Sprintf("grpc://localhost:%d", grpc_server.Port()))
	casClient := repb.NewContentAddressableStorageClient(conn)
	if err != nil {
		return nil, status.InternalErrorf("Error initializing ByteStreamClient: %s", err)
	}

	// Fetch the full tree.
	nextPageToken := ""
	dsc := NewDirectorySizeCounter(r.GetDigestFunction())
	for {
		stream, err := casClient.GetTree(ctx, &repb.GetTreeRequest{
			RootDigest:     r.GetDigest(),
			InstanceName:   r.GetInstanceName(),
			PageToken:      nextPageToken,
			DigestFunction: r.GetDigestFunction(),
		})

		if err != nil {
			return nil, err
		}
		for {
			rsp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			for _, directory := range rsp.GetDirectories() {
				dsc.Add(directory)
			}
		}
		if nextPageToken == "" {
			break
		}
	}

	dsc.GetOutput()

	return &capb.GetTreeDirectorySizesResponse{
		Sizes: dsc.GetOutput(),
	}, nil
}
