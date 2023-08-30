package directory_size

import (
	"context"
	"flag"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	directorySizesEnabled = flag.Bool("cache.directory_sizes_enabled", false, "If true, enable an RPC that computes the cumulative size of directories stored in the cache.")
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

	// We haven't seen this digest before, so start tracking it.
	dsc.pendingSize[digestString] = dirDigest.GetSizeBytes()
	for _, f := range dir.GetFiles() {
		dsc.pendingSize[digestString] += f.GetDigest().GetSizeBytes()
	}
	// If this directory has no subdirectories, we're already done.
	if len(dir.GetDirectories()) == 0 {
		dsc.finish(digestString)
		return nil
	}

	dsc.pendingChildren[digestString] = make(map[string]struct{})

	for _, d := range dir.GetDirectories() {
		subdirDigest := digest.String(d.GetDigest())
		// If we've already found the child directory's size, count it
		// and move on.
		if subDirTotal, ok := dsc.totalSize[subdirDigest]; ok {
			dsc.pendingSize[digestString] += subDirTotal
			continue
		}

		// Otherwise, add dir to the subdir's parents, and add subdir to the
		// parent's children.  Note that we will add a parent more than once
		// so that if it has two subdirectories with identical contents, we
		// will count that subdirectory twice.
		if _, ok := dsc.parents[subdirDigest]; !ok {
			dsc.parents[subdirDigest] = make([]string, 0)
		}
		dsc.pendingChildren[digestString][subdirDigest] = struct{}{}
		dsc.parents[subdirDigest] = append(dsc.parents[subdirDigest], digestString)
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
	// Okay, this could be more efficient and more confusing, but since we
	// want to count identical subdirectories twice, we just iterate twice.
	// I don't know, if we wanted, we could just make parents hold an integer
	// instead of being a straight set.  Whatever.
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
func (dsc *directorySizeCounter) GetOutput() []*capb.DigestWithTotalSize {
	out := make([]*capb.DigestWithTotalSize, 0, len(dsc.totalSize))
	for k, v := range dsc.totalSize {
		out = append(out, &capb.DigestWithTotalSize{Digest: k, TotalSize: v})
	}
	return out
}

func GetTreeDirectorySizes(ctx context.Context, env environment.Env, req *capb.GetTreeDirectorySizesRequest) (*capb.GetTreeDirectorySizesResponse, error) {
	if !*directorySizesEnabled {
		return &capb.GetTreeDirectorySizesResponse{}, nil
	}

	casClient := env.GetContentAddressableStorageClient()
	if casClient == nil {
		return nil, status.UnimplementedError("Directory tree size computation requires a connection to a CAS server.")
	}
	nextPageToken := ""
	dsc := NewDirectorySizeCounter(req.GetDigestFunction())
	digestFunction := req.GetDigestFunction()
	if digestFunction == repb.DigestFunction_UNKNOWN {
		digestFunction = digest.InferOldStyleDigestFunctionInDesperation(req.GetRootDigest())
	}
	for {
		stream, err := casClient.GetTree(ctx, &repb.GetTreeRequest{
			RootDigest:     req.GetRootDigest(),
			InstanceName:   req.GetInstanceName(),
			PageToken:      nextPageToken,
			DigestFunction: digestFunction,
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

	return &capb.GetTreeDirectorySizesResponse{
		Sizes: dsc.GetOutput(),
	}, nil
}
