// Package execution_graph parses and analyzes Bazel execution graph logs
// (the output of --experimental_enable_execution_graph_log; see
// proto/execution_graph.proto).
//
// The analysis computes the critical path of the invocation and the "drag" of
// nodes, edges, and factors: how much shorter the invocation's longest
// dependency chain would be if the node / edge / factor took zero time.
package execution_graph

import (
	"bufio"
	"errors"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/protobuf/encoding/protodelim"

	egpb "github.com/buildbuddy-io/buildbuddy/proto/execution_graph"
)

// Generous per-node size limit, to bound memory when parsing corrupt logs.
const maxNodeSizeBytes = 16 * 1024 * 1024

// ParseCompressedLog parses a zstd-compressed execution graph log: a stream
// of varint-length-delimited execution_graph.Node protos, as produced by
// Bazel's --experimental_enable_execution_graph_log flag.
//
// If maxNodes > 0 and the log contains more than maxNodes nodes, a
// ResourceExhausted error is returned.
func ParseCompressedLog(r io.Reader, maxNodes int) ([]*egpb.Node, error) {
	dr, err := compression.NewZstdDecompressingReader(io.NopCloser(r))
	if err != nil {
		return nil, err
	}
	defer dr.Close()
	return ParseLog(dr, maxNodes)
}

// ParseLog parses an uncompressed execution graph log.
func ParseLog(r io.Reader, maxNodes int) ([]*egpb.Node, error) {
	br := bufio.NewReader(r)
	unmarshalOpts := protodelim.UnmarshalOptions{MaxSize: maxNodeSizeBytes}
	var nodes []*egpb.Node
	for {
		node := &egpb.Node{}
		err := unmarshalOpts.UnmarshalFrom(br, node)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, status.InvalidArgumentErrorf("parse execution graph log: %s", err)
		}
		nodes = append(nodes, node)
		if maxNodes > 0 && len(nodes) > maxNodes {
			return nil, status.ResourceExhaustedErrorf("execution graph log has more than %d nodes", maxNodes)
		}
	}
	return nodes, nil
}
