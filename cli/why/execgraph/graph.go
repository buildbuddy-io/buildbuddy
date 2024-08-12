package execgraph

import (
	"bufio"
	"io"

	"github.com/buildbuddy-io/buildbuddy/proto/spawn"
	"github.com/dominikbraun/graph"
	"google.golang.org/protobuf/encoding/protodelim"
)

type ExecGraph graph.Graph[string, File]

func ReadGraph(r *bufio.Reader) (ExecGraph, error) {
	var s spawn.SpawnExec
	g := graph.New(FileHash, graph.Directed(), graph.Acyclic())
	for {
		err := protodelim.UnmarshalFrom(r, &s)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if err = addSpawnExec(g, &s); err != nil {
			return nil, err
		}
	}
	return g, nil
}

func FindRoots(g ExecGraph) ([]File, error) {
	m, err := g.PredecessorMap()
	if err != nil {
		return nil, err
	}

	var roots []File
	for v, predecessors := range m {
		if len(predecessors) == 0 {
			roots = append(roots, v)
		}
	}

	return roots, nil
}

func addSpawnExec(g graph.Graph[string, File], s *spawn.SpawnExec) error {
	for i, outProto := range s.ActualOutputs {
		out := ProtoToFile(outProto)
		_ = g.AddVertex(out)
		for _, inProto := range s.Inputs {
			in := ProtoToFile(inProto)
			if i == 0 {
				_ = g.AddVertex(in)
			}
			if err := g.AddEdge(FileHash(out), FileHash(in), graph.EdgeData(Spawn{})); err != nil {
				return err
			}
		}
	}
	return nil
}
