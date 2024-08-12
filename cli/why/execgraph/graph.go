package execgraph

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/buildbuddy-io/buildbuddy/proto/spawn"
	"google.golang.org/protobuf/encoding/protodelim"
	"io"
)

type ExecGraph = *Graph[File, Spawn, string]

func ReadGraph(r *bufio.Reader) (ExecGraph, error) {
	var s spawn.SpawnExec
	g := NewGraph[Spawn](FileHash)
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

func FindRoots(g ExecGraph) []string {
	roots := g.Roots()
	var result []string
	for _, root := range roots {
		result = append(result, root.Path)
	}
	return result
}

func addSpawnExec(g ExecGraph, se *spawn.SpawnExec) error {
	if se.ExitCode != 0 {
		return nil
	}
	for _, outProto := range se.ActualOutputs {
		out := ProtoToFile(outProto)
		for _, inProto := range se.Inputs {
			in := ProtoToFile(inProto)
			s := ProtoToSpawn(se)
			err := g.AddEdge(out, in, s)
			if errors.Is(err, ErrEdgeExists) {
				existingSpawn, _ := g.Edge(out, in)
				if existingSpawn.Mnmemonic == s.Mnmemonic && s.Mnmemonic == "Javac" {
					_ = g.RemoveEdge(out, in)
					_ = g.AddEdge(out, in, s)
					err = nil
				} else {
					return fmt.Errorf("failed to add edge from %s to %s: %w", out.Path, in.Path, err)
				}
			}
			if err != nil {
				// We added the vertices above and are not disallowing cycles, so this should never happen.
				panic(fmt.Sprintf("failed to add edge from %s to %s: %s", out.Path, in.Path, err))
			}
		}
	}
	return nil
}
