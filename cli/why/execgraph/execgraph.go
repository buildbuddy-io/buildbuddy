package execgraph

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/proto/spawn"
	"google.golang.org/protobuf/encoding/protodelim"
)

type ExecGraph = *Graph[File, *Spawn, string]

var noInputsFile = File{}

func ReadGraph(r *bufio.Reader) (ExecGraph, error) {
	var s spawn.SpawnExec
	g := NewGraph[*Spawn](FileHash)
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

func VerifyCompleteness(g ExecGraph) error {
	leaves := g.Leaves()
	var missingSpawn []string
	for _, leave := range leaves {
		if !leave.IsSourceFile() {
			missingSpawn = append(missingSpawn, leave.Path)
		}
	}
	if len(missingSpawn) > 0 {
		return fmt.Errorf("missing spawns for:\n  %s\n", strings.Join(missingSpawn, "\n  "))
	}
	return nil
}

func addSpawnExec(g ExecGraph, se *spawn.SpawnExec) error {
	if se.ExitCode != 0 {
		return nil
	}
	for _, outProto := range se.ActualOutputs {
		out := ProtoToFile(outProto)
		for _, inProto := range se.Inputs {
			in := ProtoToFile(inProto)

			syntheticEdge := getSyntheticSpawn(in, se.Mnemonic, se.TargetLabel)
			if syntheticEdge != nil {
				_ = g.AddEdge(in, noInputsFile, syntheticEdge)
			}

			s := ProtoToSpawn(se)
			err := g.AddEdge(out, in, s)
			if errors.Is(err, ErrEdgeExists) {
				existingSpawn, _ := g.Edge(out, in)
				// TODO: Is this the fallback spawn?
				if existingSpawn.Mnmemonic == s.Mnmemonic && s.Mnmemonic == "Javac" {
					_ = g.RemoveEdge(out, in)
					_ = g.AddEdge(out, in, s)
					err = nil
				} else {
					return fmt.Errorf("failed to add edge from %s to %s: %w", out.Path, in.Path, err)
				}
			}
		}
	}
	return nil
}

var paramsFileRegexp = regexp.MustCompile(`.*-[0-9]+\.params`)

func getSyntheticSpawn(in File, mnemonic, label string) *Spawn {
	switch mnemonic {
	case "CppCompile":
		if strings.HasSuffix(in.Path, ".cppmap") {
			return &Spawn{Mnmemonic: "CppModuleMap", Label: label}
		}
	}
	if strings.HasSuffix(in.Path, ".params") && paramsFileRegexp.MatchString(in.Path) {
		return &Spawn{Mnmemonic: "ParameterFileWriteAction", Label: label}
	}
	return nil
}
