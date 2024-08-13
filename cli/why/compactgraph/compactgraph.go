package compactgraph

import (
	"bufio"
	"fmt"
	"io"
	"slices"

	"github.com/buildbuddy-io/buildbuddy/proto/spawn"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/encoding/protodelim"
)

type CompactGraph map[string]*Spawn

func ReadCompactLog(in io.Reader) (CompactGraph, error) {
	d, err := zstd.NewReader(in)
	if err != nil {
		return nil, err
	}
	defer d.Close()
	r := bufio.NewReader(d)

	var entry spawn.ExecLogEntry
	cg := make(CompactGraph)
	previousInputs := make(map[int32]Input)
	previousInputs[0] = &InputSet{}
	for {
		err := protodelim.UnmarshalFrom(r, &entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		switch entry.Type.(type) {
		case *spawn.ExecLogEntry_Invocation_:
			hashFunction := entry.GetInvocation().HashFunctionName
			if hashFunction != "SHA-256" {
				return nil, fmt.Errorf("unsupported hash function: %q", hashFunction)
			}
		case *spawn.ExecLogEntry_File_:
			file := ProtoToFile(entry.GetFile())
			previousInputs[entry.Id] = file
		case *spawn.ExecLogEntry_Directory_:
			dir := ProtoToDirectory(entry.GetDirectory())
			previousInputs[entry.Id] = dir
		case *spawn.ExecLogEntry_InputSet_:
			inputSet := ProtoToInputSet(entry.GetInputSet(), previousInputs)
			previousInputs[entry.Id] = inputSet
		case *spawn.ExecLogEntry_Spawn_:
			s, outputPaths := ProtoToSpawn(entry.GetSpawn(), previousInputs)
			if s != nil {
				for _, path := range outputPaths {
					cg[path] = s
				}
			}
		case *spawn.ExecLogEntry_UnresolvedSymlink_:
			panic(fmt.Sprintf("unresolved symlinks are unsupported, got %s --> %s", entry.GetUnresolvedSymlink().Path, entry.GetUnresolvedSymlink().TargetPath))
		}
	}
	return cg, nil
}

func Compare(a, b CompactGraph) (diags []string) {
	aPrimaryOutputs := a.primaryOutputs()
	bPrimaryOutputs := b.primaryOutputs()

	aExtraOutputs := difference(aPrimaryOutputs, bPrimaryOutputs)
	for _, path := range aExtraOutputs {
		diags = append(diags, fmt.Sprintf("A: extra output %s (%s)", path, a[path]))
	}
	bExtraOutputs := difference(bPrimaryOutputs, aPrimaryOutputs)
	for _, path := range bExtraOutputs {
		diags = append(diags, fmt.Sprintf("B: extra output %s (%s)", path, b[path]))
	}

	// commonOutputs := intersection(aPrimaryOutputs, bPrimaryOutputs)

	return
}

func (cg *CompactGraph) primaryOutputs() []string {
	var primaryOutputs []string
	for p, s := range *cg {
		if s.IsPrimaryOutput(p) {
			primaryOutputs = append(primaryOutputs, p)
		}
	}
	slices.Sort(primaryOutputs)
	return primaryOutputs
}

// difference computes the sorted slice a \ b for sorted slices a and b.
func difference(a, b []string) []string {
	var diff []string
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			diff = append(diff, a[i])
			i++
		case a[i] > b[j]:
			j++
		default:
			i++
			j++
		}
	}
	diff = append(diff, a[i:]...)
	return diff
}

// intersection computes the sorted slice a ∩ b for sorted slices a and b.
func intersection(a, b []string) []string {
	var intersection []string
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			intersection = append(intersection, a[i])
			i++
			j++
		}
	}
	return intersection
}
