package compactgraph

import "testing"

func TestPostOrder(t *testing.T) {
	srcFile := &File{path: "pkg/file.txt"}
	genFile := &File{path: "bazel-out/k8-fastbuild/bin/pkg/file.txt"}
	externalSrcFile := &File{path: "external/repo/pkg/file.txt"}
	externalGenFile := &File{path: "bazel-out/k8-fastbuild/bin/external/repo/pkg/file.txt"}

	set := &InputSet{
		directEntries:  []Input{srcFile, externalGenFile},
		transitiveSets: []*InputSet{{directEntries: []Input{genFile, externalSrcFile, srcFile, externalGenFile}}},
	}
	m := make(map[string]Input)
	for runfilesPath, input := range runfilesMapping(set) {
		m[runfilesPath] = input
	}
}
