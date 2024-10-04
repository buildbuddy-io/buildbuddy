package compactgraph

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostOrder(t *testing.T) {
	srcFile := &File{path: "pkg/file.txt"}
	genFile := &File{path: "bazel-out/k8-fastbuild/bin/pkg/file.txt"}
	externalSrcFile := &File{path: "external/repo/pkg/file.txt"}
	externalGenFile := &File{path: "bazel-out/k8-fastbuild/bin/external/repo/pkg/file.txt"}

	for _, tc := range []struct {
		name     string
		set      depset
		expected map[string]Input
	}{
		{
			"duplicate artifact sandwiches other entry in postorder",
			&InputSet{
				directEntries:  []Input{srcFile, externalGenFile},
				transitiveSets: []*InputSet{{directEntries: []Input{srcFile, externalGenFile, genFile, externalSrcFile}}}},
			map[string]Input{
				"_main/pkg/file.txt": genFile,
				"repo/pkg/file.txt":  externalSrcFile,
			},
		},
		{
			"duplicate symlink entries sandwich other entry in postorder",
			&SymlinkEntrySet{
				directEntries: map[string]Input{"pkg/file.txt": srcFile},
				transitiveSets: []*SymlinkEntrySet{{
					directEntries: map[string]Input{"pkg/file.txt": genFile},
					transitiveSets: []*SymlinkEntrySet{{
						directEntries: map[string]Input{"pkg/file.txt": srcFile},
					}}}}},
			map[string]Input{
				"pkg/file.txt": srcFile,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := make(map[string]Input)
			var filter InputFilter
			if _, ok := tc.set.(*InputSet); ok {
				filter = newDuplicateFilter()
			} else {
				filter = noFilter
			}
			for runfilesPath, input := range iterateAsRunfiles(tc.set, filter) {
				actual[runfilesPath] = input
			}
			assert.Equal(t, tc.expected, actual)
		})
	}
}
