package compactgraph

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunfilesTree_ComputeMapping(t *testing.T) {
	srcFile := &File{path: "pkg/file.txt"}
	genFile := &File{path: "bazel-out/k8-fastbuild/bin/pkg/file.txt"}
	externalSrcFile := &File{path: "external/repo/pkg/file.txt"}
	externalGenFile := &File{path: "bazel-out/k8-fastbuild/bin/external/repo/pkg/file.txt"}

	// See
	// https://github.com/bazelbuild/bazel/blob/release-7.4.0/src/test/java/com/google/devtools/build/lib/exec/SpawnLogContextTestBase.java
	// for the test cases that inspired the following tests.
	for _, tc := range []struct {
		name     string
		rt       RunfilesTree
		expected map[string]Input
	}{
		{
			"files collide",
			RunfilesTree{Artifacts: &InputSet{DirectEntries: []Input{srcFile, genFile}}},
			map[string]Input{
				"_main/pkg/file.txt": genFile,
			},
		},
		{
			"file collides with symlink",
			RunfilesTree{Artifacts: &InputSet{DirectEntries: []Input{srcFile}},
				Symlinks: &SymlinkEntrySet{directEntries: map[string]Input{"pkg/file.txt": genFile}}},
			map[string]Input{
				"_main/pkg/file.txt": srcFile,
			},
		},
		{
			"file collides with root symlink",
			RunfilesTree{Artifacts: &InputSet{DirectEntries: []Input{srcFile}},
				RootSymlinks: &SymlinkEntrySet{directEntries: map[string]Input{"_main/pkg/file.txt": genFile}}},
			map[string]Input{
				"_main/pkg/file.txt": genFile,
			},
		},
		{
			"symlink collides with root symlink",
			RunfilesTree{
				Symlinks:     &SymlinkEntrySet{directEntries: map[string]Input{"pkg/file.txt": srcFile}},
				RootSymlinks: &SymlinkEntrySet{directEntries: map[string]Input{"_main/pkg/file.txt": genFile}}},
			map[string]Input{
				"_main/pkg/file.txt": genFile,
			},
		},
		{
			"repo mapping manifest",
			RunfilesTree{
				RepoMappingManifest: srcFile,
				RootSymlinks:        &SymlinkEntrySet{directEntries: map[string]Input{"_repo_mapping": genFile}}},
			map[string]Input{
				"_repo_mapping": srcFile,
			},
		},
		{
			"duplicate artifact sandwiches other entry in postorder",
			RunfilesTree{
				Artifacts: &InputSet{
					DirectEntries:  []Input{srcFile, externalGenFile},
					TransitiveSets: []*InputSet{{DirectEntries: []Input{srcFile, externalGenFile, genFile, externalSrcFile}}}}},
			map[string]Input{
				"_main/pkg/file.txt": genFile,
				"repo/pkg/file.txt":  externalSrcFile,
			},
		},
		{
			"duplicate symlink entries sandwich other entry in postorder",
			RunfilesTree{
				Symlinks: &SymlinkEntrySet{
					directEntries: map[string]Input{"pkg/file.txt": srcFile},
					transitiveSets: []*SymlinkEntrySet{{
						directEntries: map[string]Input{"pkg/file.txt": genFile},
						transitiveSets: []*SymlinkEntrySet{{
							directEntries: map[string]Input{"pkg/file.txt": srcFile}}}}}}},
			map[string]Input{
				"_main/pkg/file.txt": srcFile,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.rt.Artifacts == nil {
				tc.rt.Artifacts = &InputSet{}
			}
			if tc.rt.Symlinks == nil {
				tc.rt.Symlinks = &SymlinkEntrySet{}
			}
			if tc.rt.RootSymlinks == nil {
				tc.rt.RootSymlinks = &SymlinkEntrySet{}
			}

			actual := make(map[string]Input)
			for runfilesPath, input := range tc.rt.ComputeMapping() {
				actual[runfilesPath] = input
			}
			assert.Equal(t, tc.expected, actual)
		})
	}
}
