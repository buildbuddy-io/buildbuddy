package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"

	// Register the fixture protos and their transitive imports (including
	// view.proto and its annotations) in the global descriptor registry.
	_ "github.com/buildbuddy-io/buildbuddy/tools/protoc-gen-go-vtproto-view/testdata/hello"
	_ "github.com/buildbuddy-io/buildbuddy/tools/protoc-gen-go-vtproto-view/testdata/viewtest"
)

var update = flag.Bool("update", false, "rewrite the golden files; requires running via `bazel run <this test> -- -test.run=TestGolden -update`")

// TestGolden runs the generator in-process on the registered descriptors of
// the fixture protos and compares the generated files against checked-in
// goldens. This makes emitter refactors reviewable as "generator changed,
// output identical" and keeps readable examples of the output in the tree.
// hello.proto is a minimal demonstration; viewtest.proto covers the full
// feature surface.
func TestGolden(t *testing.T) {
	for _, tc := range []struct {
		protoPath string
		golden    string
	}{
		{"tools/protoc-gen-go-vtproto-view/testdata/hello.proto", "testdata/hello_vtview.pb.go.golden"},
		{"tools/protoc-gen-go-vtproto-view/testdata/viewtest.proto", "testdata/viewtest_vtview.pb.go.golden"},
	} {
		t.Run(filepath.Base(tc.protoPath), func(t *testing.T) {
			req := codeGeneratorRequest(t, tc.protoPath)
			plugin, err := protogen.Options{}.New(req)
			require.NoError(t, err)
			require.NoError(t, generate(plugin))
			resp := plugin.Response()
			require.Nil(t, resp.Error, "generation failed: %s", resp.GetError())

			var got string
			for _, f := range resp.GetFile() {
				if strings.HasSuffix(f.GetName(), "_vtview.pb.go") {
					got = f.GetContent()
				}
			}
			require.NotEmpty(t, got, "no _vtview.pb.go file in response")

			if *update {
				ws := os.Getenv("BUILD_WORKSPACE_DIRECTORY")
				require.NotEmpty(t, ws, "-update must be invoked via bazel run so it can write to the source tree")
				dest := filepath.Join(ws, "tools/protoc-gen-go-vtproto-view", tc.golden)
				require.NoError(t, os.WriteFile(dest, []byte(got), 0644))
				t.Logf("wrote %s", dest)
				return
			}

			want, err := os.ReadFile(tc.golden)
			require.NoError(t, err, "golden file missing; regenerate with: bazel run //tools/protoc-gen-go-vtproto-view:protoc-gen-go-vtproto-view_test -- -test.run=TestGolden -update")
			require.Equal(t, string(want), got,
				"generated output changed; if intended, regenerate with: bazel run //tools/protoc-gen-go-vtproto-view:protoc-gen-go-vtproto-view_test -- -test.run=TestGolden -update")
		})
	}
}

// codeGeneratorRequest reconstructs the CodeGeneratorRequest protoc would
// send for the given file: its transitive imports in topological order, plus
// M parameters mapping BuildBuddy proto files (which rely on bazel importpath
// attributes rather than go_package options) to their Go import paths.
func codeGeneratorRequest(t *testing.T, path string) *pluginpb.CodeGeneratorRequest {
	seen := map[string]bool{}
	var files []*descriptorpb.FileDescriptorProto
	var params []string
	var add func(fd protoreflect.FileDescriptor)
	add = func(fd protoreflect.FileDescriptor) {
		if seen[fd.Path()] {
			return
		}
		seen[fd.Path()] = true
		imports := fd.Imports()
		for i := 0; i < imports.Len(); i++ {
			add(imports.Get(i).FileDescriptor)
		}
		fdp := protodesc.ToFileDescriptorProto(fd)
		if fdp.GetOptions().GetGoPackage() == "" {
			base := strings.TrimSuffix(filepath.Base(fd.Path()), ".proto")
			params = append(params, fmt.Sprintf("M%s=github.com/buildbuddy-io/buildbuddy/proto/%s", fd.Path(), base))
		}
		files = append(files, fdp)
	}
	fd, err := protoregistry.GlobalFiles.FindFileByPath(path)
	require.NoError(t, err)
	add(fd)
	return &pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{path},
		Parameter:      proto.String(strings.Join(params, ",")),
		ProtoFile:      files,
	}
}
