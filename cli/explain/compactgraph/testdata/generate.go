package main

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/otiai10/copy"
	"golang.org/x/tools/txtar"
)

const JavaProject = `
-- MODULE.bazel --
-- src/main/java/com/example/lib/BUILD.bazel --
java_library(
    name = "lib",
    srcs = ["Lib.java"],
    visibility = ["//visibility:public"],
)
-- src/main/java/com/example/lib/Lib.java --
package com.example.lib;

public class Lib {
    public static String getName() {
      return "Lib";
    }
}
-- src/test/java/com/example/lib/BUILD.bazel --
java_test(
    name = "lib_test",
    srcs = ["LibTest.java"],
    test_class = "com.example.lib.LibTest",
    deps = ["//src/main/java/com/example/lib"],
)
-- src/test/java/com/example/lib/LibTest.java --
package com.example.lib;

import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class LibTest {
    @Test
    public void testGetName() {
        assertTrue(Lib.getName().startsWith("Lib"));
    }
}
-- src/main/java/com/example/app/BUILD.bazel --
java_library(
    name = "app_lib",
    srcs = ["App.java"],
    deps = ["//src/main/java/com/example/lib"],
)

java_binary(
    name = "app",
    runtime_deps = [":app_lib"],
)
-- src/main/java/com/example/app/App.java --
package com.example.app;

public class App {
    public static void main(String[] args) {
        System.out.println("Hello, " + com.example.lib.Lib.getName() + "!");
    }
}
`

const TreeArtifactProject = `
-- MODULE.bazel --
-- in/BUILD --
load("//rules:defs.bzl", "tree_artifact_input")
tree_artifact_input(
	name = "tree_artifact",
	src = "//out:tree_artifact",
)
-- rules/BUILD --
-- rules/defs.bzl --
def _tree_artifact_output_impl(ctx):
    out = ctx.actions.declare_directory(ctx.attr.name)
    write_file = """
cat > $1/{file} <<'EOF'
{content}
EOF
"""
    command = "".join([
        write_file.format(file = k, content = v)
        for k, v in ctx.attr.files.items()
    ])
    ctx.actions.run_shell(
        outputs = [out],
        command = command,
        arguments = [out.path],
    )
    return [DefaultInfo(files = depset([out]))]

tree_artifact_output = rule(
    _tree_artifact_output_impl,
    attrs = {
        "files": attr.string_dict(),
    },
)

def _tree_artifact_input_impl(ctx):
    out = ctx.actions.declare_file(ctx.attr.name)
    args = ctx.actions.args()
    args.add(out)
    args.add_all(ctx.attr.src[DefaultInfo].files)
    ctx.actions.run_shell(
        inputs = ctx.attr.src[DefaultInfo].files,
        outputs = [out],
        arguments = [args],
        command = """cat "${@:1}" > $1""",
    )
    return [DefaultInfo(files = depset([out]))]

tree_artifact_input = rule(
    _tree_artifact_input_impl,
    attrs = {
        "src": attr.label(),
    },
)
`

const ToolRunfilesProject = `
-- MODULE.bazel --
-- tools/BUILD --
genrule(
    name = "tool_sh",
    outs = ["tool.sh"],
    executable = True,
	cmd = """
cat > $@ <<'EOF'
#!/bin/bash
echo "Tool"
EOF
""",
    visibility = ["//visibility:public"],
    # Avoid building this target in the non-exec configuration.
    tags = ["manual"],
)
-- pkg/constants.bzl --
DATA = ["file1.txt", "file2.txt"]
-- pkg/BUILD --
load(":constants.bzl", "DATA")

sh_binary(
    name = "tool",
    srcs = ["//tools:tool_sh"],
    data = DATA,
    # Avoid building this target in the non-exec configuration.
    tags = ["manual"],
)
genrule(
    name = "gen1",
    outs = ["out1"],
    cmd = "$(location :tool) > $@",
    tools = [":tool"],
)
genrule(
    name = "gen2",
    outs = ["out2"],
    cmd = """
$(location :tool) > $@
$(location :tool) >> $@
""",
    tools = [":tool"],
)
-- pkg/tool.sh --
#!/bin/bash
cat "$@"
-- pkg/file1.txt --
old
-- pkg/file2.txt --
unchanged
`

const ToolRunfilesSymlinksProject = `
-- MODULE.bazel --
-- rules/BUILD --
-- rules/defs.bzl --
def _runfiles_symlinks_impl(ctx):
    runfiles = ctx.runfiles(
        symlinks = {
            path: target[DefaultInfo].files.to_list()[0]
            for target, path in ctx.attr.symlinks.items()
        },
        root_symlinks = {
            path: target[DefaultInfo].files.to_list()[0]
            for target, path in ctx.attr.root_symlinks.items()
        },
    )
    return [DefaultInfo(files = depset(), runfiles = runfiles)]

runfiles_symlinks = rule(
    _runfiles_symlinks_impl,
    attrs = {
        "symlinks": attr.label_keyed_string_dict(
			allow_files = True,
        ),
        "root_symlinks": attr.label_keyed_string_dict(
			allow_files = True,
		),
    },
)
-- pkg/BUILD --
genrule(
	name = "gen",
	outs = ["out"],
	cmd = "$(location //tools:tool) > $@",
	tools = ["//tools:tool"],
)
-- tools/tool.sh --
#!/bin/bash
echo "Tool"
`

func main() {
	bazelisk, err := runfiles.Rlocation(os.Getenv("BAZELISK"))
	if err != nil {
		log.Fatalf("Failed to find bazelisk: %s", err)
	}

	buildWorkspaceDirectory := os.Getenv("BUILD_WORKSPACE_DIRECTORY")
	if buildWorkspaceDirectory == "" {
		log.Fatalf("BUILD_WORKSPACE_DIRECTORY environment variable must be set, run with `bazel run`")
	}
	outDir := filepath.Join(buildWorkspaceDirectory, filepath.FromSlash("cli/explain/compactgraph/testdata"))
	var toGenerate map[string]bool
	if len(os.Args) > 1 {
		toGenerate = make(map[string]bool)
		for _, arg := range os.Args[1:] {
			toGenerate[arg] = true
		}
	} else {
		logs, err := filepath.Glob(filepath.Join(outDir, "*/*.pb.zstd"))
		if err != nil {
			log.Fatalf("Failed to glob logs: %s", err)
		}
		for _, l := range logs {
			if err := os.Remove(l); err != nil {
				log.Fatalf("Failed to remove log: %s", err)
			}
		}
	}
	for _, tc := range []struct {
		name          string
		baseline      string
		baselineArgs  []string
		changes       string
		changedArgs   []string
		bazelVersions []string
	}{
		{
			name:     "java_noop_impl_change",
			baseline: JavaProject,
			changes: `
-- src/main/java/com/example/lib/Lib.java --
package com.example.lib;

public class Lib {
    public static String getName() {
      return "Lib"; // no-op impl change
    }
}
`,
			bazelVersions: []string{"7.3.1", "8.0.0"},
		},
		{
			name:     "java_impl_change",
			baseline: JavaProject,
			changes: `
-- src/main/java/com/example/lib/Lib.java --
package com.example.lib;

public class Lib {
    public static String getName() {
      return "Lib2"; // impl change
    }
}
`,
			bazelVersions: []string{"7.3.1", "8.0.0"},
		},
		{
			name:     "java_header_change",
			baseline: JavaProject,
			changes: `
-- src/main/java/com/example/lib/Lib.java --
package com.example.lib;

public class Lib {
    public static String getName() {
      return "Lib";
    }

    public static void foo() {}
}
`,
			bazelVersions: []string{"7.3.1", "8.0.0"},
		},
		{
			name: "env_change",
			baseline: `
-- MODULE.bazel --
-- pkg/BUILD --
genrule(
    name = "gen",
    outs = ["out"],
    cmd = "env > $@",
)
`,
			baselineArgs:  []string{"--action_env=EXTRA=foo", "--action_env=OLD_ONLY=old_only", "--action_env=OLD_AND_NEW=old"},
			changedArgs:   []string{"--action_env=NEW_ONLY=new_only", "--action_env=OLD_AND_NEW=new", "--action_env=EXTRA=foo"},
			bazelVersions: []string{"7.3.1", "8.0.0"},
		},
		{
			name: "non_hermetic",
			baseline: `
-- MODULE.bazel --
-- pkg/BUILD --
genrule(
    name = "gen",
    outs = ["out"],
    cmd = "uuidgen > $@",
)
`,
			bazelVersions: []string{"7.3.1"},
		},
		{
			name: "symlinks",
			baseline: `
-- MODULE.bazel --
bazel_dep(name = "bazel_skylib", version = "1.6.1")
-- pkg/BUILD --
load("@bazel_skylib//rules:copy_file.bzl", "copy_file")
copy_file(
    name = "first_symlink",
    src = "file",
    out = "symlink",
    allow_symlink = True,
)
copy_file(
    name = "second_symlink",
    src = "symlink",
    out = "symlink2",
    allow_symlink = True,
)
copy_file(
    name = "copy",
    src = "symlink2",
    out = "out",
)
-- pkg/file --
foo
`,
			changes: `
-- pkg/file --
not_foo
`,
			bazelVersions: []string{"8.0.0"},
		},
		{
			name: "target_renamed",
			baseline: `
-- MODULE.bazel --
-- pkg/BUILD --
genrule(
    name = "gen1",
    outs = ["out1"],
    cmd = "echo foo > $@",
)
genrule(
    name = "gen2",
    outs = ["out2"],
    srcs = [":gen1"],
    cmd = "cat $< > $@",
)
`,
			changes: `
-- pkg/BUILD --
genrule(
    name = "gen3",
    outs = ["out3"],
    cmd = "echo foo > $@",
)
genrule(
    name = "gen4",
    outs = ["out4"],
    srcs = [":gen3"],
    cmd = "cat $< > $@",
)
`,
			bazelVersions: []string{"7.3.1"},
		},
		{
			name: "flaky_test",
			baseline: `
-- MODULE.bazel --
-- pkg/BUILD --
sh_test(
    name = "flaky_test",
    srcs = ["flaky_test.sh"],
)
-- pkg/flaky_test.sh --
if [[ -e /tmp/bb_explain_flaky_test ]]; then
  rm /tmp/bb_explain_flaky_test
  echo "Flaky test failed"
  exit 1
else
  touch /tmp/bb_explain_flaky_test
  echo "Flaky test passed"
fi
`,
			// Ensure that /tmp is not hermetic.
			baselineArgs:  []string{"--sandbox_add_mount_pair=/tmp"},
			changedArgs:   []string{"--sandbox_add_mount_pair=/tmp"},
			bazelVersions: []string{"7.3.1"},
		},
		{
			name: "multiple_outputs",
			baseline: `
-- MODULE.bazel --
-- pkg/BUILD --
genrule(
    name = "gen1",
    srcs = ["ina", "inb", "inc"],
    outs = ["outa", "outb", "outc"],
    cmd = """
cp $(location ina) $(location outa)
cp $(location inb) $(location outb)
cp $(location inc) $(location outc)
""",
)
genrule(
    name = "gen2",
    srcs = [":gen1"],
    outs = ["outd"],
	cmd = "cat $(SRCS) > $@",
)
-- pkg/ina --
a
-- pkg/inb --
b
-- pkg/inc --
c
`,
			changes: `
-- pkg/inb --
b2
-- pkg/inc --
c2
`,
			bazelVersions: []string{"7.3.1"},
		},
		{
			name: "source_directory",
			baseline: `
-- MODULE.bazel --
-- pkg/BUILD --
genrule(
	name = "gen",
	srcs = ["src_dir"],
	outs = ["out"],
	cmd = "cat $</*.txt > $@",
)
-- pkg/src_dir/file1.txt --
old
-- pkg/src_dir/file2.txt --
unchanged
`,
			changes: `
-- pkg/src_dir/file1.txt --
new
-- pkg/src_dir/file3.txt --
new
`,
			bazelVersions: []string{"7.3.1"},
		},
		{
			name: "tree_artifact_paths",
			baseline: TreeArtifactProject + `
-- out/BUILD --
load("//rules:defs.bzl", "tree_artifact_output")
tree_artifact_output(
	name = "tree_artifact",
	files = {
		"file1.txt": "old_only",
		"file2.txt": "unchanged",
	},
	visibility = ["//visibility:public"],
)
`,
			changes: `
-- out/BUILD --
load("//rules:defs.bzl", "tree_artifact_output")
tree_artifact_output(
	name = "tree_artifact",
	files = {
		"file2.txt": "unchanged",
		"file3.txt": "new_only",
	},
	visibility = ["//visibility:public"],
)
`,
			bazelVersions: []string{"7.3.1"},
		},
		{
			name: "tree_artifact_contents",
			baseline: TreeArtifactProject + `
-- out/BUILD --
load("//rules:defs.bzl", "tree_artifact_output")
tree_artifact_output(
	name = "tree_artifact",
	files = {
		"file1.txt": "old",
		"file2.txt": "unchanged",
	},
	visibility = ["//visibility:public"],
)
`,
			changes: `
-- out/BUILD --
load("//rules:defs.bzl", "tree_artifact_output")
tree_artifact_output(
	name = "tree_artifact",
	files = {
		"file1.txt": "new",
		"file2.txt": "unchanged",
	},
	visibility = ["//visibility:public"],
)
`,
			bazelVersions: []string{"7.3.1"},
		},
		{
			name:     "tool_runfiles_paths",
			baseline: ToolRunfilesProject,
			changes: `
-- pkg/constants.bzl --
DATA = ["file1.txt", "file2.txt", "file3.txt"]
-- pkg/file3.txt --
new
`,
			bazelVersions: []string{"8.0.0"},
		},
		{
			name:     "tool_runfiles_contents",
			baseline: ToolRunfilesProject,
			changes: `
-- pkg/file1.txt --
new
`,
			bazelVersions: []string{"8.0.0"},
		},
		{
			name:     "tool_runfiles_contents_transitive",
			baseline: ToolRunfilesProject,
			changes: `
-- tools/BUILD --
genrule(
    name = "tool_sh",
    outs = ["tool.sh"],
    executable = True,
	cmd = """
cat > $@ <<'EOF'
#!/bin/bash
echo "Different Tool"
EOF
""",
    visibility = ["//visibility:public"],
    # Avoid building this target in the non-exec configuration.
    tags = ["manual"],
)
`,
			bazelVersions: []string{"8.0.0"},
		},
		{
			name:     "tool_runfiles_set_structure",
			baseline: ToolRunfilesProject,
			changes: `
-- pkg/constants.bzl --
DATA = ["file2.txt", "file1.txt"]
`,
			bazelVersions: []string{"8.0.0"},
		},
		{
			name: "tool_runfiles_symlinks_paths",
			baseline: ToolRunfilesSymlinksProject + `
-- tools/BUILD --
load("//rules:defs.bzl", "runfiles_symlinks")
runfiles_symlinks(
	name = "symlinks",
	symlinks = {
		"file1.txt": "other/pkg/common",
		"file2.txt": "old_only",
		"file3.txt": "changed",
	},
	root_symlinks = {
		"file1.txt": "other/pkg/common",
		"file2.txt": "old_only",
		"file3.txt": "changed",
	},
)
sh_binary(
    name = "tool",
	srcs = ["tool.sh"],
    data = [":symlinks"],
    visibility = ["//visibility:public"],
)
-- tools/file1.txt --
-- tools/file2.txt --
-- tools/file3.txt --
old
`,
			changes: `
-- tools/BUILD --
load("//rules:defs.bzl", "runfiles_symlinks")
runfiles_symlinks(
	name = "symlinks",
	symlinks = {
		"file1.txt": "other/pkg/common",
		"file2.txt": "new_only",
		"file3.txt": "changed",
	},
	root_symlinks = {
		"file1.txt": "other/pkg/common",
		"file2.txt": "new_only",
		"file3.txt": "changed",
	},
)
sh_binary(
    name = "tool",
	srcs = ["tool.sh"],
    data = [":symlinks"],
    visibility = ["//visibility:public"],
)
-- tools/file3.txt --
new
`,
			bazelVersions: []string{"8.0.0"},
		},
		{
			name: "tool_runfiles_symlinks_contents",
			baseline: ToolRunfilesSymlinksProject + `
-- tools/BUILD --
load("//rules:defs.bzl", "runfiles_symlinks")
runfiles_symlinks(
	name = "symlinks",
	symlinks = {
		"file1.txt": "other/pkg/common",
		"file2.txt": "../other_repo/unchanged",
		"file3.txt": "changed_target",
	},
	root_symlinks = {
		"file4.txt": "other/pkg/common",
		"file2.txt": "unchanged",
		"file3.txt": "changed_target",
	},
)
sh_binary(
    name = "tool",
	srcs = ["tool.sh"],
    data = [":symlinks"],
    visibility = ["//visibility:public"],
)
-- tools/file1.txt --
old
-- tools/file2.txt --
unchanged
-- tools/file3.txt --
unchanged
-- tools/file4.txt --
old
`,
			changes: `
-- tools/BUILD --
load("//rules:defs.bzl", "runfiles_symlinks")
runfiles_symlinks(
	name = "symlinks",
	symlinks = {
		"file1.txt": "other/pkg/common",
		"file2.txt": "../other_repo/unchanged",
		"other_file3.txt": "changed_target",
	},
	root_symlinks = {
		"file4.txt": "other/pkg/common",
		"file2.txt": "unchanged",
		"other_file3.txt": "changed_target",
	},
)
sh_binary(
    name = "tool",
	srcs = ["tool.sh"],
    data = [":symlinks"],
    visibility = ["//visibility:public"],
)
-- tools/file1.txt --
new
-- tools/file2.txt --
unchanged
-- tools/other_file3.txt --
unchanged
-- tools/file4.txt --
new
`,
			bazelVersions: []string{"8.0.0"},
		},
		{
			name: "tool_runfiles_symlinks_contents_transitive",
			baseline: ToolRunfilesSymlinksProject + `
-- tools/BUILD --
load("//rules:defs.bzl", "runfiles_symlinks")
runfiles_symlinks(
	name = "symlinks",
	symlinks = {
		"//gen": "my/file",
	},
	root_symlinks = {
	    "//gen": "my/file",
	},
    tags = ["manual"],
)
sh_binary(
    name = "tool",
	srcs = ["tool.sh"],
    data = [":symlinks"],
    visibility = ["//visibility:public"],
    tags = ["manual"],
)
-- gen/BUILD --
genrule(
	name = "gen",
	outs = ["file"],
	cmd = "echo 'Generated' > $@",
	visibility = ["//visibility:public"],
    tags = ["manual"],
)
`,
			changes: `
-- gen/BUILD --
genrule(
    name = "gen",
    outs = ["file"],
    cmd = "echo 'Generated (but different)' > $@",
    visibility = ["//visibility:public"],
    tags = ["manual"],
)
`,
			bazelVersions: []string{"8.0.0"},
		},
		{
			name: "tool_runfiles_symlinks_set_structure",
			baseline: ToolRunfilesSymlinksProject + `
-- tools/BUILD --
load("//rules:defs.bzl", "runfiles_symlinks")
runfiles_symlinks(
	name = "symlinks",
	symlinks = {
		"file1.txt": "other/pkg/common",
		"file2.txt": "also/common",
	},
	root_symlinks = {
		"file1.txt": "other/pkg/common",
		"file2.txt": "also/common",
	},
)
sh_binary(
    name = "tool",
	srcs = ["tool.sh"],
    data = [":symlinks"],
    visibility = ["//visibility:public"],
)
-- tools/file1.txt --
-- tools/file2.txt --
`,
			changes: `
-- tools/BUILD --
load("//rules:defs.bzl", "runfiles_symlinks")
runfiles_symlinks(
	name = "symlinks",
	symlinks = {
		"file2.txt": "also/common",
		"file1.txt": "other/pkg/common",
	},
	root_symlinks = {
		"file2.txt": "also/common",
		"file1.txt": "other/pkg/common",
	},
)
sh_binary(
    name = "tool",
	srcs = ["tool.sh"],
    data = [":symlinks"],
    visibility = ["//visibility:public"],
)
`,
			bazelVersions: []string{"8.0.0"},
		},
		{
			name: "settings",
			baseline: `
-- MODULE.bazel --
-- WORKSPACE.bazel --
-- pkg/BUILD --
sh_test(
    name = "test",
    srcs = ["test.sh"],
    # Add an external runfile.
    data = ["@bazel_tools//tools/bash/runfiles"],
)
-- pkg/test.sh --
#!/bin/bash
echo "Test"
`,
			changedArgs:   []string{"--legacy_external_runfiles", "--noenable_bzlmod", "--enable_workspace"},
			bazelVersions: []string{"8.0.0"},
		},
		{
			name: "transitive_invalidation",
			baseline: `
-- MODULE.bazel --
-- pkg/BUILD --
genrule(
	name = "direct1",
	srcs = ["in1.txt"],
	outs = ["out_direct1"],
	cmd = "cat $< > $@",
)
genrule(
	name = "direct2",
	srcs = ["in2.txt"],
	outs = ["out_direct2"],
	cmd = "cat $< > $@",
)
genrule(
	name = "intermediate",
	srcs = [":direct1", ":direct2"],
	outs = ["out_intermediate"],
	cmd = "cat $(SRCS) > $@",
)
genrule(
	name = "top1",
	srcs = [":intermediate"],
	outs = ["out_top1"],
	cmd = "cat $< > $@",
)
genrule(
	name = "top2",
	srcs = [":intermediate"],
	outs = ["out_top2"],
	cmd = "cat $< > $@",
)
-- pkg/in1.txt --
old
-- pkg/in2.txt --
old
`,
			changes: `
-- pkg/in1.txt --
new
-- pkg/in2.txt --
new
`,
			bazelVersions: []string{"8.0.0"},
		},
	} {
		if toGenerate != nil && !toGenerate[tc.name] {
			continue
		}

		if len(tc.bazelVersions) == 0 {
			log.Fatalf("No bazel versions specified for test %s", tc.name)
		}
		for _, bazelVersion := range tc.bazelVersions {
			tmpDir, err := os.MkdirTemp("", "explain-test-*")
			if err != nil {
				log.Fatalf("Failed to create temp dir: %s", err)
			}
			defer os.RemoveAll(tmpDir)

			extractTxtar(tmpDir, tc.baseline)
			collectLog(bazelisk, tc.baselineArgs, tmpDir, filepath.Join(outDir, bazelVersion, tc.name+"_old.pb.zstd"), bazelVersion)

			extractTxtar(tmpDir, tc.changes)
			collectLog(bazelisk, tc.changedArgs, tmpDir, filepath.Join(outDir, bazelVersion, tc.name+"_new.pb.zstd"), bazelVersion)
		}
	}
}

func collectLog(bazelisk string, args []string, projectDir, logPath, bazelVersion string) {
	outputBase, err := os.MkdirTemp("", "explain-testdata-*")
	if err != nil {
		log.Fatalf("Failed to create temp output base: %s", err)
	}
	defer os.RemoveAll(outputBase)
	// Bazel's output base can contain files with no write permissions.
	defer filepath.WalkDir(outputBase, func(path string, d fs.DirEntry, err error) error {
		return os.Chmod(path, 0755)
	})

	err = os.MkdirAll(filepath.Dir(logPath), 0755)
	if err != nil {
		log.Fatalf("Failed to create log directory: %s", err)
	}

	cmd := exec.Command(
		bazelisk,
		"--nohome_rc", "--nosystem_rc",
		"--output_base="+outputBase,
		"test", "//...",
	)
	cmd.Args = append(cmd.Args, args...)
	cmd.Args = append(
		cmd.Args,
		"--java_runtime_version=remotejdk_21",
		"--experimental_execution_log_compact_file="+logPath,
	)
	cmd.Dir = projectDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), "USE_BAZEL_VERSION="+bazelVersion)
	if err = cmd.Run(); err != nil {
		// Allow failures due to no tests as we always run with `bazel test`.
		// Also allow failures due to failing tests.
		if exitErr, ok := err.(*exec.ExitError); !ok || (exitErr.ExitCode() != 4 && exitErr.ExitCode() != 3) {
			log.Fatalf("Failed to run command: %s", err)
		}
	}
}

func extractTxtar(dir string, tar string) {
	txtarFS, err := txtar.FS(txtar.Parse([]byte(tar)))
	if err != nil {
		log.Fatalf("Failed to create txtar fs: %s", err)
	}
	err = copy.Copy(".", dir, copy.Options{
		FS:                txtarFS,
		PermissionControl: copy.AddPermission(0755)},
	)
	if err != nil {
		log.Fatalf("Failed to copy txtar fs: %s", err)
	}
}
