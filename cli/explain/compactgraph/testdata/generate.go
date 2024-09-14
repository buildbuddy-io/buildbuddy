package main

import (
	"os"
	"os/exec"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/otiai10/copy"
	"golang.org/x/tools/txtar"
)

const BaseProject = `
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

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class LibTest {
    @Test
    public void testGetName() {
        assertEquals("Lib", Lib.getName());
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

func main() {
	buildWorkspaceDirectory := os.Getenv("BUILD_WORKSPACE_DIRECTORY")
	if buildWorkspaceDirectory == "" {
		log.Fatalf("BUILD_WORKSPACE_DIRECTORY environment variable must be set, run with `bazel run`")
	}
	ownPackage := os.Args[1]
	outDir := filepath.Join(buildWorkspaceDirectory, filepath.FromSlash(ownPackage))
	for _, tc := range []struct {
		name         string
		baseline     string
		baselineArgs []string
		changes      string
		changedArgs  []string
	}{{
		name:     "add_comment",
		baseline: BaseProject,
		changes: `
-- src/main/java/com/example/lib/Lib.java --
package com.example.lib;

public class Lib {
    public static String getName() {
      return "Lib"; // no-op change
    }
}
`,
	}} {
		tmpDir, err := os.MkdirTemp("", "explain-test-*")
		if err != nil {
			log.Fatalf("Failed to create temp dir: %s", err)
		}
		defer os.RemoveAll(tmpDir)

		projectDir := filepath.Join(tmpDir, "project")
		outputBase := filepath.Join(tmpDir, "output_base")
		extractTxtar(projectDir, tc.baseline)
		collectLog(tc.baselineArgs, projectDir, outputBase, filepath.Join(outDir, tc.name+"_old.pb.zstd"))

		extractTxtar(projectDir, tc.changes)
		collectLog(tc.changedArgs, projectDir, outputBase, filepath.Join(outDir, tc.name+"_new.pb.zstd"))
	}
}

func collectLog(args []string, projectDir, outputBase, logPath string) {
	cmd := exec.Command(
		"bazel",
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
	if err := cmd.Run(); err != nil {
		log.Fatalf("Failed to run command: %s", err)
	}
}

func extractTxtar(dir string, tar string) {
	fs, err := txtar.FS(txtar.Parse([]byte(tar)))
	if err != nil {
		log.Fatalf("Failed to create txtar fs: %s", err)
	}
	err = copy.Copy(".", dir, copy.Options{
		FS:                fs,
		PermissionControl: copy.AddPermission(0755)},
	)
	if err != nil {
		log.Fatalf("Failed to copy txtar fs: %s", err)
	}
}
