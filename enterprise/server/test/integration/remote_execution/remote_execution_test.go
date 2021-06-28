package remote_execution_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestSimpleCommandWithNonZeroExitCode(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutor()

	cmd := rbe.ExecuteCustomCommand("sh", "-c", "echo hello && echo bye >&2 && exit 5")
	res := cmd.Wait()

	assert.Equal(t, 5, res.ExitCode, "exit code should be propagated")
	assert.Equal(t, "hello\n", res.Stdout, "stdout should be propagated")
	assert.Equal(t, "bye\n", res.Stderr, "stderr should be propagated")
}

func TestSimpleCommandWithZeroExitCode(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutor()

	cmd := rbe.ExecuteCustomCommand("sh", "-c", "echo hello && echo bye >&2")
	res := cmd.Wait()

	assert.Equal(t, 0, res.ExitCode, "exit code should be propagated")
	assert.Equal(t, "hello\n", res.Stdout, "stdout should be propagated")
	assert.Equal(t, "bye\n", res.Stderr, "stderr should be propagated")
}

func TestSimpleCommandWithExecutorAuthorizationEnabled(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServerWithOptions(&rbetest.BuildBuddyServerOptions{
		SchedulerServerOptions: scheduler_server.Options{
			RequireExecutorAuthorization: true,
		},
	})
	rbe.AddExecutorWithOptions(&rbetest.ExecutorOptions{
		Name:   "executor",
		APIKey: rbetest.ExecutorAPIKey,
	})

	cmd := rbe.ExecuteCustomCommand("sh", "-c", "echo hello && echo bye >&2")
	cmd.Wait()
}

func TestSimpleCommand_RunnerReuse_CanReadPreviouslyWrittenFileButNotOutputDirs(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutor()

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "container-image", Value: "none"},
			{Name: "recycle-runner", Value: "true"},
			{Name: "preserve-workspace", Value: "true"},
		},
	}

	// Note: authentication is required for workspace reuse, currently.
	opts := &rbetest.ExecuteOpts{UserID: rbe.UserID1}

	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{
			"touch", "output.txt", "undeclared_output.txt", "output_dir/output.txt",
		},
		Platform:          platform,
		OutputDirectories: []string{"output_dir"},
		OutputFiles:       []string{"output.txt"},
	}, opts)
	cmd.Wait()

	cmd = rbe.Execute(&repb.Command{
		Arguments: []string{
			"ls", "output.txt", "undeclared_output.txt", "output_dir/output.txt",
		},
		Platform: platform,
	}, opts)
	res := cmd.Wait()

	assert.Equal(
		t, "undeclared_output.txt\n", res.Stdout,
		"should be able to read undeclared outputs but not other outputs")
}

func TestSimpleCommand_RunnerReuse_ReLinksFilesFromFileCache(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutor()

	tmpDir := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, tmpDir, map[string]string{
		"f1.input": "A",
		"f2.input": "B",
	})
	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "container-image", Value: "none"},
			{Name: "recycle-runner", Value: "true"},
			{Name: "preserve-workspace", Value: "true"},
		},
	}
	opts := &rbetest.ExecuteOpts{InputRootDir: tmpDir, UserID: rbe.UserID1}

	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{"cat", "f1.input", "f2.input"},
		Platform:  platform,
	}, opts)
	res := cmd.Wait()

	require.Equal(t, "AB", res.Stdout)

	tmpDir = testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, tmpDir, map[string]string{
		// Overwrite "a.input" with "B" so that we attempt to link over "a.input"
		// from the filecache ("B" should exist in the filecache since it was
		// present as "b.input" in the previous action).
		"f1.input": "B",
	})
	opts = &rbetest.ExecuteOpts{InputRootDir: tmpDir, UserID: rbe.UserID1}

	cmd = rbe.Execute(&repb.Command{
		Arguments: []string{"cat", "f1.input", "f2.input"},
		Platform:  platform,
	}, opts)
	res = cmd.Wait()

	// If this still equals "A" then we probably didn't properly delete
	// "a.input" before linking it from FileCache.
	require.Equal(t, "BB", res.Stdout)
}

func TestSimpleCommand_RunnerReuse_ReLinksFilesFromDuplicateInputs(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutor()

	tmpDir := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, tmpDir, map[string]string{
		"f1.input": "A",
		"f2.input": "A",
	})
	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "container-image", Value: "none"},
			{Name: "recycle-runner", Value: "true"},
			{Name: "preserve-workspace", Value: "true"},
		},
	}
	opts := &rbetest.ExecuteOpts{InputRootDir: tmpDir, UserID: rbe.UserID1}

	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{"cat", "f1.input", "f2.input"},
		Platform:  platform,
	}, opts)
	res := cmd.Wait()

	require.Equal(t, "AA", res.Stdout)

	tmpDir = testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, tmpDir, map[string]string{
		"f1.input": "B",
		"f2.input": "B",
	})
	opts = &rbetest.ExecuteOpts{InputRootDir: tmpDir, UserID: rbe.UserID1}

	cmd = rbe.Execute(&repb.Command{
		Arguments: []string{"cat", "f1.input", "f2.input"},
		Platform:  platform,
	}, opts)
	res = cmd.Wait()

	// If either of these is equal to "A" then we didn't properly link the file
	// from its duplicate.
	require.Equal(t, "BB", res.Stdout)
}

func TestSimpleCommand_RunnerReuse_MultipleExecutors_RoutesCommandToSameExecutor(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServers(3)
	rbe.AddExecutors(10)

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "recycle-runner", Value: "true"},
			{Name: "preserve-workspace", Value: "true"},
		},
	}
	opts := &rbetest.ExecuteOpts{UserID: rbe.UserID1}

	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{"touch", "foo.txt"},
		Platform:  platform,
	}, opts)
	res := cmd.Wait()

	require.Equal(t, 0, res.ExitCode)

	cmd = rbe.Execute(&repb.Command{
		Arguments: []string{"stat", "foo.txt"},
		Platform:  platform,
	}, opts)
	res = cmd.Wait()

	require.Equal(t, 0, res.ExitCode)
}

func TestSimpleCommandWithMultipleExecutors(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutors(5)

	cmd := rbe.ExecuteCustomCommand("sh", "-c", "echo hello && echo bye >&2")
	res := cmd.Wait()

	assert.Equal(t, 0, res.ExitCode, "exit code should be propagated")
	assert.Equal(t, "hello\n", res.Stdout, "stdout should be propagated")
	assert.Equal(t, "bye\n", res.Stderr, "stderr should be propagated")
}

func TestManySimpleCommandsWithMultipleExecutors(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutors(5)

	var cmds []*rbetest.Command
	for i := 0; i < 5; i++ {
		cmd := rbe.ExecuteCustomCommand("sh", "-c", fmt.Sprintf("echo 'hello from command %d'", i))
		cmds = append(cmds, cmd)
	}
	for i := range cmds {
		res := cmds[i].Wait()
		assert.Equal(t, 0, res.ExitCode, "exit code should be propagated")
		assert.Equal(t, fmt.Sprintf("hello from command %d\n", i), res.Stdout, "stdout should be propagated")
		assert.Equal(t, "", res.Stderr, "stderr should be empty")
	}
}

func TestManySimpleCommandsWithMultipleExecutors_TaskStreamingEnabled(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	for i := 0; i < 5; i++ {
		rbe.AddExecutorWithOptions(&rbetest.ExecutorOptions{
			Name:                fmt.Sprintf("executor%d", i+1),
			EnableWorkStreaming: true,
		})
	}

	var cmds []*rbetest.Command
	for i := 0; i < 5; i++ {
		cmd := rbe.ExecuteCustomCommand("sh", "-c", fmt.Sprintf("echo 'hello from command %d'", i))
		cmds = append(cmds, cmd)
	}
	for i := range cmds {
		res := cmds[i].Wait()
		assert.Equal(t, 0, res.ExitCode, "exit code should be propagated")
		assert.Equal(t, fmt.Sprintf("hello from command %d\n", i), res.Stdout, "stdout should be propagated")
		assert.Equal(t, "", res.Stderr, "stderr should be empty")
	}
}

func TestBasicActionIO(t *testing.T) {
	tmpDir := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, tmpDir, map[string]string{
		"greeting.input":       "Hello ",
		"child/farewell.input": "Goodbye ",
	})

	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	rbe.AddExecutor()

	opts := &rbetest.ExecuteOpts{InputRootDir: tmpDir}
	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{
			"sh", "-c", strings.Join([]string{
				`set -e`,
				// Create a file in the output directory.
				// No need to create the output directory itself; executor is
				// responsible for that.
				`cp greeting.input out_dir/hello_world.output`,
				`printf 'world' >> out_dir/hello_world.output`,
				// Create a file in a child dir of the output directory.
				// Need to create the child directory ourselves since it's not a declared
				// output directory. Note that the executor should still upload it as
				// part of the output dir tree.
				`mkdir out_dir/child`,
				`cp child/farewell.input out_dir/child/goodbye_world.output`,
				`printf 'world' >> out_dir/child/goodbye_world.output`,
				// Create an explicitly declared output
				`cp greeting.input out_files_dir/hello_bb.output`,
				`printf 'BB' >> out_files_dir/hello_bb.output`,
				// Create another explicitly declared output.
				// No need to create out_files_dir/child; executor is responsible for that.
				`cp child/farewell.input out_files_dir/child/goodbye_bb.output`,
				`printf 'BB' >> out_files_dir/child/goodbye_bb.output`,
			}, "\n"),
		},
		OutputDirectories: []string{"out_dir"},
		OutputFiles: []string{
			"out_files_dir/hello_bb.output",
			"out_files_dir/child/goodbye_bb.output",
		},
	}, opts)
	res := cmd.Wait()

	require.Equal(t, 0, res.ExitCode)

	outDir := rbe.DownloadOutputsToNewTempDir(res)

	testfs.AssertExactFileContents(t, outDir, map[string]string{
		"out_dir/hello_world.output":            "Hello world",
		"out_dir/child/goodbye_world.output":    "Goodbye world",
		"out_files_dir/hello_bb.output":         "Hello BB",
		"out_files_dir/child/goodbye_bb.output": "Goodbye BB",
	})
}

func TestComplexActionIO(t *testing.T) {
	t.Skip() // TODO: De-flake and re-enable.

	tmpDir := testfs.MakeTempDir(t)
	sizes := []int{
		10e1, 10e2, 10e3, 10e4, 10e5, 10e6,
		10e1, 10e2, 10e3, 10e4, 10e5, 10e6,
	}
	inputDirs := []string{"" /*input root*/, "a", "b", "a/child", "b/child"}
	outputFiles := []string{}
	contents := map[string]string{}
	for _, dir := range inputDirs {
		if err := os.MkdirAll(filepath.Join(tmpDir, dir), 0777); err != nil {
			assert.FailNow(t, err.Error())
		}
		for i, size := range sizes {
			relPath := filepath.Join(dir, fmt.Sprintf("file_%d.input", i))
			content := testfs.WriteRandomString(t, tmpDir, relPath, size)
			contents[relPath] = content
			outputFiles = append(outputFiles, filepath.Join("out_files_dir", dir, fmt.Sprintf("file_%d.output", i)))
		}
	}

	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	rbe.AddExecutor()

	opts := &rbetest.ExecuteOpts{InputRootDir: tmpDir}
	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{"sh", "-c", strings.Join([]string{
			`set -e`,
			`input_paths=$(find . -type f)`,
			// Mirror the input tree to out_files_dir, skipping the first byte so that
			// the output digests are different. Note that we don't create directories
			// here since the executor is responsible for creating parent dirs of
			// output files.
			`
			for path in $input_paths; do
				output_path="out_files_dir/$(echo "$path" | sed 's/.input/.output/')"
				cat "$path" | tail -c +2 > "$output_path"
			done
			`,
			// Mirror the input tree to out_dir, skipping the first 2 bytes this time.
			// We *do* need to create parent dirs since the executor is only
			// responsible for creating the top-level out_dir.
			`
			for path in $input_paths; do
				output_path="out_dir/$(echo "$path" | sed 's/.input/.output/')"
				mkdir -p out_dir/"$(dirname "$path")"
				cat "$path" | tail -c +3 > "$output_path"
			done
			`,
		}, "\n")},
		OutputDirectories: []string{"out_dir"},
		OutputFiles:       outputFiles,
	}, opts)
	res := cmd.Wait()

	require.Equal(t, 0, res.ExitCode)
	require.Empty(t, res.Stderr)

	outDir := rbe.DownloadOutputsToNewTempDir(res)

	skippedBytes := map[string]int{
		"out_files_dir": 1,
		"out_dir":       2,
	}
	missing := []string{}
	for parent, nSkippedBytes := range skippedBytes {
		for _, dir := range inputDirs {
			for i, _ := range sizes {
				inputRelPath := filepath.Join(dir, fmt.Sprintf("file_%d.input", i))
				outputRelPath := filepath.Join(parent, dir, fmt.Sprintf("file_%d.output", i))
				if testfs.Exists(t, outDir, outputRelPath) {
					inputContents, ok := contents[inputRelPath]
					require.Truef(t, ok, "sanity check: missing input contents of %s", inputRelPath)
					expectedContents := inputContents[nSkippedBytes:]
					actualContents := testfs.ReadFileAsString(t, outDir, outputRelPath)
					// Not using assert.Equal here since the diff can be huge.
					assert.Truef(
						t, expectedContents == actualContents,
						"contents of %s did not match (expected len: %d; actual len: %d)",
						outputRelPath, len(expectedContents), len(actualContents),
					)
				} else {
					missing = append(missing, outputRelPath)
				}
			}
		}
	}
	assert.Empty(t, missing)
}

func TestUnregisterExecutor(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()

	// Start with two executors.
	// AddExecutors will block until both are registered.
	executors := rbe.AddExecutors(2)

	// Remove one of the executors.
	// RemoveExecutor will block until the executor is unregistered.
	rbe.RemoveExecutor(executors[0])
}

func TestMultipleSchedulersAndExecutors(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	// Start with 2 BuildBuddy servers.
	rbe.AddBuildBuddyServer()
	rbe.AddBuildBuddyServer()
	rbe.AddExecutors(5)

	var cmds []*rbetest.Command
	for i := 0; i < 10; i++ {
		cmd := rbe.ExecuteCustomCommand("sh", "-c", fmt.Sprintf("echo 'hello from command %d'", i))
		cmds = append(cmds, cmd)
	}
	for i := range cmds {
		res := cmds[i].Wait()
		assert.Equal(t, 0, res.ExitCode, "exit code should be propagated")
		assert.Equal(t, fmt.Sprintf("hello from command %d\n", i), res.Stdout, "stdout should be propagated")
		assert.Equal(t, "", res.Stderr, "stderr should be empty")
	}
}

func TestWorkSchedulingOnNewExecutor(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddSingleTaskExecutorWithOptions(&rbetest.ExecutorOptions{Name: "busyExecutor1"})
	rbe.AddSingleTaskExecutorWithOptions(&rbetest.ExecutorOptions{Name: "busyExecutor2"})

	// Schedule 2 controlled commands to keep existing executors busy.
	cmd1 := rbe.ExecuteControlledCommand("command1")
	cmd2 := rbe.ExecuteControlledCommand("command2")

	// Wait until both the commands actually start running on the executors.
	cmd1.WaitStarted()
	cmd2.WaitStarted()

	// Schedule some additional commands that existing executors can't take on.
	var cmds []*rbetest.Command
	for i := 0; i < 10; i++ {
		cmd := rbe.ExecuteCustomCommand("sh", "-c", fmt.Sprintf("echo 'hello from command %d'", i))
		cmds = append(cmds, cmd)
	}

	for _, cmd := range cmds {
		cmd.WaitAccepted()
	}

	// Add a new executor that should get assigned the additional tasks.
	rbe.AddExecutorWithOptions(&rbetest.ExecutorOptions{Name: "newExecutor"})

	for i, cmd := range cmds {
		res := cmd.Wait()
		assert.Equal(t, "newExecutor", res.Executor, "[%s] should have been executed on new executor", cmd.Name)
		assert.Equal(t, 0, res.ExitCode, "exit code should be propagated")
		assert.Equal(t, fmt.Sprintf("hello from command %d\n", i), res.Stdout, "stdout should be propagated")
		assert.Equal(t, "", res.Stderr, "stderr should be empty")
	}

	// Allow controlled commands to exit.
	cmd1.Exit(0)
	cmd2.Exit(0)

	cmd1.Wait()
	cmd2.Wait()
}

// Test WaitExecution across different severs.
func TestWaitExecution(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	// Start multiple servers so that executions are spread out across different servers.
	for i := 0; i < 5; i++ {
		rbe.AddBuildBuddyServer()
	}
	rbe.AddExecutors(5)

	var cmds []*rbetest.ControlledCommand
	for i := 0; i < 10; i++ {
		cmds = append(cmds, rbe.ExecuteControlledCommand(fmt.Sprintf("command%d", i+1)))
	}

	// Wait until all the commands have started running & have been accepted by the server.
	for _, c := range cmds {
		c.WaitStarted()
		c.WaitAccepted()
	}

	// Cancel in-flight Execute requests and call the WaitExecution API.
	for _, c := range cmds {
		c.ReplaceWaitUsingWaitExecutionAPI()
	}

	for i, c := range cmds {
		c.Exit(int32(i))
	}

	for i, cmd := range cmds {
		res := cmd.Wait()
		assert.Equal(t, i, res.ExitCode, "exit code should be propagated for command %q", cmd.Name)
	}
}
