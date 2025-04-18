package bare_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/bare"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func makeTempDirWithWorldTxt(t *testing.T) string {
	dir := testfs.MakeTempDir(t)

	f, err := os.Create(fmt.Sprintf("%s/world.txt", dir))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_, err = f.WriteString("world")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestHelloWorldOnBareMetal(t *testing.T) {
	ctx := context.Background()
	tempDir := makeTempDirWithWorldTxt(t)
	cmd := &repb.Command{
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			&repb.Command_EnvironmentVariable{Name: "GREETING", Value: "Hello"},
		},
		Arguments: []string{"sh", "-c", fmt.Sprintf("printf \"$GREETING $(cat %s/world.txt)!\"", tempDir)},
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				&repb.Platform_Property{
					Name:  "container-image",
					Value: "none",
				},
			},
		},
	}
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	bareContainer := bare.NewBareCommandContainer(&bare.Opts{})
	result := bareContainer.Run(ctx, cmd, tempDir, oci.Credentials{})

	if result.Error != nil {
		t.Fatal(result.Error)
	}
	assert.Regexp(t, "^(/usr)?/bin/sh\\s", result.CommandDebugString, "sanity check: command should be run bare")
	assert.Equal(t, "Hello world!", string(result.Stdout),
		"stdout should equal 'Hello world!' ('$GREETING' env var should be replaced with 'Hello', and "+
			"tempfile containing 'world' should be readable.)",
	)
	assert.Empty(t, string(result.Stderr), "stderr should be empty")
	assert.Equal(t, 0, result.ExitCode, "should exit with success")
}

func TestLogFiles(t *testing.T) {
	flags.Set(t, "executor.bare.enable_log_files", true)
	ctx := context.Background()
	ctr := bare.NewBareCommandContainer(&bare.Opts{})
	workDir := testfs.MakeTempDir(t)

	result := ctr.Run(ctx, &repb.Command{Arguments: []string{"bash", "-ec", `
		echo test-stdout >&1
		echo test-stderr >&2
		while true; do
			logged_stderr=$(cat "$(pwd).stderr")
			logged_stdout=$(cat "$(pwd).stdout")
			if [[ $logged_stderr == test-stderr ]] && [[ $logged_stdout == test-stdout ]]; then
				exit 0
			fi
			if [[ -n $logged_stderr ]] && [[ -n $logged_stdout ]]; then
				echo >&2 "Unexpected contents: stderr='$logged_stderr' stdout='$logged_stdout'"
				exit 1
			fi
			# Wait a little bit and try again in case the log files have not
			# been flushed yet.
			sleep 0.01
		done
	`}}, workDir, oci.Credentials{})

	assert.Equal(t, "test-stderr\n", string(result.Stderr))
	assert.Equal(t, "test-stdout\n", string(result.Stdout))
	assert.Equal(t, 0, result.ExitCode)
}

func TestTMPDIR(t *testing.T) {
	for _, test := range []struct {
		name            string
		relativeWorkDir bool
	}{
		{
			name:            "absolute work dir",
			relativeWorkDir: false,
		},
		{
			name:            "relative work dir",
			relativeWorkDir: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			flags.Set(t, "executor.bare.enable_tmpdir", true)
			ctx := context.Background()
			provider := bare.Provider{}
			ctr, err := provider.New(ctx, &container.Init{})
			require.NoError(t, err)

			workDir := testfs.MakeTempDir(t)
			if test.relativeWorkDir {
				t.Chdir(filepath.Dir(workDir))
				workDir = filepath.Base(workDir)
			}

			res := ctr.Run(ctx, &repb.Command{
				Arguments: []string{"bash", "-ec", `
					echo -n foo > $TMPDIR/foo.txt
					# Make sure TMPDIR is absolute.
					if ! [[ $TMPDIR == /* ]]; then
						echo >&2 "TMPDIR is not absolute: $TMPDIR"
						exit 1
					fi
				`},
			}, workDir, oci.Credentials{})
			assert.Empty(t, string(res.Stderr))
			require.NoError(t, res.Error)

			b, err := os.ReadFile(filepath.Join(workDir+".tmp", "foo.txt"))
			require.NoError(t, err)
			assert.Equal(t, "foo", string(b))
			_, err = os.Stat(workDir + ".tmp")
			require.NoError(t, err)

			err = ctr.Remove(ctx)
			require.NoError(t, err)

			_, err = os.Stat(workDir + ".tmp")
			require.True(t, os.IsNotExist(err), "unexpected error: %v", err)
		})
	}
}
