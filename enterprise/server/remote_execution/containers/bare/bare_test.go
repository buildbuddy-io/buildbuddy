package bare_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
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
	stdout, stderr, stdio := commandutil.BufferStdio()
	result := bareContainer.Run(ctx, cmd, tempDir, oci.Credentials{}, stdio)

	if result.Error != nil {
		t.Fatal(result.Error)
	}
	assert.Regexp(t, "^(/usr)?/bin/sh\\s", result.CommandDebugString, "sanity check: command should be run bare")
	assert.Equal(t, "Hello world!", stdout.String(),
		"stdout should equal 'Hello world!' ('$GREETING' env var should be replaced with 'Hello', and "+
			"tempfile containing 'world' should be readable.)",
	)
	assert.Empty(t, stderr.String(), "stderr should be empty")
	// Output should only be written to the stdio writers, not buffered in the
	// command result.
	assert.Empty(t, string(result.Stdout))
	assert.Empty(t, string(result.Stderr))
	assert.Equal(t, 0, result.ExitCode, "should exit with success")
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

			_, stderr, stdio := commandutil.BufferStdio()
			res := ctr.Run(ctx, &repb.Command{
				Arguments: []string{"bash", "-ec", `
					echo -n foo > $TMPDIR/foo.txt
					# Make sure TMPDIR is absolute.
					if ! [[ $TMPDIR == /* ]]; then
						echo >&2 "TMPDIR is not absolute: $TMPDIR"
						exit 1
					fi
				`},
			}, workDir, oci.Credentials{}, stdio)
			assert.Empty(t, stderr.String())
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

func TestBareRun_WorkingDirectory(t *testing.T) {
	ctx := context.Background()
	workDir := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, workDir, map[string]string{
		"subdir/greeting.txt": "hello",
	})

	cmd := &repb.Command{
		Arguments:        []string{"sh", "-c", "cat greeting.txt"},
		WorkingDirectory: "subdir",
	}
	ctr := bare.NewBareCommandContainer(&bare.Opts{})
	stdout, _, stdio := commandutil.BufferStdio()
	result := ctr.Run(ctx, cmd, workDir, oci.Credentials{}, stdio)

	require.NoError(t, result.Error)
	assert.Equal(t, 0, result.ExitCode)
	assert.Equal(t, "hello", stdout.String())
}

func TestBareExec_WorkingDirectory(t *testing.T) {
	ctx := context.Background()
	workDir := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, workDir, map[string]string{
		"subdir/greeting.txt": "hello",
	})

	cmd := &repb.Command{
		Arguments:        []string{"sh", "-c", "cat greeting.txt"},
		WorkingDirectory: "subdir",
	}
	ctr := bare.NewBareCommandContainer(&bare.Opts{})
	err := ctr.Create(ctx, workDir)
	require.NoError(t, err)
	result := ctr.Exec(ctx, cmd, nil)

	require.NoError(t, result.Error)
	assert.Equal(t, 0, result.ExitCode)
	assert.Equal(t, "hello", string(result.Stdout))
}
