package persistentworker_test

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/persistentworker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type stderrContainer struct {
	container.CommandContainer
	stderr []byte
}

func (c *stderrContainer) Exec(ctx context.Context, command *repb.Command, stdio *interfaces.Stdio) *interfaces.CommandResult {
	// Wait for a complete work request before exiting so that the stderr is
	// associated with the current request rather than the worker startup.
	r := bufio.NewReader(stdio.Stdin)
	size, err := binary.ReadUvarint(r)
	if err == nil {
		_, err = io.CopyN(io.Discard, r, int64(size))
	}
	if err != nil {
		return &interfaces.CommandResult{Error: err, ExitCode: -2}
	}
	_, _ = stdio.Stderr.Write(c.stderr)
	return &interfaces.CommandResult{ExitCode: 1}
}

func TestWorkerExec_Stderr(t *testing.T) {
	tests := []struct {
		name       string
		stderr     []byte
		wantStderr string
	}{
		{
			name:       "empty",
			wantStderr: "<empty>",
		},
		{
			name:       "valid UTF-8",
			stderr:     []byte("hello, 世界"),
			wantStderr: "hello, 世界",
		},
		{
			name:       "invalid UTF-8",
			stderr:     []byte{'a', 0xff, 'b'},
			wantStderr: "a�b",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			cmd := &repb.Command{Arguments: []string{"worker"}}
			ctr := &stderrContainer{stderr: test.stderr}
			w, err := persistentworker.Start(ctx, &workspace.Workspace{}, ctr, "proto", cmd)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, w.Stop())
			})

			result := w.Exec(ctx, cmd)
			require.Error(t, result.Error)
			require.True(t, utf8.ValidString(result.Error.Error()), "error contains invalid UTF-8: %q", result.Error)
			require.Contains(t, result.Error.Error(), "persistent worker stderr:\n"+test.wantStderr)
		})
	}
}

type echoArgsContainer struct {
	container.CommandContainer
}

func (c *echoArgsContainer) Exec(ctx context.Context, command *repb.Command, stdio *interfaces.Stdio) *interfaces.CommandResult {
	// Echo the arguments using the JSON worker protocol.
	var request struct {
		Arguments []string `json:"arguments"`
	}
	if err := json.NewDecoder(stdio.Stdin).Decode(&request); err != nil {
		return &interfaces.CommandResult{Error: err, ExitCode: -2}
	}
	err := json.NewEncoder(stdio.Stdout).Encode(map[string]string{
		"output": strings.Join(request.Arguments, "\n"),
	})
	return &interfaces.CommandResult{Error: err}
}

func TestWorkerExec_ExpandsFlagFiles(t *testing.T) {
	ctx := context.Background()
	ws, err := workspace.New(nil, t.TempDir(), &workspace.Opts{})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(ws.Path(), "flags"), []byte("one\n@nested\nliteral\n"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(ws.Path(), "nested"), []byte("two\n"), 0644))

	cmd := &repb.Command{Arguments: []string{"worker", "@flags"}}
	w, err := persistentworker.Start(ctx, ws, &echoArgsContainer{}, "json", cmd)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Stop()) })

	result := w.Exec(ctx, cmd)
	require.NoError(t, result.Error)
	require.Equal(t, 0, result.ExitCode)
	require.Equal(t, "one\ntwo\nliteral", string(result.Stderr))
}

func TestWorkerExec_RejectsFlagFileWorkspaceEscape(t *testing.T) {
	ctx := context.Background()
	ws, err := workspace.New(nil, t.TempDir(), &workspace.Opts{})
	require.NoError(t, err)
	outside := filepath.Join(t.TempDir(), "outside")
	require.NoError(t, os.WriteFile(outside, []byte("secret\n"), 0644))
	require.NoError(t, os.Symlink(outside, filepath.Join(ws.Path(), "link")))
	relativeOutside, err := filepath.Rel(ws.Path(), outside)
	require.NoError(t, err)

	for _, arg := range []string{"@" + relativeOutside, "@" + outside, "@link"} {
		t.Run(arg, func(t *testing.T) {
			cmd := &repb.Command{Arguments: []string{"worker", arg}}
			w, err := persistentworker.Start(ctx, ws, &echoArgsContainer{}, "json", cmd)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, w.Stop()) })

			result := w.Exec(ctx, cmd)
			require.ErrorContains(t, result.Error, "expand flag files")
		})
	}
}
