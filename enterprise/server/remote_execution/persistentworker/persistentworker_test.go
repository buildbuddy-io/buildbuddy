package persistentworker

import (
	"bufio"
	"fmt"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	wkpb "github.com/buildbuddy-io/buildbuddy/proto/worker"
)

func TestUnmarshalWorkResponse_ResetsLimitPerResponse(t *testing.T) {
	flags.Set(t, "executor.stdouterr_max_size_bytes", uint64(50))

	payload := strings.Join([]string{
		`{"exitCode":0,"output":"abcdefghijklmnopqrstuvw"}`,
		`{"exitCode":0,"output":"0123456789"}`,
	}, "\n")

	w := &Worker{
		protocol:     jsonProtocol,
		stdoutReader: bufio.NewReader(strings.NewReader(payload)),
	}

	resp1 := &wkpb.WorkResponse{}
	require.NoError(t, w.unmarshalWorkResponse(resp1))
	require.Equal(t, int32(0), resp1.ExitCode)
	require.Equal(t, "abcdefghijklmnopqrstuvw", resp1.Output)

	resp2 := &wkpb.WorkResponse{}
	require.NoError(t, w.unmarshalWorkResponse(resp2))
	require.Equal(t, "0123456789", resp2.Output)
}

func TestUnmarshalWorkResponse_EnforcesLimitPerResponse(t *testing.T) {
	flags.Set(t, "executor.stdouterr_max_size_bytes", uint64(60))

	tooLarge := strings.Repeat("a", 80)
	payload := strings.Join([]string{
		`{"exitCode":0,"output":"short"}`,
		fmt.Sprintf(`{"exitCode":0,"output":"%s"}`, tooLarge),
	}, "\n")

	w := &Worker{
		protocol:     jsonProtocol,
		stdoutReader: bufio.NewReader(strings.NewReader(payload)),
	}

	resp := &wkpb.WorkResponse{}
	require.NoError(t, w.unmarshalWorkResponse(resp))
	require.Equal(t, "short", resp.Output)

	require.Error(t, w.unmarshalWorkResponse(&wkpb.WorkResponse{}))
}
