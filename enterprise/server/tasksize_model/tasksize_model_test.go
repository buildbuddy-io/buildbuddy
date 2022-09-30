// This test can be run manually to check that the Go implementation of the task
// size model returns the same values as the model produced by the training
// script.
//
// Copy a sample prediction from the training script output into the `examples`
// list in this test, then run the test with
//
// bazel test enterprise/server/tasksize_model:tasksize_model_test \
// --test_output=streamed \
// --test_arg=--remote_execution.tasksize_model.features_config_path=/path/to/out/features.json \
// --test_arg=--remote_execution.tasksize_model.serving_address=grpc://localhost:8500

package tasksize_model

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type example struct {
	x                []float32
	expectedMemMB    int64
	expectedMilliCPU int64
}

var examples = []*example{
	// {
	// 	x:                []float32{21.0, 165.0, 2138.0, 300.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0},
	// 	expectedMemMB:    227,
	// 	expectedMilliCPU: 750,
	// },
}

func TestSamplePrediction(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	m, err := New(env)
	require.NoError(t, err)

	for m.Predict(ctx, &repb.ExecutionTask{}) == nil {
		log.Infof("Waiting for model to initialize...")
		time.Sleep(1 * time.Second)
	}

	for _, ex := range examples {
		cpu, err := m.predict(ctx, "cpu", ex.x)
		require.NoError(t, err)
		mem, err := m.predict(ctx, "mem", ex.x)
		require.NoError(t, err)

		require.Equal(t, ex.expectedMemMB, int64(mem/1e6))
		require.Equal(t, ex.expectedMilliCPU, int64(cpu))
	}
}
