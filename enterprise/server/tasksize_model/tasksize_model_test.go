package tasksize_model_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize_model"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestTaskSizeModel(t *testing.T) {
	// Simple model: no normalization, no one-hot features; only place weight on
	// arg count (first feature)
	paramsJSON := `{
		"normalization": {
			"mean": [0, 0, 0, 0, 0],
			"variance": [1, 1, 1, 1, 1]
		},
		"memory": {
			"weights": [1000, 0, 0, 0, 0],
			"bias": 1000000
		},
		"cpu": {
			"weights": [100, 0, 0, 0, 0],
			"bias": 100
		}
	}`
	// t1 has 1 arg; t2 has 2 args
	t1 := &repb.ExecutionTask{Command: &repb.Command{Arguments: []string{"./tool"}}}
	t2 := &repb.ExecutionTask{Command: &repb.Command{Arguments: []string{"./tool", "--flag"}}}

	m, err := tasksize_model.New(paramsJSON)
	require.NoError(t, err)

	s1 := m.Predict(t1)

	require.Equal(t, int64(1000000+1*1000), s1.EstimatedMemoryBytes)
	require.Equal(t, int64(100+1*100), s1.EstimatedMilliCpu)

	s2 := m.Predict(t2)

	require.Equal(t, int64(1000000+2*1000), s2.EstimatedMemoryBytes)
	require.Equal(t, int64(100+2*100), s2.EstimatedMilliCpu)
}
