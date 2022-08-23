package tasksize_model

import (
	"encoding/json"
	"math"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

const (
	// minMemoryPredictionBytes is the minimum memory value to be returned
	// from Predict().
	minMemoryPredictionBytes = 1 * 1e6 // 1MB

	// minMilliCPUPrediction is the minimum CPU value to be returned from
	// Predict().
	minMilliCPUPrediction = 1 // 1ms
)

var (
	// fileExtensionRegexp matches file extensions. This is the same regexp
	// used when training the model.
	fileExtensionRegexp = regexp.MustCompile(`\.[a-z0-9]{1,4}$`)
)

type Normalization struct {
	Mean     []float32 `json:"mean"`
	Variance []float32 `json:"variance"`
}

func (n *Normalization) Apply(x []float32) {
	for i := range x {
		x[i] = (x[i] - n.Mean[i]) / float32(math.Sqrt(float64(n.Variance[i])))
		if isNaN(x[i]) {
			x[i] = 0
		}
	}
}

type Regression struct {
	Weights []float32 `json:"weights"`
	Bias    float32   `json:"bias"`
}

func (r *Regression) Evaluate(x []float32) float32 {
	return dot(r.Weights, x) + r.Bias
}

type Model struct {
	// Data needed to compute feature vectors ("X"):

	OSValues   []string `json:"os_values"`
	ArchValues []string `json:"arch_values"`
	Tools      []string `json:"tools"`
	TestSizes  []string `json:"test_sizes"`
	Extensions []string `json:"extensions"`

	extensionIndex map[string]int

	// Parameters used in the formula to compute resource usage ("Y"):

	// Normalization is the normalization to apply to the input feature vector.
	Normalization *Normalization `json:"normalization"`

	// Memory holds the linear regression equation parameters for computing
	// memory from the normalized feature vector.
	Memory *Regression `json:"memory"`

	// CPU holds the linear regression equation parameters for computing CPU
	// usage from the normalized feature vector.
	CPU *Regression `json:"cpu"`
}

// New initializes the prediction model from JSON configuration.
func New(paramsJSON string) (*Model, error) {
	m := &Model{}
	if err := json.Unmarshal([]byte(paramsJSON), m); err != nil {
		return nil, err
	}
	m.extensionIndex = make(map[string]int, len(m.Extensions))
	for i, v := range m.Extensions {
		m.extensionIndex[v] = i
	}

	// Validate params to ensure there will be no panic at runtime when taking
	// dot products.
	x := m.featureVector(&repb.ExecutionTask{})
	if len(x) != len(m.Normalization.Mean) {
		return nil, status.InvalidArgumentErrorf("normalization.mean params length (%d) does not match feature vector length (%d)", len(m.Normalization.Mean), len(x))
	}
	if len(x) != len(m.Normalization.Variance) {
		return nil, status.InvalidArgumentErrorf("normalization.variance params length (%d) does not match feature vector length (%d)", len(m.Normalization.Variance), len(x))
	}
	if len(x) != len(m.Memory.Weights) {
		return nil, status.InvalidArgumentErrorf("memory.weights params length (%d) does not match feature vector length (%d)", len(m.Memory.Weights), len(x))
	}
	if len(x) != len(m.CPU.Weights) {
		return nil, status.InvalidArgumentErrorf("cpu.weights params length (%d) does not match feature vector length (%d)", len(m.CPU.Weights), len(x))
	}

	return m, nil
}

// Predict predicts the resource usage of a task based on the configured
// model parameters. It returns nil if the model is not configured.
func (m *Model) Predict(task *repb.ExecutionTask) *scpb.TaskSize {
	// Don't use predicted task sizes for Firecracker tasks for now, since task
	// sizes are used as hard limits on allowed resources.
	props := platform.ParseProperties(task)
	// If a task size is explicitly requested, measured task size is not used.
	if props.EstimatedComputeUnits != 0 {
		return nil
	}
	if props.WorkloadIsolationType == string(platform.FirecrackerContainerType) {
		return nil
	}
	if props.DisablePredictedTaskSize {
		return nil
	}

	x := m.featureVector(task)
	m.Normalization.Apply(x)

	size := &scpb.TaskSize{
		EstimatedMemoryBytes: int64(m.Memory.Evaluate(x)),
		EstimatedMilliCpu:    int64(m.CPU.Evaluate(x)),
	}
	// Apply a minimum to safeguard against overscheduling in cases where the
	// model underestimates several tasks in a short time period. This also
	// prevents returning negative values, which the model does not guard
	// against on its own.
	if size.EstimatedMemoryBytes < minMemoryPredictionBytes {
		size.EstimatedMemoryBytes = minMemoryPredictionBytes
	}
	if size.EstimatedMilliCpu < minMilliCPUPrediction {
		size.EstimatedMilliCpu = minMilliCPUPrediction
	}
	return size
}

// featureVector converts a command to a fixed-length numeric feature vector.
func (m *Model) featureVector(task *repb.ExecutionTask) []float32 {
	cmd := task.GetCommand()
	argCount := len(cmd.GetArguments())
	tool := ""
	if len(cmd.GetArguments()) > 0 {
		tool = filepath.Base(cmd.GetArguments()[0])
	}

	features := make(
		[]float32, 0,
		5+len(m.OSValues)+len(m.ArchValues)+len(m.Tools)+len(m.TestSizes)+
			len(m.Extensions))

	features = append(features,
		float32(argCount),
		float32(task.GetAction().GetInputRootDigest().GetSizeBytes()),
		float32(task.GetAction().GetCommandDigest().GetSizeBytes()),
		envFloat32(cmd, "TEST_TIMEOUT", 300),
		envFloat32(cmd, "TEST_TOTAL_SHARDS", 1),
	)
	features = appendOneHot(features, m.OSValues, platform.FindValue(cmd.GetPlatform(), "OSFamily"))
	features = appendOneHot(features, m.ArchValues, platform.FindValue(cmd.GetPlatform(), "Arch"))
	features = appendOneHot(features, m.Tools, tool)
	features = appendOneHot(features, m.TestSizes, envValue(cmd, "TEST_SIZE"))
	features = m.appendExtensionCounts(features, cmd)

	return features
}

// appendExtensionCounts appends the number of occurences of each extension
// without allocating a new array.
func (m *Model) appendExtensionCounts(features []float32, cmd *repb.Command) []float32 {
	counts := features[len(features) : len(features)+len(m.Extensions)]
	var args []string
	if len(cmd.GetArguments()) >= 1 {
		args = cmd.GetArguments()[1:]
	}
	for _, arg := range args {
		ext := fileExtensionRegexp.FindString(arg)
		if ext == "" {
			continue
		}
		i, ok := m.extensionIndex[ext]
		if !ok {
			continue
		}
		counts[i] += 1
	}
	return features[:len(features)+len(counts)]
}

// appendOneHot appends a one-hot vector into the given feature vector without
// allocating a new array.
func appendOneHot(features []float32, values []string, value string) []float32 {
	oneHot := features[len(features) : len(features)+len(values)]
	for i, v := range values {
		if v == value {
			oneHot[i] = 1
			break
		}
	}
	return features[:len(features)+len(oneHot)]
}

func envValue(cmd *repb.Command, key string) string {
	for _, e := range cmd.GetEnvironmentVariables() {
		if e.GetName() == key {
			return e.GetValue()
		}
	}
	return ""
}

func envFloat32(cmd *repb.Command, key string, defaultValue float32) float32 {
	str := envValue(cmd, key)
	if str == "" {
		return defaultValue
	}
	f, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return defaultValue
	}
	return float32(f)
}

// dot computes the dot product of two vectors with the same length.
func dot(a, b []float32) float32 {
	p := float32(0)
	for i := range a {
		p += a[i] * b[i]
	}
	return p
}

func isNaN(f float32) bool {
	return f != f
}
