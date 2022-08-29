//go:build linux && !android
// +build linux,!android

package tasksize_model

import (
	"encoding/json"
	"flag"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	tf "github.com/tensorflow/tensorflow/tensorflow/go"
)

var (
	rootPath = flag.String("remote_execution.task_size_model.root_path", "", "Root dir containing saved Tensorflow models (cpu, mem) and features.json configuration.")
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

type TFModel struct {
	// Data needed to compute feature vectors ("X").

	OSValues   []string `json:"os_values"`
	ArchValues []string `json:"arch_values"`
	Tools      []string `json:"tools"`
	TestSizes  []string `json:"test_sizes"`
	Extensions []string `json:"extensions"`

	extensionIndex map[string]int

	memoryModel *tf.SavedModel
	cpuModel    *tf.SavedModel
}

// New returns a TensorFlow model configured from the root path.
func New() (Model, error) {
	m := &TFModel{}

	f, err := os.Open(filepath.Join(*rootPath, "features.json"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, m); err != nil {
		return nil, err
	}
	m.extensionIndex = make(map[string]int, len(m.Extensions))
	for i, v := range m.Extensions {
		m.extensionIndex[v] = i
	}

	// Now load the Tensorflow models.
	const defaultTag = "serve"
	memoryModel, err := tf.LoadSavedModel(filepath.Join(*rootPath, "mem"), []string{defaultTag}, nil)
	if err != nil {
		panic(err)
	}
	m.memoryModel = memoryModel
	cpuModel, err := tf.LoadSavedModel(filepath.Join(*rootPath, "cpu"), []string{defaultTag}, nil)
	if err != nil {
		panic(err)
	}
	m.cpuModel = cpuModel

	x := m.featureVector(&repb.ExecutionTask{})
	xt, err := tf.NewTensor(x)
	if err != nil {
		return nil, err
	}

	// Run a test prediction to ensure the feature vector shape matches the
	// model's expected input shape.
	if _, err := m.predict(m.memoryModel, xt); err != nil {
		return nil, status.InternalErrorf("saved memory model test prediction failed: %s", err)
	}
	if _, err := m.predict(m.cpuModel, xt); err != nil {
		return nil, status.InternalErrorf("saved CPU model test prediction failed: %s", err)
	}

	log.Infof("Initialized task size Tensorflow model from %s", *rootPath)
	return m, nil
}

func (m *TFModel) predict(model *tf.SavedModel, xt *tf.Tensor) (float32, error) {
	result, err := model.Session.Run(
		map[tf.Output]*tf.Tensor{
			model.Graph.Operation("serving_default_normalization_input").Output(0): xt,
		},
		[]tf.Output{
			model.Graph.Operation("StatefulPartitionedCall").Output(0),
		},
		nil,
	)
	if err != nil {
		return 0, err
	}
	if len(result) != 1 {
		return 0, status.InternalErrorf("unexpected result count %d", len(result))
	}
	t := result[0]
	v, ok := t.Value().([][]float32)
	if !ok {
		return 0, status.InternalErrorf("unexpected result type %T", t.Value())
	}
	if len(v) != 1 || len(v[0]) != 1 {
		return 0, status.InternalErrorf("unexpected result shape %v", t.Shape())
	}
	return v[0][0], nil
}

// Predict predicts the resource usage of a task based on the configured
// model parameters. It returns nil if the model is not configured.
func (m *TFModel) Predict(task *repb.ExecutionTask) *scpb.TaskSize {
	if !isPredictionEnabled(task) {
		return nil
	}

	x := m.featureVector(task)
	xt, err := tf.NewTensor(x)
	if err != nil {
		log.Warningf("Failed to create tensor: %s", err)
		return nil
	}

	mem, err := m.predict(m.memoryModel, xt)
	if err != nil {
		log.Warningf("Task size memory prediction failed: %s", err)
		return nil
	}
	cpu, err := m.predict(m.cpuModel, xt)
	if err != nil {
		log.Warningf("Task size CPU prediction failed: %s", err)
	}

	size := &scpb.TaskSize{
		EstimatedMemoryBytes: int64(mem),
		EstimatedMilliCpu:    int64(cpu),
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
func (m *TFModel) featureVector(task *repb.ExecutionTask) []float32 {
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
func (m *TFModel) appendExtensionCounts(features []float32, cmd *repb.Command) []float32 {
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
