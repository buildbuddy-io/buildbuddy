package tasksize_model

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	tf_framework "github.com/buildbuddy-io/tensorflow-proto/tensorflow/core/framework"
	tf "github.com/buildbuddy-io/tensorflow-proto/tensorflow_serving/apis"
)

var (
	featuresConfigPath = flag.String("remote_execution.task_size_model.features_config_path", "", "Path pointing to features.json config file.")
	servingAddress     = flag.String("remote_execution.task_size_model.serving_address", "", "gRPC address pointing to TensorFlow Serving prediction service with task size models (cpu, mem).")
)

const (
	// minMemoryPredictionBytes is the minimum memory value to be returned
	// from Predict().
	minMemoryPredictionBytes = 1 * 1e6 // 1MB

	// minMilliCPUPrediction is the minimum CPU value to be returned from
	// Predict().
	minMilliCPUPrediction = 1 // 1ms

	// How much time to allow for initializing the model (connecting to the
	// TF server and making a test prediction).
	initTimeout = 10 * time.Second

	// How long to wait before giving up on an individual prediction.
	// This is intentionally very short because in the worst case where all
	// requests are failing, this can become a bottleneck for RBE performance.
	predictionTimeout = 5 * time.Millisecond

	// Names referencing the saved model.

	memModelName    = "mem"
	cpuModelName    = "cpu"
	modelInputName  = "x_input"
	modelOutputName = "y"
)

var (
	// fileExtensionRegexp matches file extensions. This is the same regexp
	// used when training the model.
	fileExtensionRegexp = regexp.MustCompile(`\.[a-z0-9]{1,4}$`)
)

type FeaturesConfig struct {
	OSValues   []string `json:"os_values"`
	ArchValues []string `json:"arch_values"`
	Tools      []string `json:"tools"`
	TestSizes  []string `json:"test_sizes"`
	Extensions []string `json:"extensions"`
}

type Model struct {
	config         *FeaturesConfig
	extensionIndex map[string]int

	client tf.PredictionServiceClient

	readyMu sync.RWMutex
	ready   bool
}

// New returns a TensorFlow model configured from the root path.
func New(env environment.Env) (*Model, error) {
	m := &Model{}

	f, err := os.Open(*featuresConfigPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	m.config = &FeaturesConfig{}
	if err := json.Unmarshal(b, m.config); err != nil {
		return nil, err
	}
	m.extensionIndex = make(map[string]int, len(m.config.Extensions))
	for i, v := range m.config.Extensions {
		m.extensionIndex[v] = i
	}

	// Connect to TF serving.
	conn, err := grpc_client.DialSimple(*servingAddress)
	if err != nil {
		return nil, status.UnavailableErrorf("failed to dial TensorFlow serving at %s: %s", *servingAddress, err)
	}
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		return conn.Close()
	})
	m.client = tf.NewPredictionServiceClient(conn)

	// Run a test prediction in the background to ensure the feature vector
	// shape matches the model's expected input shape.
	go func() {
		if err := m.makeTestPrediction(env.GetServerContext()); err != nil {
			log.Errorf("Failed to initialize task size Tensorflow model: %s", err)
			return
		}
		m.readyMu.Lock()
		m.ready = true
		m.readyMu.Unlock()
		log.Infof("Initialized task size Tensorflow model: read features config from %s; connected to %s", *featuresConfigPath, *servingAddress)
	}()

	return m, nil
}

func (m *Model) makeTestPrediction(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, initTimeout)
	defer cancel()

	x := m.featureVector(&repb.ExecutionTask{})
	if _, err := m.callModel(ctx, memModelName, x); err != nil {
		return status.InternalErrorf("saved memory model test prediction failed: %s", err)
	}
	if _, err := m.callModel(ctx, cpuModelName, x); err != nil {
		return status.InternalErrorf("saved CPU model test prediction failed: %s", err)
	}
	return nil
}

func (m *Model) isReady() bool {
	m.readyMu.RLock()
	defer m.readyMu.RUnlock()
	return m.ready
}

func (m *Model) callModel(ctx context.Context, model string, x []float32) (float32, error) {
	req := &tf.PredictRequest{
		ModelSpec: &tf.ModelSpec{Name: model},
		Inputs: map[string]*tf_framework.TensorProto{
			modelInputName: {
				Dtype: tf_framework.DataType_DT_FLOAT,
				TensorShape: &tf_framework.TensorShapeProto{
					Dim: []*tf_framework.TensorShapeProto_Dim{
						{Size: 1},
						{Size: int64(len(x))},
					},
				},
				FloatVal: x,
			},
		},
		OutputFilter: []string{modelOutputName},
	}
	rsp, err := m.client.Predict(ctx, req)
	if err != nil {
		return 0, err
	}
	out, ok := rsp.Outputs[modelOutputName]
	if !ok {
		return 0, status.InternalErrorf("missing '%s' output in model response", modelOutputName)
	}
	if len(out.GetFloatVal()) != 1 {
		return 0, status.InternalErrorf("output has unexpected length %d", len(out.GetFloatVal()))
	}
	return out.GetFloatVal()[0], nil
}

// Predict predicts the resource usage of a task based on the configured
// model parameters. It returns nil if the model is not configured.
func (m *Model) Predict(ctx context.Context, task *repb.ExecutionTask) *scpb.TaskSize {
	if !m.isReady() {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, predictionTimeout)
	defer cancel()

	// Don't use predicted task sizes for Firecracker tasks for now, since task
	// sizes are used as hard limits on allowed resources.
	props, err := platform.ParseProperties(task)
	if err != nil {
		log.CtxInfof(ctx, "Failed to parse task properties: %s", err)
		return nil
	}
	// If a task size is explicitly requested, measured task size is not used.
	if props.EstimatedComputeUnits != 0 {
		return nil
	}
	// Don't use predicted task sizes for Firecracker tasks for now, since task
	// sizes are used as hard limits on allowed resources.
	if props.WorkloadIsolationType == string(platform.FirecrackerContainerType) {
		return nil
	}
	if props.DisablePredictedTaskSize {
		return nil
	}

	start := time.Now()
	s, err := m.predict(ctx, task)
	metrics.RemoteExecutionTaskSizePredictionDurationUsec.With(prometheus.Labels{
		metrics.StatusHumanReadableLabel: status.MetricsLabel(err),
	}).Observe(float64(time.Since(start).Microseconds()))
	if err != nil {
		log.Warningf("Failed to predict task size: %s", err)
		return nil
	}
	return s
}

func (m *Model) predict(ctx context.Context, task *repb.ExecutionTask) (*scpb.TaskSize, error) {
	x := m.featureVector(task)

	eg, ctx := errgroup.WithContext(ctx)
	var mem, cpu int64
	eg.Go(func() error {
		y, err := m.callModel(ctx, memModelName, x)
		if err != nil {
			return err
		}
		mem = int64(y)
		return nil
	})
	eg.Go(func() error {
		y, err := m.callModel(ctx, cpuModelName, x)
		if err != nil {
			return err
		}
		cpu = int64(y)
		return nil
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	size := &scpb.TaskSize{
		EstimatedMemoryBytes: mem,
		EstimatedMilliCpu:    cpu,
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
	return size, nil
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
		5+len(m.config.OSValues)+len(m.config.ArchValues)+len(m.config.Tools)+len(m.config.TestSizes)+
			len(m.config.Extensions))

	features = append(features,
		float32(argCount),
		float32(task.GetAction().GetInputRootDigest().GetSizeBytes()),
		float32(task.GetAction().GetCommandDigest().GetSizeBytes()),
		envFloat32(cmd, "TEST_TIMEOUT", 300),
		envFloat32(cmd, "TEST_TOTAL_SHARDS", 1),
	)
	features = appendOneHot(features, m.config.OSValues, platform.FindValue(cmd.GetPlatform(), "OSFamily"))
	features = appendOneHot(features, m.config.ArchValues, platform.FindValue(cmd.GetPlatform(), "Arch"))
	features = appendOneHot(features, m.config.Tools, tool)
	features = appendOneHot(features, m.config.TestSizes, envValue(cmd, "TEST_SIZE"))
	features = m.appendExtensionCounts(features, cmd)

	return features
}

// appendExtensionCounts appends the number of occurences of each extension
// without allocating a new array.
func (m *Model) appendExtensionCounts(features []float32, cmd *repb.Command) []float32 {
	counts := features[len(features) : len(features)+len(m.config.Extensions)]
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
