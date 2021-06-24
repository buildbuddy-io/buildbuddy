// Package rbetest provides a test fixture for writing Remote Build Execution
// integration tests.
package rbetest

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/execution_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/priority_task_scheduler"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/task_router"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbeclient"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	retpb "github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/proto"
	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	guuid "github.com/google/uuid"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	TestUserID1    = "US1"
	ExecutorAPIKey = "EXECUTOR_API_KEY"

	testCommandBinaryRunfilePath = "enterprise/server/test/integration/remote_execution/command/testcommand_/testcommand"
	testCommandBinaryName        = "testcommand"

	// We are currently not testing non-default instances, but we should at some point.
	defaultInstanceName = ""

	defaultWaitTimeout = 20 * time.Second
)

// Env is an integration test environment for Remote Build Execution.
type Env struct {
	t                     *testing.T
	testEnv               *testenv.TestEnv
	rbeClient             *rbeclient.Client
	redisTarget           string
	buildBuddyServers     []*BuildBuddyServer
	executors             map[string]*Executor
	testCommandController *testCommandController
	// Used to generate executor names when not specified.
	executorNameCounter uint64
	envOpts             *enterprise_testenv.Options
}

func (r *Env) GetRemoteExecutionClient() repb.ExecutionClient {
	return r.buildBuddyServers[rand.Intn(len(r.buildBuddyServers))].executionClient
}

func (r *Env) GetByteStreamClient() bspb.ByteStreamClient {
	return r.buildBuddyServers[rand.Intn(len(r.buildBuddyServers))].byteStreamClient
}

func (r *Env) GetContentAddressableStorageClient() repb.ContentAddressableStorageClient {
	return r.buildBuddyServers[rand.Intn(len(r.buildBuddyServers))].casClient
}

func (r *Env) uploadInputRoot(ctx context.Context, rootDir string) *repb.Digest {
	r.testEnv.SetByteStreamClient(r.GetByteStreamClient())
	r.testEnv.SetContentAddressableStorageClient(r.GetContentAddressableStorageClient())

	digest, err := cachetools.UploadDirectoryToCAS(ctx, r.testEnv, "" /*=instanceName*/, rootDir)
	if err != nil {
		assert.FailNow(r.t, err.Error())
	}
	return digest
}

func (r *Env) setupRootDirectoryWithTestCommandBinary(ctx context.Context) *repb.Digest {
	rfp, err := bazel.Runfile(testCommandBinaryRunfilePath)
	if err != nil {
		assert.FailNow(r.t, "unable to find test binary in runfiles", err.Error())
	}
	rootDir := testfs.MakeTempDir(r.t)
	testfs.CopyFile(r.t, rfp, rootDir, testCommandBinaryName)
	return r.uploadInputRoot(ctx, rootDir)
}

// NewRBETestEnv sets up components required for testing Remote Build Execution.
// The returned environment does not have any executors by default. Use the
// Add*Executor functions to add executors.
func NewRBETestEnv(t *testing.T) *Env {
	seed := time.Now().UnixNano()
	rand.Seed(seed)
	log.Infof("Test seed: %d", seed)

	redisTarget := testredis.Start(t)
	envOpts := &enterprise_testenv.Options{RedisTarget: redisTarget}
	testEnv := enterprise_testenv.GetCustomTestEnv(t, envOpts)
	testEnv.SetAuthenticator(newTestAuthenticator())
	rbe := &Env{
		testEnv:     testEnv,
		t:           t,
		redisTarget: redisTarget,
		executors:   make(map[string]*Executor),
		envOpts:     envOpts,
	}
	rbe.testCommandController = newTestCommandController(t)
	rbe.rbeClient = rbeclient.New(rbe)
	return rbe
}

type BuildBuddyServerOptions struct {
	SchedulerServerOptions scheduler_server.Options
	ExecutionServerOptions execution_server.Options
}

// buildBuddyServerEnv is a specialized environment that allows us to return a random SchedulerClient for every
// GetSchedulerClient call.
type buildBuddyServerEnv struct {
	*testenv.TestEnv
	rbeEnv *Env
}

func (e *buildBuddyServerEnv) GetSchedulerClient() scpb.SchedulerClient {
	return e.rbeEnv.buildBuddyServers[0].schedulerClient
}

type BuildBuddyServer struct {
	t    *testing.T
	env  *buildBuddyServerEnv
	port int

	schedulerServer *scheduler_server.SchedulerServer
	executionServer repb.ExecutionServer

	// Clients used by test framework.
	executionClient  repb.ExecutionClient
	casClient        repb.ContentAddressableStorageClient
	byteStreamClient bspb.ByteStreamClient
	schedulerClient  scpb.SchedulerClient
}

func newBuildBuddyServer(t *testing.T, env *buildBuddyServerEnv, opts *BuildBuddyServerOptions) *BuildBuddyServer {
	port := app.FreePort(t)
	opts.SchedulerServerOptions.LocalPortOverride = int32(port)

	env.SetAuthenticator(newTestAuthenticator())
	router, err := task_router.New(env)
	require.NoError(t, err)
	env.SetTaskRouter(router)
	scheduler, err := scheduler_server.NewSchedulerServerWithOptions(env, &opts.SchedulerServerOptions)
	if err != nil {
		assert.FailNowf(t, "could not setup SchedulerServer", err.Error())
	}
	executionServer, err := execution_server.NewExecutionServerWithOptions(env, &opts.ExecutionServerOptions)
	if err != nil {
		assert.FailNowf(t, "could not setup ExecutionServer", err.Error())
	}

	server := &BuildBuddyServer{
		t:               t,
		env:             env,
		port:            port,
		schedulerServer: scheduler,
		executionServer: executionServer,
	}
	server.start()

	clientConn, err := grpc_client.DialTarget(fmt.Sprintf("grpc://localhost:%d", port))
	if err != nil {
		assert.FailNowf(t, "could not connect to BuildBuddy server", err.Error())
	}
	server.executionClient = repb.NewExecutionClient(clientConn)
	server.casClient = repb.NewContentAddressableStorageClient(clientConn)
	server.byteStreamClient = bspb.NewByteStreamClient(clientConn)
	server.schedulerClient = scpb.NewSchedulerClient(clientConn)

	return server
}

func (s *BuildBuddyServer) start() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		assert.FailNow(s.t, fmt.Sprintf("could not listen on port %d", s.port), err.Error())
	}
	grpcServer, grpcServerRunFunc := s.env.GRPCServer(lis)

	// Configure services needed by remote execution.

	s.env.SetSchedulerService(s.schedulerServer)
	scpb.RegisterSchedulerServer(grpcServer, s.schedulerServer)
	repb.RegisterExecutionServer(grpcServer, s.executionServer)

	byteStreamServer, err := byte_stream_server.NewByteStreamServer(s.env)
	if err != nil {
		assert.FailNowf(s.t, "could not setup ByteStreamServer", err.Error())
	}
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)

	casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(s.env)
	if err != nil {
		assert.FailNowf(s.t, "could not setup ContentAddressableStorageServer", err.Error())
	}
	repb.RegisterContentAddressableStorageServer(grpcServer, casServer)

	acServer, err := action_cache_server.NewActionCacheServer(s.env)
	if err != nil {
		assert.FailNowf(s.t, "could not setup ActionCacheServer", err.Error())
	}
	repb.RegisterActionCacheServer(grpcServer, acServer)

	go grpcServerRunFunc()
}

type testCommandController struct {
	t    *testing.T
	port int
	mu   sync.Mutex
	// Commands that were ever connected, even if they are not connected now.
	startedCommands map[string]bool
	// Channels to instruct waiters that a command has started.
	commandStartWaiters map[string]chan struct{}
	// Commands that are currently connected.
	connectedCommands map[string]chan *retpb.ControllerToCommandRequest
}

func (c *testCommandController) RegisterCommand(req *retpb.RegisterCommandRequest, stream retpb.CommandController_RegisterCommandServer) error {
	opChannel := make(chan *retpb.ControllerToCommandRequest)
	log.Printf("Test command registering: %s", req.GetCommandName())
	c.mu.Lock()
	c.startedCommands[req.GetCommandName()] = true
	if ch, ok := c.commandStartWaiters[req.GetCommandName()]; ok {
		close(ch)
		delete(c.commandStartWaiters, req.GetCommandName())
	}
	c.connectedCommands[req.GetCommandName()] = opChannel
	c.mu.Unlock()

	for {
		op := <-opChannel
		log.Printf("Sending request to command [%s]:\n%s", req.GetCommandName(), proto.MarshalTextString(req))
		err := stream.Send(op)
		if err != nil {
			log.Printf("Send failed: %v", err)
			break
		}
	}
	return nil
}

// waitStarted returns a channel that will be closed when command with given name first connects to the controller.
// Channel is closed immediately if the command has already connected.
func (c *testCommandController) waitStarted(name string) <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.commandStartWaiters[name]; ok {
		assert.FailNow(c.t, fmt.Sprintf("there's already someone waiting for %q to start", name))
	}
	ch := make(chan struct{}, 1)
	if _, ok := c.startedCommands[name]; ok {
		close(ch)
	} else {
		c.commandStartWaiters[name] = ch
	}
	return ch
}

func (c *testCommandController) exit(name string, exitCode int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch, ok := c.connectedCommands[name]
	if !ok {
		assert.FailNow(c.t, fmt.Sprintf("command %q is not connected", name))
	}
	ch <- &retpb.ControllerToCommandRequest{
		ExitOp: &retpb.ExitOp{ExitCode: exitCode},
	}
}

func newTestCommandController(t *testing.T) *testCommandController {
	port := app.FreePort(t)
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		assert.FailNowf(t, "could not listen on port", err.Error())
	}

	controller := &testCommandController{
		t:                   t,
		port:                port,
		startedCommands:     make(map[string]bool),
		commandStartWaiters: make(map[string]chan struct{}),
		connectedCommands:   make(map[string]chan *retpb.ControllerToCommandRequest),
	}

	server := grpc.NewServer()
	retpb.RegisterCommandControllerServer(server, controller)
	go server.Serve(listener)

	return controller
}

type ExecutorOptions struct {
	// Optional name that will appear in executor logs.
	// To ease debugging, you should specify an explicit name if it's important to the test on which executor an action
	// is executed.
	Name string
	// Register the executor using new API that supports work streaming.
	EnableWorkStreaming bool
	// Optional API key to be sent by executor
	APIKey string
	// Optional server to be used for task leasing, cache requests, etc
	// If not specified the executor will connect to a random server.
	Server                 *BuildBuddyServer
	resourceTrackerOptions resources.TrackerOptions
}

// Executor is a handle for a running executor instance.
type Executor struct {
	hostPort           string
	grpcServer         *grpc.Server
	cancelRegistration context.CancelFunc
}

// Stop unregisters the executor from the BuildBuddy server.
func (e *Executor) stop() {
	e.grpcServer.Stop()
	e.cancelRegistration()
}

func (r *Env) AddBuildBuddyServer() *BuildBuddyServer {
	return r.AddBuildBuddyServerWithOptions(&BuildBuddyServerOptions{})
}

func (r *Env) AddBuildBuddyServers(n int) {
	for i := 0; i < n; i++ {
		r.AddBuildBuddyServer()
	}
}

func (r *Env) AddBuildBuddyServerWithOptions(opts *BuildBuddyServerOptions) *BuildBuddyServer {
	envOpts := &enterprise_testenv.Options{RedisTarget: r.redisTarget}
	env := &buildBuddyServerEnv{TestEnv: enterprise_testenv.GetCustomTestEnv(r.t, envOpts), rbeEnv: r}

	server := newBuildBuddyServer(r.t, env, opts)
	// We're using an in-memory SQLite database so we need to make sure all servers share the same handle.
	if len(r.buildBuddyServers) > 0 {
		server.env.SetDBHandle(r.buildBuddyServers[0].env.GetDBHandle())
	}
	r.buildBuddyServers = append(r.buildBuddyServers, server)
	return server
}

// AddExecutorWithOptions brings up an executor with custom options.
// Blocks until executor registers with the scheduler.
func (r *Env) AddExecutorWithOptions(opts *ExecutorOptions) *Executor {
	executor := r.addExecutor(opts)
	r.waitForExecutorRegistration()
	return executor
}

// AddExecutor brings up an executor with an auto generated name with default settings.
// Use this function in tests where it's not important on which executor tasks are executed,
// otherwise use AddExecutorWithOptions and specify a custom Name.
// Blocks until executor registers with the scheduler.
func (r *Env) AddExecutor() *Executor {
	name := fmt.Sprintf("unnamedExecutor%d", atomic.AddUint64(&r.executorNameCounter, 1))
	return r.AddExecutorWithOptions(&ExecutorOptions{Name: name})
}

// AddSingleTaskExecutorWithOptions brings up an executor with custom options that is configured with capacity to
// accept only a single "default" sized task.
// Blocks until executor registers with the scheduler.
func (r *Env) AddSingleTaskExecutorWithOptions(options *ExecutorOptions) *Executor {
	optionsCopy := *options
	optionsCopy.resourceTrackerOptions = resources.TrackerOptions{
		RAMBytesCapacityOverride:  tasksize.DefaultMemEstimate,
		CPUMillisCapacityOverride: tasksize.DefaultCPUEstimate,
	}
	executor := r.addExecutor(&optionsCopy)
	r.waitForExecutorRegistration()
	return executor
}

// AddSingleTaskExecutor brings up an executor with an auto generated name that is configured
// with capacity to accept only a single "default" sized task.
// Use this function in tests where it's not important on which executor tasks are executed,
// otherwise use AddSingleTaskExecutorWithOptions and specify a custom Name.
// Blocks until executor registers with the scheduler.
func (r *Env) AddSingleTaskExecutor() *Executor {
	name := fmt.Sprintf("unnamedExecutor%d(single task)", atomic.AddUint64(&r.executorNameCounter, 1))
	return r.AddSingleTaskExecutorWithOptions(&ExecutorOptions{Name: name})
}

// AddNamedExecutors brings up N named executors with default settings.
// Use this function if it matters for the test on which executor tasks are executed, otherwise
// use AddExecutors.
// Blocks until all executors register with the scheduler.
func (r *Env) AddNamedExecutors(names []string) []*Executor {
	var executors []*Executor
	for _, name := range names {
		executors = append(executors, r.addExecutor(&ExecutorOptions{Name: name}))
	}
	r.waitForExecutorRegistration()
	return executors
}

// AddExecutors brings up N executors with auto generated names with default settings.
// Use this function in tests where it's not important on which executor tasks are exected,
// otherwise use AddNamedExecutors.
// Blocks until all executors register with the scheduler.
func (r *Env) AddExecutors(n int) []*Executor {
	var names []string
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("unnamedExecutor%d", atomic.AddUint64(&r.executorNameCounter, 1))
		names = append(names, name)
	}
	return r.AddNamedExecutors(names)
}

func (r *Env) addExecutor(options *ExecutorOptions) *Executor {
	buildBuddyServer := options.Server
	if buildBuddyServer == nil {
		buildBuddyServer = r.buildBuddyServers[rand.Intn(len(r.buildBuddyServers))]
	}

	clientConn, err := grpc_client.DialTarget(fmt.Sprintf("grpc://localhost:%d", buildBuddyServer.port))
	if err != nil {
		assert.FailNowf(r.t, "could not create client to BuildBuddy server", err.Error())
	}

	env := enterprise_testenv.GetCustomTestEnv(r.t, r.envOpts)
	env.SetResourceTracker(resources.NewTracker(&options.resourceTrackerOptions))
	r.t.Cleanup(func() {
		env.GetHealthChecker().Shutdown()
		log.Infof("Waiting for executor %q to shut down.", options.Name)
		env.GetHealthChecker().WaitForGracefulShutdown()
		log.Infof("Shut down for executor %q completed.", options.Name)
	})

	env.SetRemoteExecutionClient(repb.NewExecutionClient(clientConn))
	env.SetSchedulerClient(scpb.NewSchedulerClient(clientConn))
	env.SetAuthenticator(newTestAuthenticator())

	executorConfig := env.GetConfigurator().GetExecutorConfig()
	fc, err := filecache.NewFileCache(executorConfig.LocalCacheDirectory, executorConfig.LocalCacheSizeBytes)
	if err != nil {
		assert.FailNow(r.t, "create file cache", err)
	}
	env.SetFileCache(fc)

	localServer, startLocalServer := env.LocalGRPCServer()
	casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	if err != nil {
		assert.FailNowf(r.t, "could not create CAS server", err.Error())
	}
	repb.RegisterContentAddressableStorageServer(localServer, casServer)

	byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
	if err != nil {
		assert.FailNowf(r.t, "could not create ByteStream server", err.Error())
	}
	bspb.RegisterByteStreamServer(localServer, byteStreamServer)

	actionCacheServer, err := action_cache_server.NewActionCacheServer(env)
	if err != nil {
		assert.FailNowf(r.t, "could not create ActionCache server", err.Error())
	}
	repb.RegisterActionCacheServer(localServer, actionCacheServer)
	go startLocalServer()

	localConn, err := env.LocalGRPCConn(context.Background())
	if err != nil {
		assert.FailNowf(r.t, "could not connect to executor GRPC server", err.Error())
	}

	env.SetActionCacheClient(repb.NewActionCacheClient(localConn))
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(localConn))
	env.SetByteStreamClient(bspb.NewByteStreamClient(localConn))

	executorUUID, err := guuid.NewRandom()
	require.NoError(r.t, err)
	executorID := executorUUID.String()

	exec, err := executor.NewExecutor(env, executorID, &executor.Options{NameOverride: options.Name})
	if err != nil {
		assert.FailNowf(r.t, fmt.Sprintf("could not create executor %q", options.Name), err.Error())
	}
	taskScheduler := priority_task_scheduler.NewPriorityTaskScheduler(env, exec)
	taskScheduler.Start()

	executorPort := app.FreePort(r.t)
	execLis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", executorPort))
	if err != nil {
		assert.FailNowf(r.t, fmt.Sprintf("could not listen on port %d", executorPort), err.Error())
	}
	executorGRPCServer, execRunFunc := env.GRPCServer(execLis)

	scpb.RegisterQueueExecutorServer(executorGRPCServer, taskScheduler)

	go execRunFunc()

	ctx, cancel := context.WithCancel(context.Background())

	opts := &scheduler_client.Options{
		PortOverride:        int32(executorPort),
		HostnameOverride:    "localhost",
		NodeNameOverride:    options.Name,
		EnableWorkStreaming: options.EnableWorkStreaming,
		APIKeyOverride:      options.APIKey,
	}
	registration, err := scheduler_client.NewRegistration(env, taskScheduler, executorID, opts)
	if err != nil {
		assert.FailNowf(r.t, "could not create executor registration", err.Error())
	}
	registration.Start(ctx)

	executor := &Executor{
		hostPort:           fmt.Sprintf("localhost:%d", executorPort),
		grpcServer:         executorGRPCServer,
		cancelRegistration: cancel,
	}
	r.executors[executor.hostPort] = executor
	return executor
}

func newTestAuthenticator() *testauth.TestAuthenticator {
	users := testauth.TestUsers(TestUserID1, "GR1")
	users[ExecutorAPIKey] = &testauth.TestUser{
		GroupID:       "GR123",
		AllowedGroups: []string{"GR123"},
		Capabilities:  []akpb.ApiKey_Capability{akpb.ApiKey_REGISTER_EXECUTOR_CAPABILITY},
	}
	return testauth.NewTestAuthenticator(users)
}

func (r *Env) RemoveExecutor(executor *Executor) {
	if _, ok := r.executors[executor.hostPort]; !ok {
		assert.FailNow(r.t, fmt.Sprintf("Executor %q not in executor map", executor.hostPort))
	}
	executor.stop()
	delete(r.executors, executor.hostPort)
	r.waitForExecutorRegistration()
}

// waitForExecutorRegistration waits until the set of all registered executors matches expected internal set.
func (r *Env) waitForExecutorRegistration() {
	expectedNodesByHostIp := make(map[string]bool)
	for _, e := range r.executors {
		expectedNodesByHostIp[e.hostPort] = true
	}

	nodesByHostIp := make(map[string]bool)
	deadline := time.Now().Add(defaultWaitTimeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()

		nodesByHostIp = make(map[string]bool)

		// Doesn't matter which server we query.
		nodes, err := r.buildBuddyServers[0].schedulerServer.GetAllExecutionNodes(ctx)
		if err != nil {
			assert.FailNowf(r.t, "could not fetch registered execution nodes", err.Error())
		}
		for _, node := range nodes {
			nodesByHostIp[fmt.Sprintf("%s:%d", node.Host, node.Port)] = true
		}
		if reflect.DeepEqual(expectedNodesByHostIp, nodesByHostIp) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	require.Equal(r.t, expectedNodesByHostIp, nodesByHostIp, "set of registered executors should converge")
}

func (r *Env) DownloadOutputsToNewTempDir(res *CommandResult) string {
	tmpDir := testfs.MakeTempDir(r.t)

	env := enterprise_testenv.GetCustomTestEnv(r.t, r.envOpts)
	env.SetByteStreamClient(r.GetByteStreamClient())
	env.SetContentAddressableStorageClient(r.GetContentAddressableStorageClient())
	// TODO: Does the context need the user ID if the CommandResult was produced
	// by an authenticated user?
	if err := r.rbeClient.DownloadActionOutputs(context.Background(), env, res.CommandResult, tmpDir); err != nil {
		assert.FailNow(r.t, "failed to download action outputs", err.Error())
	}
	return tmpDir
}

type Command struct {
	env *Env
	*rbeclient.Command
	rbeClient *rbeclient.Client
	userID    string
}

type CommandResult struct {
	*rbeclient.CommandResult
	Stdout string
	Stderr string
}

// Wait blocks until the command has finished executing.
func (c *Command) Wait() *CommandResult {
	timeout := time.NewTimer(defaultWaitTimeout)
	for {
		select {
		case status, ok := <-c.StatusChannel():
			if !ok {
				assert.FailNow(c.env.t, fmt.Sprintf("command %q did not send a result", c.Name))
			}
			if status.Stage != repb.ExecutionStage_COMPLETED {
				continue
			}
			timeout.Stop()
			if status.Err != nil {
				assert.FailNowf(c.env.t, fmt.Sprintf("command %q did not finish succesfully", c.Name), status.Err.Error())
			}
			ctx := context.Background()
			ctx = c.env.withUserID(ctx, c.userID)
			stdout, stderr, err := c.rbeClient.GetStdoutAndStderr(ctx, status)
			if err != nil {
				assert.FailNowf(c.env.t, "could not fetch outputs", err.Error())
			}
			return &CommandResult{
				CommandResult: status,
				Stdout:        stdout,
				Stderr:        stderr,
			}
		case <-timeout.C:
			assert.FailNow(c.env.t, fmt.Sprintf("command %q did not finish within timeout", c.Name))
			return nil
		}
	}
}

func (c *Command) WaitAccepted() {
	select {
	case <-c.AcceptedChannel():
		// Command accepted.
	case <-time.After(defaultWaitTimeout):
		assert.FailNow(c.env.t, fmt.Sprintf("command %q was not accepted within timeout", c.Name))
	}
}

func (c *Command) ReplaceWaitUsingWaitExecutionAPI() {
	err := c.Command.ReplaceWaitUsingWaitExecutionAPI(context.Background())
	if err != nil {
		assert.FailNow(c.env.t, "could not switch from Execute to WaitExecution API", err.Error())
	}
}

// ControlledCommand is a handle for a remotely executed command which can be controlled from the test.
// By default the controlled command blocks until it receives further instructions from the test.
type ControlledCommand struct {
	t *testing.T
	*Command
	controller *testCommandController
}

// WaitStarted blocks until the command has started running on an executor.
func (c *ControlledCommand) WaitStarted() {
	ch := c.controller.waitStarted(c.Name)
	select {
	case <-ch:
		// Command successfully started.
	case <-time.After(defaultWaitTimeout):
		assert.FailNow(c.t, fmt.Sprintf("command %q did not start running within timeout", c.Name))
	}
}

// Exit instructs the controlled command to exit with the given exit code.
func (c *ControlledCommand) Exit(exitCode int32) {
	c.controller.exit(c.Name, exitCode)
}

// ExecuteControlledCommand anonymously executes a special test command binary
// that connects to a controller server and blocks until it receives further
// instructions from the controller.
// The returned handle is used to send instructions to the test command.
func (r *Env) ExecuteControlledCommand(name string) *ControlledCommand {
	ctx := context.Background()
	args := []string{"./" + testCommandBinaryName, "--name", name, "--controller", fmt.Sprintf("localhost:%d", r.testCommandController.port)}

	ctx, err := prefix.AttachUserPrefixToContext(ctx, r.testEnv)
	if err != nil {
		assert.FailNowf(r.t, "could not attach user prefix", err.Error())
	}

	inputRootDigest := r.setupRootDirectoryWithTestCommandBinary(ctx)

	cmd, err := r.rbeClient.PrepareCommand(ctx, defaultInstanceName, name, inputRootDigest, minimalCommand(args...))
	if err != nil {
		assert.FailNow(r.t, fmt.Sprintf("Could not prepare command %q", name), err.Error())
	}

	err = cmd.Start(ctx)
	if err != nil {
		assert.FailNow(r.t, fmt.Sprintf("Could not execute command %q", name), err.Error())
	}
	return &ControlledCommand{
		t:          r.t,
		Command:    &Command{r, cmd, r.rbeClient, "" /*=userID*/},
		controller: r.testCommandController,
	}
}

// ExecuteCustomCommand anonymously executes the command specified by args.
func (r *Env) ExecuteCustomCommand(args ...string) *Command {
	return r.Execute(minimalCommand(args...), &ExecuteOpts{})
}

func (r *Env) withUserID(ctx context.Context, userID string) context.Context {
	ctx = r.testEnv.GetAuthenticator().AuthContextFromAPIKey(ctx, userID)
	jwt, _ := testauth.TestJWTForUserID(userID)
	ctx = metadata.AppendToOutgoingContext(
		ctx,
		// Test authenticator treats user IDs as both API keys and JWTs.
		testauth.APIKeyHeader, userID,
		"x-buildbuddy-jwt", jwt,
	)
	return ctx
}

type ExecuteOpts struct {
	// InputRootDir is the path to the dir containing inputs for the command.
	InputRootDir string
	// UserID is the ID of the authenticated user that should execute the command.
	UserID string
}

func (r *Env) Execute(command *repb.Command, opts *ExecuteOpts) *Command {
	ctx := context.Background()
	if opts.UserID != "" {
		ctx = r.withUserID(ctx, opts.UserID)
	}

	ctx, err := prefix.AttachUserPrefixToContext(ctx, r.testEnv)
	if err != nil {
		assert.FailNowf(r.t, "could not attach user prefix", err.Error())
	}

	var inputRootDigest *repb.Digest
	if opts.InputRootDir != "" {
		inputRootDigest = r.uploadInputRoot(ctx, opts.InputRootDir)
	} else {
		inputRootDigest = r.setupRootDirectoryWithTestCommandBinary(ctx)
	}

	name := strings.Join(command.GetArguments(), " ")
	cmd, err := r.rbeClient.PrepareCommand(ctx, defaultInstanceName, name, inputRootDigest, command)
	if err != nil {
		assert.FailNowf(r.t, fmt.Sprintf("unable to request action execution for command %q", name), err.Error())
	}

	err = cmd.Start(ctx)
	if err != nil {
		assert.FailNow(r.t, fmt.Sprintf("Could not execute command %q", name), err.Error())
	}
	return &Command{r, cmd, r.rbeClient, opts.UserID}
}

func minimalCommand(args ...string) *repb.Command {
	return &repb.Command{
		Arguments: args,
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "container-image", Value: "none"},
			},
		},
	}
}
