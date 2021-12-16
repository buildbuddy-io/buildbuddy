// Package rbetest provides a test fixture for writing Remote Build Execution
// integration tests.
package rbetest

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_server"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/fileresolver"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	bundle "github.com/buildbuddy-io/buildbuddy/enterprise"
	retpb "github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/proto"
	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	guuid "github.com/google/uuid"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	ExecutorAPIKey = "EXECUTOR_API_KEY"

	testCommandBinaryRunfilePath = "enterprise/server/test/integration/remote_execution/command/testcommand_/testcommand"
	testCommandBinaryName        = "testcommand"

	// We are currently not testing non-default instances, but we should at some point.
	defaultInstanceName = ""

	defaultWaitTimeout = 20 * time.Second
)

func init() {
	// Set umask to match the executor process.
	syscall.Umask(0)
}

// Env is an integration test environment for Remote Build Execution.
type Env struct {
	t                     *testing.T
	testEnv               *testenv.TestEnv
	rbeClient             *rbeclient.Client
	redisTarget           string
	rootDataDir           string
	buildBuddyServers     []*BuildBuddyServer
	executors             map[string]*Executor
	testCommandController *testCommandController
	// Used to generate executor names when not specified.
	executorNameCounter uint64
	envOpts             *enterprise_testenv.Options

	UserID1         string
	GroupID1        string
	ExecutorGroupID string
}

func (r *Env) GetBuildBuddyServiceClient() bbspb.BuildBuddyServiceClient {
	return r.buildBuddyServers[rand.Intn(len(r.buildBuddyServers))].buildBuddyServiceClient
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

	digest, _, err := cachetools.UploadDirectoryToCAS(ctx, r.testEnv, "" /*=instanceName*/, rootDir)
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
	// Create a user and group in the DB for use in tests (this will also create
	// an API key for the group).
	// TODO(http://go/b/949): Add a fake OIDC provider and then just have a real
	// user log into the app to do all of this setup in a more sane way.
	orgURLID := "test"
	userID := "US1"
	ctx := context.Background()
	err := testEnv.GetUserDB().InsertUser(ctx, &tables.User{
		UserID: userID,
		Email:  "user@example.com",
	})
	require.NoError(t, err)
	groupID, err := testEnv.GetUserDB().InsertOrUpdateGroup(ctx, &tables.Group{
		URLIdentifier: &orgURLID,
		UserID:        userID,
	})
	require.NoError(t, err)
	err = testEnv.GetUserDB().AddUserToGroup(ctx, userID, groupID)
	require.NoError(t, err)
	// Update the API key value to match the user ID, since the test authenticator
	// treats user IDs and API keys the same.
	err = testEnv.GetDBHandle().Exec(
		`UPDATE APIKeys SET value = ? WHERE group_id = ?`, userID, groupID).Error
	require.NoError(t, err)
	// Create executor group
	execGroupSlug := "executor-group"
	executorGroupID, err := testEnv.GetUserDB().InsertOrUpdateGroup(ctx, &tables.Group{
		URLIdentifier: &execGroupSlug,
		UserID:        userID,
	})
	require.NoError(t, err)
	// Note: This root data dir (under which all executors' data is placed) does
	// not get cleaned up until after all executors are shutdown (in the cleanup
	// func below), since test cleanup funcs are run in LIFO order.
	rootDataDir := testfs.MakeTempDir(t)
	rbe := &Env{
		testEnv:         testEnv,
		t:               t,
		redisTarget:     redisTarget,
		executors:       make(map[string]*Executor),
		envOpts:         envOpts,
		GroupID1:        groupID,
		UserID1:         userID,
		ExecutorGroupID: executorGroupID,
		rootDataDir:     rootDataDir,
	}
	testEnv.SetAuthenticator(rbe.newTestAuthenticator())
	rbe.testCommandController = newTestCommandController(t)
	rbe.rbeClient = rbeclient.New(rbe)

	t.Cleanup(func() {
		log.Warningf("Shutting down executors...")
		var wg sync.WaitGroup
		for id, e := range rbe.executors {
			id, e := id, e
			e.env.GetHealthChecker().Shutdown()
			wg.Add(1)
			go func() {
				log.Infof("Waiting for executor %q to shut down.", id)
				e.env.GetHealthChecker().WaitForGracefulShutdown()
				wg.Done()
			}()
			log.Infof("Shut down for executor %q completed.", id)
		}
		log.Warningf("Waiting for executor shutdown to finish...")
		wg.Wait()
	})

	return rbe
}

type BuildBuddyServerOptions struct {
	SchedulerServerOptions scheduler_server.Options

	// EnvModifier modifies the environment before starting the BuildBuddy server.
	EnvModifier func(env *testenv.TestEnv)
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

	schedulerServer         *scheduler_server.SchedulerServer
	executionServer         repb.ExecutionServer
	buildBuddyServiceServer *buildbuddy_server.BuildBuddyServer
	buildEventServer        *build_event_server.BuildEventProtocolServer

	// Clients used by test framework.
	executionClient         repb.ExecutionClient
	casClient               repb.ContentAddressableStorageClient
	byteStreamClient        bspb.ByteStreamClient
	schedulerClient         scpb.SchedulerClient
	buildBuddyServiceClient bbspb.BuildBuddyServiceClient
}

func newBuildBuddyServer(t *testing.T, env *buildBuddyServerEnv, opts *BuildBuddyServerOptions) *BuildBuddyServer {
	port := app.FreePort(t)
	opts.SchedulerServerOptions.LocalPortOverride = int32(port)

	env.SetAuthenticator(env.rbeEnv.newTestAuthenticator())
	router, err := task_router.New(env)
	require.NoError(t, err, "could not set up TaskRouter")
	env.SetTaskRouter(router)
	executionServer, err := execution_server.NewExecutionServer(env)
	require.NoError(t, err, "could not set up ExecutionServer")
	env.SetRemoteExecutionService(executionServer)
	env.SetBuildEventHandler(build_event_handler.NewBuildEventHandler(env))

	if opts.EnvModifier != nil {
		opts.EnvModifier(env.TestEnv)
	}

	scheduler, err := scheduler_server.NewSchedulerServerWithOptions(env, &opts.SchedulerServerOptions)
	require.NoError(t, err, "could not set up SchedulerServer")
	buildEventServer, err := build_event_server.NewBuildEventProtocolServer(env)
	require.NoError(t, err, "could not set up BuildEventProtocolServer")
	buildBuddyServiceServer, err := buildbuddy_server.NewBuildBuddyServer(env, nil /*=sslService*/)
	require.NoError(t, err, "could not set up BuildBuddyServiceServer")

	server := &BuildBuddyServer{
		t:                       t,
		env:                     env,
		port:                    port,
		schedulerServer:         scheduler,
		executionServer:         executionServer,
		buildBuddyServiceServer: buildBuddyServiceServer,
		buildEventServer:        buildEventServer,
	}
	server.start()

	clientConn, err := grpc_client.DialTarget(fmt.Sprintf("grpc://localhost:%d", port))
	if err != nil {
		assert.FailNowf(t, "could not connect to BuildBuddy server", err.Error())
	}
	server.executionClient = repb.NewExecutionClient(clientConn)
	env.SetRemoteExecutionClient(server.executionClient)
	server.casClient = repb.NewContentAddressableStorageClient(clientConn)
	server.byteStreamClient = bspb.NewByteStreamClient(clientConn)
	server.schedulerClient = scpb.NewSchedulerClient(clientConn)
	server.buildBuddyServiceClient = bbspb.NewBuildBuddyServiceClient(clientConn)

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
	bbspb.RegisterBuildBuddyServiceServer(grpcServer, s.buildBuddyServiceServer)
	pepb.RegisterPublishBuildEventServer(grpcServer, s.buildEventServer)

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

func (s *BuildBuddyServer) GRPCPort() int {
	return s.port
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
	// Optional API key to be sent by executor
	APIKey string
	// Optional Pool name for the executor
	Pool string
	// Optional server to be used for task leasing, cache requests, etc
	// If not specified the executor will connect to a random server.
	Server                       *BuildBuddyServer
	priorityTaskSchedulerOptions priority_task_scheduler.Options
}

// Executor is a handle for a running executor instance.
type Executor struct {
	env                *testenv.TestEnv
	hostPort           string
	grpcServer         *grpc.Server
	cancelRegistration context.CancelFunc
	taskScheduler      *priority_task_scheduler.PriorityTaskScheduler
}

// Stop unregisters the executor from the BuildBuddy server.
func (e *Executor) stop() {
	e.env.GetHealthChecker().Shutdown()
	e.env.GetHealthChecker().WaitForGracefulShutdown()
}

// ShutdownTaskScheduler stops the task scheduler from de-queueing any more work.
func (e *Executor) ShutdownTaskScheduler() {
	ctx, cancel := context.WithTimeout(context.Background(), defaultWaitTimeout)
	defer cancel()
	e.taskScheduler.Shutdown(ctx)
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
	// We're using an in-memory SQLite database so we need to make sure all servers share the same handle.
	env.SetDBHandle(r.testEnv.GetDBHandle())
	env.SetUserDB(r.testEnv.GetUserDB())
	env.SetInvocationDB(r.testEnv.GetInvocationDB())

	server := newBuildBuddyServer(r.t, env, opts)
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
	optionsCopy.priorityTaskSchedulerOptions = priority_task_scheduler.Options{
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
	name := fmt.Sprintf("unnamedExecutor%d_singleTask", atomic.AddUint64(&r.executorNameCounter, 1))
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

	env.SetRemoteExecutionClient(repb.NewExecutionClient(clientConn))
	env.SetSchedulerClient(scpb.NewSchedulerClient(clientConn))
	env.SetAuthenticator(r.newTestAuthenticator())

	bundleFS, err := bundle.Get()
	require.NoError(r.t, err)
	env.SetFileResolver(fileresolver.New(bundleFS, "enterprise"))
	err = resources.Configure(env)
	require.NoError(r.t, err)

	executorConfig := env.GetConfigurator().GetExecutorConfig()
	executorConfig.Pool = options.Pool
	// Place executor data under the env root dir, since that dir gets removed
	// only after all the executors have shutdown.
	executorConfig.RootDirectory = filepath.Join(r.rootDataDir, filepath.Join(options.Name, "builds"))
	executorConfig.LocalCacheDirectory = filepath.Join(r.rootDataDir, filepath.Join(options.Name, "filecache"))

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
	if options.Name != "" {
		executorID = options.Name
	}

	exec, err := executor.NewExecutor(env, executorID, &executor.Options{NameOverride: options.Name})
	if err != nil {
		assert.FailNowf(r.t, fmt.Sprintf("could not create executor %q", options.Name), err.Error())
	}
	taskScheduler := priority_task_scheduler.NewPriorityTaskScheduler(env, exec, &options.priorityTaskSchedulerOptions)
	taskScheduler.Start()

	executorPort := app.FreePort(r.t)
	execLis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", executorPort))
	if err != nil {
		assert.FailNowf(r.t, fmt.Sprintf("could not listen on port %d", executorPort), err.Error())
	}
	executorGRPCServer, execRunFunc := env.GRPCServer(execLis)
	env.GetHealthChecker().RegisterShutdownFunction(grpc_server.GRPCShutdownFunc(executorGRPCServer))

	scpb.RegisterQueueExecutorServer(executorGRPCServer, taskScheduler)

	go execRunFunc()

	ctx, cancel := context.WithCancel(context.Background())

	opts := &scheduler_client.Options{
		PortOverride:     int32(executorPort),
		HostnameOverride: "localhost",
		NodeNameOverride: options.Name,
		APIKeyOverride:   options.APIKey,
	}
	registration, err := scheduler_client.NewRegistration(env, taskScheduler, executorID, opts)
	if err != nil {
		assert.FailNowf(r.t, "could not create executor registration", err.Error())
	}
	registration.Start(ctx)

	executor := &Executor{
		env:                env,
		hostPort:           fmt.Sprintf("localhost:%d", executorPort),
		grpcServer:         executorGRPCServer,
		cancelRegistration: cancel,
		taskScheduler:      taskScheduler,
	}
	r.executors[executor.hostPort] = executor
	return executor
}

func (r *Env) newTestAuthenticator() *testauth.TestAuthenticator {
	users := testauth.TestUsers(r.UserID1, r.GroupID1)
	users[ExecutorAPIKey] = &testauth.TestUser{
		GroupID:       r.ExecutorGroupID,
		AllowedGroups: []string{r.ExecutorGroupID},
		// TODO(bduffany): Replace `role.Admin` below with `role.Default` since API
		// keys cannot have admin rights in practice. This is needed because some
		// tests perform some RPCs which require admin rights, and we'll need to
		// either (a) refactor those tests to authenticate as an admin user, or (b)
		// make it legitimately possible for an API key to have admin role.
		GroupMemberships: []*interfaces.GroupMembership{
			{GroupID: r.ExecutorGroupID, Role: role.Admin},
		},
		Capabilities: []akpb.ApiKey_Capability{akpb.ApiKey_REGISTER_EXECUTOR_CAPABILITY},
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

	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	ctx = r.WithUserID(ctx, ExecutorAPIKey)

	for time.Now().Before(deadline) {
		nodesByHostIp = make(map[string]bool)

		client := r.GetBuildBuddyServiceClient()
		req := &scpb.GetExecutionNodesRequest{
			RequestContext: &ctxpb.RequestContext{
				GroupId: r.ExecutorGroupID,
			},
		}
		rsp, err := client.GetExecutionNodes(ctx, req)
		require.NoError(r.t, err)
		for _, e := range rsp.GetExecutor() {
			nodesByHostIp[fmt.Sprintf("%s:%d", e.Node.Host, e.Node.Port)] = true
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

// getResult blocks until the command either finishes executing or encounters
// an execution error. It fails the test immediately if an error occurs that
// is not a remote execution error.
func (c *Command) getResult() (*CommandResult, error) {
	timeout := time.NewTimer(defaultWaitTimeout)
	for {
		select {
		case result, ok := <-c.StatusChannel():
			if !ok {
				assert.FailNow(c.env.t, fmt.Sprintf("command %q did not send a result", c.Name))
			}
			if result.Stage != repb.ExecutionStage_COMPLETED {
				continue
			}
			timeout.Stop()
			if result.Err != nil {
				return nil, result.Err
			}
			ctx := context.Background()
			ctx = c.env.WithUserID(ctx, c.userID)
			stdout, stderr, err := c.rbeClient.GetStdoutAndStderr(ctx, result)
			if err != nil {
				assert.FailNowf(c.env.t, "could not fetch outputs", err.Error())
			}
			return &CommandResult{
				CommandResult: result,
				Stdout:        stdout,
				Stderr:        stderr,
			}, nil
		case <-timeout.C:
			assert.FailNow(c.env.t, fmt.Sprintf("command %q did not finish within timeout", c.Name))
			return nil, nil
		}
	}
}

// Wait blocks until the command has finished executing.
func (c *Command) Wait() *CommandResult {
	result, err := c.getResult()
	require.NoError(c.env.t, err)
	return result
}

// MustFail asserts that the command encounters an execution error, and returns
// the resulting error. Note that if the command runs to completion and returns
// a non-zero exit code, this is considered a successful execution, not an
// execution error.
func (c *Command) MustFail() error {
	_, err := c.getResult()
	require.Error(c.env.t, err)
	return err
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

func (r *Env) WithUserID(ctx context.Context, userID string) context.Context {
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
	// RemoteHeaders is a set of remote headers to append to the outgoing gRPC
	// context when executing the command.
	RemoteHeaders map[string]string
	// If true, the command will reference a missing input root digest.
	SimulateMissingDigest bool
}

func (r *Env) Execute(command *repb.Command, opts *ExecuteOpts) *Command {
	ctx := context.Background()
	if opts.UserID != "" {
		ctx = r.WithUserID(ctx, opts.UserID)
	}
	ctx, err := prefix.AttachUserPrefixToContext(ctx, r.testEnv)
	if err != nil {
		assert.FailNowf(r.t, "could not attach user prefix", err.Error())
	}
	for header, value := range opts.RemoteHeaders {
		ctx = metadata.AppendToOutgoingContext(ctx, header, value)
	}

	var inputRootDigest *repb.Digest
	if opts.SimulateMissingDigest {
		// Generate a digest, but don't upload it.
		inputRootDigest, _ = testdigest.NewRandomDigestBuf(r.t, 1234)
	} else {
		if opts.InputRootDir != "" {
			inputRootDigest = r.uploadInputRoot(ctx, opts.InputRootDir)
		} else {
			inputRootDigest = r.setupRootDirectoryWithTestCommandBinary(ctx)
		}
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
