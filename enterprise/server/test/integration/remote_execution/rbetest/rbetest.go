// Package rbetest provides a test fixture for writing Remote Build Execution
// integration tests.
package rbetest

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_execution_collector"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/execution_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/runner"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/priority_task_scheduler"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/task_router"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbeclient"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testcontext"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_server"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/capabilities_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testolapdb"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/fileresolver"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/xcode"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/prototext"

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

func (r *Env) GetRemoteExecutionTarget() string {
	server := r.buildBuddyServers[rand.Intn(len(r.buildBuddyServers))]
	return fmt.Sprintf("grpc://localhost:%d", server.port)
}

func (r *Env) GetBuildBuddyServerTarget() string {
	server := r.buildBuddyServers[rand.Intn(len(r.buildBuddyServers))]
	return fmt.Sprintf("grpc://localhost:%d", server.port)
}

func (r *Env) GetByteStreamClient() bspb.ByteStreamClient {
	return r.buildBuddyServers[rand.Intn(len(r.buildBuddyServers))].byteStreamClient
}

func (r *Env) GetContentAddressableStorageClient() repb.ContentAddressableStorageClient {
	return r.buildBuddyServers[rand.Intn(len(r.buildBuddyServers))].casClient
}

func (r *Env) GetActionResultStorageClient() repb.ActionCacheClient {
	return r.buildBuddyServers[rand.Intn(len(r.buildBuddyServers))].acClient
}

func (r *Env) GetOLAPDBHandle() *testolapdb.Handle {
	return r.buildBuddyServers[rand.Intn(len(r.buildBuddyServers))].olapDBHandle
}

func (r *Env) ShutdownBuildBuddyServers() {
	log.Info("Waiting for buildbuddy servers to shutdown")
	var wg sync.WaitGroup
	for _, app := range r.buildBuddyServers {
		app := app
		app.env.GetHealthChecker().Shutdown()
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Infof("Waiting for buildbuddy server with port %d to shut down.", app.port)
			app.env.GetHealthChecker().WaitForGracefulShutdown()
			log.Infof("Waiting for buildbuddy server with port %d to shut down.", app.port)
		}()
	}
	wg.Wait()
	log.Info("Buildbuddy servers are shut down")
}

type envLike struct {
	*testenv.TestEnv
	r *Env
}

func (el *envLike) GetByteStreamClient() bspb.ByteStreamClient {
	return el.r.buildBuddyServers[rand.Intn(len(el.r.buildBuddyServers))].byteStreamClient
}

func (el *envLike) GetContentAddressableStorageClient() repb.ContentAddressableStorageClient {
	return el.r.buildBuddyServers[rand.Intn(len(el.r.buildBuddyServers))].casClient
}

func (el *envLike) GetCapabilitiesClient() repb.CapabilitiesClient {
	return el.r.buildBuddyServers[rand.Intn(len(el.r.buildBuddyServers))].capabilitiesClient
}

func (r *Env) uploadInputRoot(ctx context.Context, rootDir string) *repb.Digest {
	digest, _, err := cachetools.UploadDirectoryToCAS(ctx, &envLike{r.testEnv, r}, "" /*=instanceName*/, rootDir)
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

	redisTarget := testredis.Start(t).Target
	envOpts := &enterprise_testenv.Options{RedisTarget: redisTarget}

	testEnv := enterprise_testenv.GetCustomTestEnv(t, envOpts)
	flags.Set(t, "app.enable_write_to_olap_db", true)
	flags.Set(t, "app.enable_write_executions_to_olap_db", true)
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
	err = testEnv.GetDBHandle().DB(ctx).Exec(
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
				log.Infof("Shut down for executor %q completed.", id)
				wg.Done()
			}()
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
	capabilitiesServer      repb.CapabilitiesServer
	buildBuddyServiceServer *buildbuddy_server.BuildBuddyServer
	buildEventServer        *build_event_server.BuildEventProtocolServer

	// Clients used by test framework.
	executionClient         repb.ExecutionClient
	casClient               repb.ContentAddressableStorageClient
	byteStreamClient        bspb.ByteStreamClient
	schedulerClient         scpb.SchedulerClient
	buildBuddyServiceClient bbspb.BuildBuddyServiceClient
	acClient                repb.ActionCacheClient
	capabilitiesClient      repb.CapabilitiesClient
	olapDBHandle            *testolapdb.Handle
}

func newBuildBuddyServer(t *testing.T, env *buildBuddyServerEnv, opts *BuildBuddyServerOptions) *BuildBuddyServer {
	port := testport.FindFree(t)
	opts.SchedulerServerOptions.LocalPortOverride = int32(port)

	env.SetAuthenticator(env.rbeEnv.newTestAuthenticator())
	router, err := task_router.New(env)
	require.NoError(t, err, "could not set up TaskRouter")
	env.SetTaskRouter(router)
	err = tasksize.Register(env)
	require.NoError(t, err, "could not set up TaskSizer")
	executionServer, err := execution_server.NewExecutionServer(env)
	require.NoError(t, err, "could not set up ExecutionServer")
	env.SetRemoteExecutionService(executionServer)

	olapDBHandle := testolapdb.NewHandle()
	env.SetOLAPDBHandle(olapDBHandle)
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
	capabilitiesServer := capabilities_server.NewCapabilitiesServer(
		env,
		/*cache=*/ true,
		/*remoteExec=*/ true,
		/*zstd=*/ true,
	)

	err = redis_execution_collector.Register(env)
	require.NoError(t, err, "could not set up ExecutionCollector")

	server := &BuildBuddyServer{
		t:                       t,
		env:                     env,
		port:                    port,
		schedulerServer:         scheduler,
		executionServer:         executionServer,
		buildBuddyServiceServer: buildBuddyServiceServer,
		buildEventServer:        buildEventServer,
		capabilitiesServer:      capabilitiesServer,
		olapDBHandle:            olapDBHandle,
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
	server.acClient = repb.NewActionCacheClient(clientConn)
	server.capabilitiesClient = repb.NewCapabilitiesClient(clientConn)

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
	repb.RegisterCapabilitiesServer(grpcServer, s.capabilitiesServer)
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

func (s *BuildBuddyServer) GRPCAddress() string {
	return fmt.Sprintf("grpc://localhost:%d", s.GRPCPort())
}

const (
	testCommandStateUnknown = iota
	testCommandStateStarted
	testCommandStateDisconnected
)

type testCommandState struct {
	mu           sync.Mutex
	opChan       chan *retpb.ControllerToCommandRequest
	state        int
	stateChanged chan struct{}
}

func (s *testCommandState) SetState(state int) {
	s.mu.Lock()
	s.state = state
	if s.stateChanged != nil {
		s.stateChanged <- struct{}{}
	}
	s.mu.Unlock()
}

func (s *testCommandState) waitState(ctx context.Context, predicate func(state int) bool) error {
	s.mu.Lock()
	reached := predicate(s.state)
	if reached {
		s.mu.Unlock()
		return nil
	}

	s.stateChanged = make(chan struct{})
	s.mu.Unlock()

	for {
		select {
		case <-s.stateChanged:
			s.mu.Lock()
			reached := predicate(s.state)
			if reached {
				s.mu.Unlock()
				return nil
			}
			s.mu.Unlock()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *testCommandState) WaitStarted(ctx context.Context) error {
	return s.waitState(ctx, func(state int) bool {
		return state > testCommandStateUnknown
	})
}

func (s *testCommandState) WaitDisconnected(ctx context.Context) error {
	return s.waitState(ctx, func(state int) bool {
		return state == testCommandStateDisconnected
	})
}

type testCommandController struct {
	t    *testing.T
	port int
	mu   sync.Mutex
	// Commands that were ever connected, even if they are not connected now.
	startedCommands map[string]bool
	// Channels to instruct waiters that a command has started.
	commandStartWaiters map[string]chan struct{}
	// Current command state.
	commandState map[string]*testCommandState
}

func (c *testCommandController) RegisterCommand(stream retpb.CommandController_RegisterCommandServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	opChannel := make(chan *retpb.ControllerToCommandRequest)
	cmdName := req.GetCommandName()
	log.Infof("Test command registering: %s", cmdName)
	c.mu.Lock()
	c.startedCommands[req.GetCommandName()] = true

	cmdState := &testCommandState{opChan: opChannel, state: testCommandStateStarted}
	c.commandState[cmdName] = cmdState
	if ch, ok := c.commandStartWaiters[req.GetCommandName()]; ok {
		close(ch)
		delete(c.commandStartWaiters, req.GetCommandName())
	}
	c.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, err := stream.Recv()
			if err != nil {
				log.Warningf("Test command %q stream closed: %s", cmdName, err)
				cmdState.SetState(testCommandStateDisconnected)
				return
			}
		}
	}()

	for {
		select {
		case op := <-opChannel:
			reqText, err := prototext.Marshal(req)
			if err != nil {
				log.Warningf("Marshal failed: %v", err)
				break
			}
			log.Infof("Sending request to command [%s]:\n%s", req.GetCommandName(), string(reqText))
			err = stream.Send(op)
			if err != nil {
				log.Warningf("Send failed: %v", err)
				break
			}
		case <-done:
			return nil
		}
	}
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
	state, ok := c.commandState[name]
	if !ok {
		assert.FailNow(c.t, fmt.Sprintf("command %q is not connected", name))
	}
	state.opChan <- &retpb.ControllerToCommandRequest{
		ExitOp: &retpb.ExitOp{ExitCode: exitCode},
	}
}

func (c *testCommandController) waitDisconnected(ctx context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, ok := c.commandState[name]
	if !ok {
		assert.FailNow(c.t, fmt.Sprintf("command %q was never connected", name))
	}
	return state.WaitDisconnected(ctx)
}

func newTestCommandController(t *testing.T) *testCommandController {
	port := testport.FindFree(t)
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		assert.FailNowf(t, "could not listen on port", err.Error())
	}

	controller := &testCommandController{
		t:                   t,
		port:                port,
		startedCommands:     make(map[string]bool),
		commandStartWaiters: make(map[string]chan struct{}),
		commandState:        make(map[string]*testCommandState),
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
	// Optional interceptor for command execution results.
	RunInterceptor
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
func (r *Env) AddExecutorWithOptions(t testing.TB, opts *ExecutorOptions) *Executor {
	executor := r.addExecutor(t, opts)
	r.waitForExecutorRegistration()
	return executor
}

// AddExecutor brings up an executor with an auto generated name with default settings.
// Use this function in tests where it's not important on which executor tasks are executed,
// otherwise use AddExecutorWithOptions and specify a custom Name.
// Blocks until executor registers with the scheduler.
func (r *Env) AddExecutor(t testing.TB) *Executor {
	name := fmt.Sprintf("unnamedExecutor%d", atomic.AddUint64(&r.executorNameCounter, 1))
	return r.AddExecutorWithOptions(t, &ExecutorOptions{Name: name})
}

// AddSingleTaskExecutorWithOptions brings up an executor with custom options that is configured with capacity to
// accept only a single "default" sized task.
// Blocks until executor registers with the scheduler.
func (r *Env) AddSingleTaskExecutorWithOptions(t testing.TB, options *ExecutorOptions) *Executor {
	optionsCopy := *options
	optionsCopy.priorityTaskSchedulerOptions = priority_task_scheduler.Options{
		RAMBytesCapacityOverride:  tasksize.DefaultMemEstimate,
		CPUMillisCapacityOverride: tasksize.DefaultCPUEstimate,
	}
	executor := r.addExecutor(t, &optionsCopy)
	r.waitForExecutorRegistration()
	return executor
}

// AddSingleTaskExecutor brings up an executor with an auto generated name that is configured
// with capacity to accept only a single "default" sized task.
// Use this function in tests where it's not important on which executor tasks are executed,
// otherwise use AddSingleTaskExecutorWithOptions and specify a custom Name.
// Blocks until executor registers with the scheduler.
func (r *Env) AddSingleTaskExecutor(t testing.TB) *Executor {
	name := fmt.Sprintf("unnamedExecutor%d_singleTask", atomic.AddUint64(&r.executorNameCounter, 1))
	return r.AddSingleTaskExecutorWithOptions(t, &ExecutorOptions{Name: name})
}

// AddNamedExecutors brings up N named executors with default settings.
// Use this function if it matters for the test on which executor tasks are executed, otherwise
// use AddExecutors.
// Blocks until all executors register with the scheduler.
func (r *Env) AddNamedExecutors(t testing.TB, names []string) []*Executor {
	var executors []*Executor
	for _, name := range names {
		executors = append(executors, r.addExecutor(t, &ExecutorOptions{Name: name}))
	}
	r.waitForExecutorRegistration()
	return executors
}

// AddExecutors brings up N executors with auto generated names with default settings.
// Use this function in tests where it's not important on which executor tasks are exected,
// otherwise use AddNamedExecutors.
// Blocks until all executors register with the scheduler.
func (r *Env) AddExecutors(t testing.TB, n int) []*Executor {
	var names []string
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("unnamedExecutor%d", atomic.AddUint64(&r.executorNameCounter, 1))
		names = append(names, name)
	}
	return r.AddNamedExecutors(t, names)
}

func (r *Env) addExecutor(t testing.TB, options *ExecutorOptions) *Executor {
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
	xl := xcode.NewXcodeLocator()
	env.SetXcodeLocator(xl)

	bundleFS, err := bundle.Get()
	require.NoError(r.t, err)
	env.SetFileResolver(fileresolver.New(bundleFS, "enterprise"))
	err = resources.Configure()
	require.NoError(r.t, err)

	flags.Set(t, "executor.pool", options.Pool)
	// Place executor data under the env root dir, since that dir gets removed
	// only after all the executors have shutdown.
	flags.Set(t, "executor.root_directory", filepath.Join(r.rootDataDir, filepath.Join(options.Name, "builds")))
	localCacheDirectory := filepath.Join(r.rootDataDir, filepath.Join(options.Name, "filecache"))

	fc, err := filecache.NewFileCache(localCacheDirectory, 1_000_000_000 /* Default cache size value */)
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
	env.SetCapabilitiesClient(repb.NewCapabilitiesClient(clientConn))

	executorUUID, err := guuid.NewRandom()
	require.NoError(r.t, err)
	executorID := executorUUID.String()
	if options.Name != "" {
		executorID = options.Name
	}

	runnerPool := NewTestRunnerPool(r.t, env, options.RunInterceptor)

	exec, err := executor.NewExecutor(env, executorID, runnerPool, &executor.Options{NameOverride: options.Name})
	if err != nil {
		assert.FailNowf(r.t, fmt.Sprintf("could not create executor %q", options.Name), err.Error())
	}
	taskScheduler := priority_task_scheduler.NewPriorityTaskScheduler(env, exec, runnerPool, &options.priorityTaskSchedulerOptions)
	taskScheduler.Start()

	executorPort := testport.FindFree(r.t)
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
		APIKeyOverride:   options.APIKey,
	}
	registration, err := scheduler_client.NewRegistration(env, taskScheduler, executorID, options.Name, opts)
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
	env.SetCapabilitiesClient(r.testEnv.GetCapabilitiesClient())
	// TODO: Does the context need the user ID if the CommandResult was produced
	// by an authenticated user?
	if err := r.rbeClient.DownloadActionOutputs(context.Background(), env, res.CommandResult, tmpDir); err != nil {
		assert.FailNow(r.t, "failed to download action outputs", err.Error())
	}
	return tmpDir
}

func (r *Env) GetActionResultForFailedAction(ctx context.Context, cmd *Command, invocationID string) (*repb.ActionResult, error) {
	d := cmd.GetActionResourceName().GetDigest()
	actionResultDigest, err := digest.AddInvocationIDToDigest(d, invocationID)
	if err != nil {
		assert.FailNow(r.t, fmt.Sprintf("unable to attach invocation ID %q to digest", invocationID))
	}
	req := &repb.GetActionResultRequest{
		InstanceName: cmd.GetActionResourceName().GetInstanceName(),
		ActionDigest: actionResultDigest,
	}
	acClient := r.GetActionResultStorageClient()
	return acClient.GetActionResult(context.Background(), req)
}

func (r *Env) GetStdoutAndStderr(ctx context.Context, actionResult *repb.ActionResult, instanceName string) (string, string, error) {
	stdout := ""
	if actionResult.GetStdoutDigest() != nil {
		d := digest.NewResourceName(actionResult.GetStdoutDigest(), instanceName)
		buf := bytes.NewBuffer(make([]byte, 0, d.GetDigest().GetSizeBytes()))
		err := cachetools.GetBlob(ctx, r.GetByteStreamClient(), d, buf)
		if err != nil {
			return "", "", status.UnavailableErrorf("error retrieving stdout from CAS: %v", err)
		}
		stdout = buf.String()
	}

	stderr := ""
	if actionResult.GetStderrDigest() != nil {
		d := digest.NewResourceName(actionResult.GetStderrDigest(), instanceName)
		buf := bytes.NewBuffer(make([]byte, 0, d.GetDigest().GetSizeBytes()))
		err := cachetools.GetBlob(ctx, r.GetByteStreamClient(), d, buf)
		if err != nil {
			return "", "", status.InternalErrorf("error retrieving stderr from CAS: %v", err)
		}
		stderr = buf.String()
	}

	return stdout, stderr, nil
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
func (c *Command) getResult() *CommandResult {
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
			var stdout, stderr string
			if result.ActionResult != nil {
				ctx := context.Background()
				ctx = c.env.WithUserID(ctx, c.userID)
				var err error
				stdout, stderr, err = c.env.GetStdoutAndStderr(ctx, result.ActionResult, result.InstanceName)
				if err != nil {
					assert.FailNowf(c.env.t, "could not fetch outputs", err.Error())
				}
			}
			return &CommandResult{
				CommandResult: result,
				Stdout:        stdout,
				Stderr:        stderr,
			}
		case <-timeout.C:
			assert.FailNow(c.env.t, fmt.Sprintf("command %q did not finish within timeout", c.Name))
			return nil
		}
	}
}

// Wait blocks until the command has terminated with a normal exit. If the
// command fails to be started or exits abnormally (timeout, killed, etc.)
// then this will fail the test.
func (c *Command) Wait() *CommandResult {
	result := c.getResult()
	require.NoError(c.env.t, result.Err)
	return result
}

// MustFailToStart asserts that the command did not begin execution due to an
// error such as a bad request (no executors registered, missing inputs, invalid
// platform props, etc). This returns the raw error that was encountered.
func (c *Command) MustFailToStart() error {
	result := c.getResult()
	require.Nil(c.env.t, result.ActionResult, "command unexpectedly return an action result")
	require.Error(c.env.t, result.Err, "command should have failed to start")
	return result.Err
}

// MustTerminateAbnormally asserts that the command began execution but did not
// return an exit code, for example due to being killed mid-execution. In this
// case, we expect both an error as well as any partial debug output from the
// command. The Err field in the returned CommandResult will contain the error
// that caused the command to terminate abnormally, and stdout/stderr can be
// inspected to see any partial debug output from the command before it was
// terminated.
func (c *Command) MustTerminateAbnormally() *CommandResult {
	result := c.getResult()
	require.NotNil(
		c.env.t, result.ActionResult,
		"expected action result with debug outputs to help diagnose abnormal termination")
	require.Error(c.env.t, result.Err, "expected abnormal termination")
	return result
}

// MustBeCancelled asserts that the command was cancelled by the execution
// service. An ExecuteResponse is not made available to clients in this case.
func (c *Command) MustBeCancelled() {
	result := c.getResult()
	require.True(
		c.env.t, status.IsCanceledError(result.Err),
		"expected Canceled error but got: %s", result.Err)
}

// MustFailAfterSchedulerRetry asserts that the command failed after exhausting
// all scheduler-level retry attempts. An ExecuteResponse is not made available
// to the client in this case. This is only useful for tasks which will not be
// retried by Bazel (currently just CI runner tasks).
func (c *Command) MustFailAfterSchedulerRetry() error {
	result := c.getResult()
	require.Error(c.env.t, result.Err)
	require.Contains(c.env.t, result.Err.Error(), "already attempted")
	return result.Err
}

func (c *Command) WaitAccepted() string {
	select {
	case opName := <-c.AcceptedChannel():
		// Command accepted.
		return opName
	case <-time.After(defaultWaitTimeout):
		assert.FailNow(c.env.t, fmt.Sprintf("command %q was not accepted within timeout", c.Name))
		return ""
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

func (c *ControlledCommand) WaitDisconnected() {
	ctx, cancel := context.WithTimeout(context.Background(), defaultWaitTimeout)
	defer cancel()
	err := c.controller.waitDisconnected(ctx, c.Name)
	require.NoError(c.t, err)
}

type ExecuteControlledOpts struct {
	InvocationID string
}

// ExecuteControlledCommand anonymously executes a special test command binary
// that connects to a controller server and blocks until it receives further
// instructions from the controller.
// The returned handle is used to send instructions to the test command.
func (r *Env) ExecuteControlledCommand(name string, opts *ExecuteControlledOpts) *ControlledCommand {
	ctx := context.Background()
	args := []string{"./" + testCommandBinaryName, "--name", name, "--controller", fmt.Sprintf("localhost:%d", r.testCommandController.port)}

	ctx, err := prefix.AttachUserPrefixToContext(ctx, r.testEnv)
	if err != nil {
		assert.FailNowf(r.t, "could not attach user prefix", err.Error())
	}
	if opts.InvocationID != "" {
		ctx = testcontext.AttachInvocationIDToContext(r.t, ctx, opts.InvocationID)
	}

	inputRootDigest := r.setupRootDirectoryWithTestCommandBinary(ctx)

	cmd, err := r.rbeClient.PrepareCommand(ctx, defaultInstanceName, name, inputRootDigest, minimalCommand(args...), 0 /*=timeout*/)
	if err != nil {
		assert.FailNow(r.t, fmt.Sprintf("Could not prepare command %q", name), err.Error())
	}

	err = cmd.Start(ctx, &rbeclient.StartOpts{SkipCacheLookup: true})
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
	// The invocation ID in the incoming gRPC context.
	InvocationID string
	// ActionTimeout is the value of Timeout to be passed to the spawned action.
	ActionTimeout time.Duration
	// Whether action cache should be checked for existing results. By default,
	// we skip the action cache check for tests.
	CheckCache bool
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

	if opts.InvocationID != "" {
		ctx = testcontext.AttachInvocationIDToContext(r.t, ctx, opts.InvocationID)
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
	cmd, err := r.rbeClient.PrepareCommand(ctx, defaultInstanceName, name, inputRootDigest, command, opts.ActionTimeout)
	if err != nil {
		assert.FailNowf(r.t, fmt.Sprintf("unable to request action execution for command %q", name), err.Error())
	}

	err = cmd.Start(ctx, &rbeclient.StartOpts{SkipCacheLookup: !opts.CheckCache})
	if err != nil {
		assert.FailNow(r.t, fmt.Sprintf("Could not execute command %q", name), err.Error())
	}
	return &Command{r, cmd, r.rbeClient, opts.UserID}
}

// RunFunc is the function signature for runner.Runner.Run().
type RunFunc func(ctx context.Context) *interfaces.CommandResult

// RunInterceptor returns a command result for testing purposes, optionally
// delegating to the real runner implementation to execute the command and get a
// real result.
type RunInterceptor func(ctx context.Context, original RunFunc) *interfaces.CommandResult

// AlwaysReturn returns a RunInterceptor that returns a fixed result on every
// task attempt.
func AlwaysReturn(result *interfaces.CommandResult) RunInterceptor {
	return func(ctx context.Context, original RunFunc) *interfaces.CommandResult {
		return result
	}
}

// ReturnForFirstAttempt returns a RunInterceptor that returns the given result
// only on the very first task attempt. Subsequent runs even across different
// runners will return the real command result.
func ReturnForFirstAttempt(result *interfaces.CommandResult) RunInterceptor {
	attempt := int32(0)
	return func(ctx context.Context, original RunFunc) *interfaces.CommandResult {
		if n := atomic.AddInt32(&attempt, 1); n == 1 {
			return result
		}
		return original(ctx)
	}
}

// testRunnerPool returns runners whose Run() results can be controlled by the
// test.
type testRunnerPool struct {
	interfaces.RunnerPool
	runInterceptor RunInterceptor
}

func NewTestRunnerPool(t testing.TB, env environment.Env, runInterceptor RunInterceptor) interfaces.RunnerPool {
	realPool, err := runner.NewPool(env, &runner.PoolOptions{})
	require.NoError(t, err)
	return &testRunnerPool{realPool, runInterceptor}
}

func (p *testRunnerPool) Get(ctx context.Context, task *repb.ScheduledTask) (interfaces.Runner, error) {
	realRunner, err := p.RunnerPool.Get(ctx, task)
	if err != nil {
		return nil, err
	}
	return &testRunner{realRunner, p.runInterceptor}, nil
}

func (p *testRunnerPool) TryRecycle(ctx context.Context, r interfaces.Runner, finishedCleanly bool) {
	tr := r.(*testRunner)
	p.RunnerPool.TryRecycle(ctx, tr.Runner, finishedCleanly)
}

// testRunner is a Runner implementation that allows injecting error results
// for command execution.
type testRunner struct {
	interfaces.Runner
	interceptor RunInterceptor
}

func (r *testRunner) Run(ctx context.Context) *interfaces.CommandResult {
	if r.interceptor == nil {
		return r.Runner.Run(ctx)
	}
	return r.interceptor(ctx, r.Runner.Run)
}

// WaitForAnyPooledRunner waits for the runner pool count across all executors
// to be at least 1. This can be called after a command is complete,
// to ensure that the runner has been made available for recycling.
func WaitForAnyPooledRunner(t *testing.T, ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	r := retry.DefaultWithContext(ctx)
	for r.Next() {
		if testmetrics.GaugeValue(t, metrics.RunnerPoolCount) > 0 {
			break
		}
	}
	require.NoError(t, ctx.Err(), "timed out waiting for runner to be pooled")
}

func minimalCommand(args ...string) *repb.Command {
	return &repb.Command{
		Arguments: args,
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "container-image", Value: "none"},
				{Name: "OSFamily", Value: runtime.GOOS},
				{Name: "Arch", Value: runtime.GOARCH},
			},
		},
	}
}
