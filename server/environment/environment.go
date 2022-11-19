package environment

import (
	"context"
	"io/fs"

	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc"

	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

// The environment struct allows for easily injecting many of buildbuddy's core
// dependencies without enumerating them.
//
// Rather than requiring a handler to have a signature like this:
//  - func NewXHandler(a interfaces.A, b interfaces.B, c interfaces.C) *Handler {}
// you can instead have a handler like this:
//   - func NewXHandler(env *environment.Env) *Handler {}
//
// Code that accepts an environment for dependency injection is required to
// gracefully handle missing *optional* dependencies.
//
// Do not put anything in the environment that would not be broadly useful
// across handlers.

type Env interface {
	// The following dependencies are required.
	GetServerContext() context.Context

	// Optional dependencies below here. For example: enterprise-only things,
	// or services that may not always be configured, like webhooks.
	GetDBHandle() interfaces.DBHandle
	// GetStaticFilesystem returns the FS that is used to serve from the /static
	// directory.
	GetStaticFilesystem() fs.FS
	// GetAppFilesystem returns the FS used to serve JS and CSS resources needed
	// by the app, including the app bundle and any lazily loaded JS chunks.
	GetAppFilesystem() fs.FS
	GetBlobstore() interfaces.Blobstore
	GetInvocationDB() interfaces.InvocationDB
	GetHealthChecker() interfaces.HealthChecker
	GetAuthenticator() interfaces.Authenticator
	SetAuthenticator(a interfaces.Authenticator)
	GetWebhooks() []interfaces.Webhook
	SetWebhooks([]interfaces.Webhook)
	GetBuildEventHandler() interfaces.BuildEventHandler
	GetBuildEventProxyClients() []pepb.PublishBuildEventClient
	SetBuildEventProxyClients([]pepb.PublishBuildEventClient)
	GetCache() interfaces.Cache
	SetCache(interfaces.Cache)
	GetUserDB() interfaces.UserDB
	GetAuthDB() interfaces.AuthDB
	GetInvocationStatService() interfaces.InvocationStatService
	GetExecutionService() interfaces.ExecutionService
	GetInvocationSearchService() interfaces.InvocationSearchService
	GetSplashPrinter() interfaces.SplashPrinter
	GetActionCacheClient() repb.ActionCacheClient
	GetByteStreamClient() bspb.ByteStreamClient
	SetByteStreamClient(bspb.ByteStreamClient)
	GetSchedulerClient() scpb.SchedulerClient
	GetCapabilitiesClient() repb.CapabilitiesClient
	GetRemoteExecutionClient() repb.ExecutionClient
	SetRemoteExecutionClient(repb.ExecutionClient)
	GetContentAddressableStorageClient() repb.ContentAddressableStorageClient
	GetAPIService() interfaces.ApiService
	SetAPIService(interfaces.ApiService)
	GetFileCache() interfaces.FileCache
	GetRemoteExecutionService() interfaces.RemoteExecutionService
	SetRemoteExecutionService(interfaces.RemoteExecutionService)
	GetSchedulerService() interfaces.SchedulerService
	SetSchedulerService(interfaces.SchedulerService)
	GetTaskRouter() interfaces.TaskRouter
	SetTaskRouter(interfaces.TaskRouter)
	GetTaskSizer() interfaces.TaskSizer
	SetTaskSizer(interfaces.TaskSizer)
	GetDefaultRedisClient() redis.UniversalClient
	SetDefaultRedisClient(redis.UniversalClient)
	GetRemoteExecutionRedisClient() redis.UniversalClient
	SetRemoteExecutionRedisClient(redis.UniversalClient)
	GetRemoteExecutionRedisPubSubClient() redis.UniversalClient
	SetRemoteExecutionRedisPubSubClient(redis.UniversalClient)
	GetMetricsCollector() interfaces.MetricsCollector
	SetMetricsCollector(interfaces.MetricsCollector)
	GetKeyValStore() interfaces.KeyValStore
	SetKeyValStore(interfaces.KeyValStore)
	GetRepoDownloader() interfaces.RepoDownloader
	GetWorkflowService() interfaces.WorkflowService
	GetRunnerService() interfaces.RunnerService
	GetGitProviders() interfaces.GitProviders
	GetUsageService() interfaces.UsageService
	SetUsageService(interfaces.UsageService)
	GetUsageTracker() interfaces.UsageTracker
	SetUsageTracker(interfaces.UsageTracker)
	GetXcodeLocator() interfaces.XcodeLocator
	SetQuotaManager(interfaces.QuotaManager)
	GetQuotaManager() interfaces.QuotaManager
	// GetFileResolver returns an FS that can be used to read server-side
	// resources that aren't intended to be directly served to end users. It first
	// consults the bundle and falls back to runfiles.
	//
	// See server/util/fileresolver/fileresolver.go
	GetFileResolver() fs.FS
	GetMux() interfaces.HttpServeMux
	SetMux(interfaces.HttpServeMux)
	GetInternalHTTPMux() interfaces.HttpServeMux
	SetInternalHTTPMux(mux interfaces.HttpServeMux)
	GetListenAddr() string
	SetListenAddr(string)
	GetBuildBuddyServer() interfaces.BuildBuddyServer
	SetBuildBuddyServer(interfaces.BuildBuddyServer)
	GetSSLService() interfaces.SSLService
	SetSSLService(interfaces.SSLService)
	GetBuildEventServer() pepb.PublishBuildEventServer
	SetBuildEventServer(pepb.PublishBuildEventServer)
	GetCASServer() repb.ContentAddressableStorageServer
	SetCASServer(repb.ContentAddressableStorageServer)
	GetByteStreamServer() bspb.ByteStreamServer
	SetByteStreamServer(bspb.ByteStreamServer)
	GetActionCacheServer() repb.ActionCacheServer
	SetActionCacheServer(repb.ActionCacheServer)
	GetPushServer() rapb.PushServer
	SetPushServer(rapb.PushServer)
	GetFetchServer() rapb.FetchServer
	SetFetchServer(rapb.FetchServer)
	GetCapabilitiesServer() repb.CapabilitiesServer
	SetCapabilitiesServer(repb.CapabilitiesServer)
	GetGRPCServer() *grpc.Server
	SetGRPCServer(*grpc.Server)
	GetGRPCSServer() *grpc.Server
	GetInternalGRPCServer() *grpc.Server
	SetInternalGRPCServer(*grpc.Server)
	GetInternalGRPCSServer() *grpc.Server
	SetInternalGRPCSServer(*grpc.Server)
	SetGRPCSServer(*grpc.Server)
	GetRegistryServer() rgpb.RegistryServer
	SetRegistryServer(r rgpb.RegistryServer)
	GetOLAPDBHandle() interfaces.OLAPDBHandle
	SetOLAPDBHandle(dbh interfaces.OLAPDBHandle)
	GetKMS() interfaces.KMS
	SetKMS(k interfaces.KMS)
	GetSecretService() interfaces.SecretService
	SetSecretService(s interfaces.SecretService)
	GetExecutionCollector() interfaces.ExecutionCollector
	SetExecutionCollector(c interfaces.ExecutionCollector)
}
