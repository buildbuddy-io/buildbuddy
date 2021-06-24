package environment

import (
	"io/fs"

	"github.com/go-redis/redis/v8"

	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"

	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
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
	GetConfigurator() *config.Configurator

	// Optional dependencies below here. For example: enterprise-only things,
	// or services that may not always be configured, like webhooks.
	GetDBHandle() *db.DBHandle
	GetStaticFilesystem() fs.FS
	GetAppFilesystem() fs.FS
	GetBlobstore() interfaces.Blobstore
	GetInvocationDB() interfaces.InvocationDB
	GetHealthChecker() interfaces.HealthChecker
	GetAuthenticator() interfaces.Authenticator
	SetAuthenticator(a interfaces.Authenticator)
	GetWebhooks() []interfaces.Webhook
	GetBuildEventHandler() interfaces.BuildEventHandler
	GetBuildEventProxyClients() []pepb.PublishBuildEventClient
	GetCache() interfaces.Cache
	GetUserDB() interfaces.UserDB
	GetAuthDB() interfaces.AuthDB
	GetInvocationStatService() interfaces.InvocationStatService
	GetExecutionService() interfaces.ExecutionService
	GetInvocationSearchService() interfaces.InvocationSearchService
	GetSplashPrinter() interfaces.SplashPrinter
	GetActionCacheClient() repb.ActionCacheClient
	GetByteStreamClient() bspb.ByteStreamClient
	GetSchedulerClient() scpb.SchedulerClient
	GetRemoteExecutionClient() repb.ExecutionClient
	GetContentAddressableStorageClient() repb.ContentAddressableStorageClient
	GetAPIService() interfaces.ApiService
	GetFileCache() interfaces.FileCache
	GetRemoteExecutionService() interfaces.RemoteExecutionService
	GetSchedulerService() interfaces.SchedulerService
	GetTaskRouter() interfaces.TaskRouter
	GetResourceTracker() interfaces.ResourceTracker
	GetCacheRedisClient() *redis.Client
	GetRemoteExecutionRedisClient() *redis.Client
	GetRemoteExecutionRedisPubSubClient() *redis.Client
	GetMetricsCollector() interfaces.MetricsCollector
	GetRepoDownloader() interfaces.RepoDownloader
	GetWorkflowService() interfaces.WorkflowService
	GetGitProviders() interfaces.GitProviders
}
