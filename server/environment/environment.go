package environment

import (
	"context"
	"io/fs"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc"

	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	socipb "github.com/buildbuddy-io/buildbuddy/proto/soci"
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
	GetWebhooks() []interfaces.Webhook
	GetBuildEventHandler() interfaces.BuildEventHandler
	GetBuildEventProxyClients() []pepb.PublishBuildEventClient
	GetCache() interfaces.Cache
	GetUserDB() interfaces.UserDB
	GetAuthDB() interfaces.AuthDB
	GetInvocationStatService() interfaces.InvocationStatService
	GetExecutionService() interfaces.ExecutionService
	GetExecutionSearchService() interfaces.ExecutionSearchService
	GetInvocationSearchService() interfaces.InvocationSearchService
	GetSplashPrinter() interfaces.SplashPrinter
	GetActionCacheClient() repb.ActionCacheClient
	GetByteStreamClient() bspb.ByteStreamClient
	GetPooledByteStreamClient() interfaces.PooledByteStreamClient
	GetSchedulerClient() scpb.SchedulerClient
	GetCapabilitiesClient() repb.CapabilitiesClient
	GetRemoteExecutionClient() repb.ExecutionClient
	GetContentAddressableStorageClient() repb.ContentAddressableStorageClient
	GetAPIService() interfaces.ApiService
	GetFileCache() interfaces.FileCache
	GetRemoteExecutionService() interfaces.RemoteExecutionService
	GetSchedulerService() interfaces.SchedulerService
	GetTaskRouter() interfaces.TaskRouter
	GetTaskSizer() interfaces.TaskSizer
	GetDefaultRedisClient() redis.UniversalClient
	GetRemoteExecutionRedisClient() redis.UniversalClient
	GetRemoteExecutionRedisPubSubClient() redis.UniversalClient
	GetMetricsCollector() interfaces.MetricsCollector
	GetKeyValStore() interfaces.KeyValStore
	GetRepoDownloader() interfaces.RepoDownloader
	GetWorkflowService() interfaces.WorkflowService
	GetGitHubApp() interfaces.GitHubApp
	GetRunnerService() interfaces.RunnerService
	GetGitProviders() interfaces.GitProviders
	GetUsageService() interfaces.UsageService
	GetUsageTracker() interfaces.UsageTracker
	GetXcodeLocator() interfaces.XcodeLocator
	GetQuotaManager() interfaces.QuotaManager
	GetMux() interfaces.HttpServeMux
	GetHTTPServerWaitGroup() *sync.WaitGroup
	GetInternalHTTPMux() interfaces.HttpServeMux
	GetListenAddr() string
	GetBuildBuddyServer() interfaces.BuildBuddyServer
	GetSSLService() interfaces.SSLService
	GetBuildEventServer() pepb.PublishBuildEventServer
	GetCASServer() repb.ContentAddressableStorageServer
	GetByteStreamServer() bspb.ByteStreamServer
	GetActionCacheServer() repb.ActionCacheServer
	GetPushServer() rapb.PushServer
	GetFetchServer() rapb.FetchServer
	GetCapabilitiesServer() repb.CapabilitiesServer
	GetGRPCServer() *grpc.Server
	GetGRPCSServer() *grpc.Server
	GetInternalGRPCServer() *grpc.Server
	GetInternalGRPCSServer() *grpc.Server
	GetOLAPDBHandle() interfaces.OLAPDBHandle
	GetKMS() interfaces.KMS
	GetSecretService() interfaces.SecretService
	GetExecutionCollector() interfaces.ExecutionCollector
	GetSuggestionService() interfaces.SuggestionService
	GetCrypter() interfaces.Crypter
	GetSociArtifactStoreServer() socipb.SociArtifactStoreServer
	GetSingleFlightDeduper() interfaces.SingleFlightDeduper
	GetPromQuerier() interfaces.PromQuerier
	GetAuditLogger() interfaces.AuditLogger
	GetIPRulesService() interfaces.IPRulesService
	GetClientIdentityService() interfaces.ClientIdentityService
	GetImageCacheAuthenticator() interfaces.ImageCacheAuthenticator
	GetServerNotificationService() interfaces.ServerNotificationService
	GetGCPService() interfaces.GCPService
	GetSCIMService() interfaces.SCIMService
	GetGossipService() interfaces.GossipService
	GetCommandRunner() interfaces.CommandRunner
	GetCodesearchService() interfaces.CodesearchService
	GetSnapshotService() interfaces.SnapshotService
}
