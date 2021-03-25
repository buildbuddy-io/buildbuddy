package real_environment

import (
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"

	bspb "google.golang.org/genproto/googleapis/bytestream"

	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

type executionClientConfig struct {
	client           repb.ExecutionClient
	maxDuration      time.Duration
	disableStreaming bool
}

func (cc *executionClientConfig) GetExecutionClient() repb.ExecutionClient {
	return cc.client
}
func (cc *executionClientConfig) GetMaxDuration() time.Duration {
	return cc.maxDuration
}
func (cc *executionClientConfig) DisableStreaming() bool {
	return cc.disableStreaming
}

type RealEnv struct {
	configurator  *config.Configurator
	healthChecker interfaces.HealthChecker

	dbHandle                        *db.DBHandle
	blobstore                       interfaces.Blobstore
	invocationDB                    interfaces.InvocationDB
	authenticator                   interfaces.Authenticator
	webhooks                        []interfaces.Webhook
	buildEventProxyClients          []pepb.PublishBuildEventClient
	cache                           interfaces.Cache
	userDB                          interfaces.UserDB
	authDB                          interfaces.AuthDB
	buildEventHandler               interfaces.BuildEventHandler
	invocationSearchService         interfaces.InvocationSearchService
	invocationStatService           interfaces.InvocationStatService
	splashPrinter                   interfaces.SplashPrinter
	actionCacheClient               repb.ActionCacheClient
	byteStreamClient                bspb.ByteStreamClient
	schedulerClient                 scpb.SchedulerClient
	remoteExecutionClient           repb.ExecutionClient
	contentAddressableStorageClient repb.ContentAddressableStorageClient
	executionClients                map[string]*executionClientConfig
	APIService                      interfaces.ApiService
	fileCache                       interfaces.FileCache
	remoteExecutionService          interfaces.RemoteExecutionService
	schedulerService                interfaces.SchedulerService
	metricsCollector                interfaces.MetricsCollector
	executionService                interfaces.ExecutionService
	repoDownloader                  interfaces.RepoDownloader
	workflowService                 interfaces.WorkflowService
	cacheRedisClient                *redis.Client
	remoteExecutionRedisClient      *redis.Client
}

func NewRealEnv(c *config.Configurator, h interfaces.HealthChecker) *RealEnv {
	return &RealEnv{
		configurator:  c,
		healthChecker: h,

		executionClients: make(map[string]*executionClientConfig, 0),
	}
}

// Required -- no SETTERs for these.
func (r *RealEnv) GetConfigurator() *config.Configurator {
	return r.configurator
}

func (r *RealEnv) GetHealthChecker() interfaces.HealthChecker {
	return r.healthChecker
}

// Optional -- may not be set depending on environment configuration.
func (r *RealEnv) SetDBHandle(h *db.DBHandle) {
	r.dbHandle = h
}
func (r *RealEnv) GetDBHandle() *db.DBHandle {
	return r.dbHandle
}

func (r *RealEnv) GetBlobstore() interfaces.Blobstore {
	return r.blobstore
}
func (r *RealEnv) SetBlobstore(bs interfaces.Blobstore) {
	r.blobstore = bs
}

func (r *RealEnv) GetInvocationDB() interfaces.InvocationDB {
	return r.invocationDB
}
func (r *RealEnv) SetInvocationDB(idb interfaces.InvocationDB) {
	r.invocationDB = idb
}

func (r *RealEnv) GetWebhooks() []interfaces.Webhook {
	return r.webhooks
}
func (r *RealEnv) SetWebhooks(wh []interfaces.Webhook) {
	r.webhooks = wh
}

func (r *RealEnv) GetBuildEventHandler() interfaces.BuildEventHandler {
	return r.buildEventHandler
}
func (r *RealEnv) SetBuildEventHandler(b interfaces.BuildEventHandler) {
	r.buildEventHandler = b
}

func (r *RealEnv) GetInvocationSearchService() interfaces.InvocationSearchService {
	return r.invocationSearchService
}
func (r *RealEnv) SetInvocationSearchService(s interfaces.InvocationSearchService) {
	r.invocationSearchService = s
}

func (r *RealEnv) GetBuildEventProxyClients() []pepb.PublishBuildEventClient {
	return r.buildEventProxyClients
}
func (r *RealEnv) SetBuildEventProxyClients(clients []pepb.PublishBuildEventClient) {
	r.buildEventProxyClients = clients
}

func (r *RealEnv) GetCache() interfaces.Cache {
	return r.cache
}
func (r *RealEnv) SetCache(c interfaces.Cache) {
	r.cache = c
}

func (r *RealEnv) GetAuthenticator() interfaces.Authenticator {
	return r.authenticator
}
func (r *RealEnv) SetAuthenticator(a interfaces.Authenticator) {
	r.authenticator = a
}

func (r *RealEnv) GetUserDB() interfaces.UserDB {
	return r.userDB
}
func (r *RealEnv) SetUserDB(udb interfaces.UserDB) {
	r.userDB = udb
}

func (r *RealEnv) GetAuthDB() interfaces.AuthDB {
	return r.authDB
}
func (r *RealEnv) SetAuthDB(adb interfaces.AuthDB) {
	r.authDB = adb
}

func (r *RealEnv) GetInvocationStatService() interfaces.InvocationStatService {
	return r.invocationStatService
}
func (r *RealEnv) SetInvocationStatService(iss interfaces.InvocationStatService) {
	r.invocationStatService = iss
}

func (r *RealEnv) SetSplashPrinter(p interfaces.SplashPrinter) {
	r.splashPrinter = p
}
func (r *RealEnv) GetSplashPrinter() interfaces.SplashPrinter {
	return r.splashPrinter
}

func (r *RealEnv) SetActionCacheClient(a repb.ActionCacheClient) {
	r.actionCacheClient = a
}
func (r *RealEnv) GetActionCacheClient() repb.ActionCacheClient {
	return r.actionCacheClient
}

func (r *RealEnv) SetByteStreamClient(b bspb.ByteStreamClient) {
	r.byteStreamClient = b
}
func (r *RealEnv) GetByteStreamClient() bspb.ByteStreamClient {
	return r.byteStreamClient
}

func (r *RealEnv) SetSchedulerClient(s scpb.SchedulerClient) {
	r.schedulerClient = s
}
func (r *RealEnv) GetSchedulerClient() scpb.SchedulerClient {
	return r.schedulerClient
}

func (r *RealEnv) SetRemoteExecutionClient(e repb.ExecutionClient) {
	r.remoteExecutionClient = e
}
func (r *RealEnv) GetRemoteExecutionClient() repb.ExecutionClient {
	return r.remoteExecutionClient
}

func (r *RealEnv) SetContentAddressableStorageClient(c repb.ContentAddressableStorageClient) {
	r.contentAddressableStorageClient = c
}
func (r *RealEnv) GetContentAddressableStorageClient() repb.ContentAddressableStorageClient {
	return r.contentAddressableStorageClient
}

func (r *RealEnv) SetAPIService(s interfaces.ApiService) {
	r.APIService = s
}
func (r *RealEnv) GetAPIService() interfaces.ApiService {
	return r.APIService
}
func (r *RealEnv) SetFileCache(s interfaces.FileCache) {
	r.fileCache = s
}
func (r *RealEnv) GetFileCache() interfaces.FileCache {
	return r.fileCache
}
func (r *RealEnv) SetRemoteExecutionService(e interfaces.RemoteExecutionService) {
	r.remoteExecutionService = e
}
func (r *RealEnv) GetRemoteExecutionService() interfaces.RemoteExecutionService {
	return r.remoteExecutionService
}
func (r *RealEnv) SetSchedulerService(s interfaces.SchedulerService) {
	r.schedulerService = s
}
func (r *RealEnv) GetSchedulerService() interfaces.SchedulerService {
	return r.schedulerService
}
func (r *RealEnv) SetMetricsCollector(c interfaces.MetricsCollector) {
	r.metricsCollector = c
}
func (r *RealEnv) GetMetricsCollector() interfaces.MetricsCollector {
	return r.metricsCollector
}
func (r *RealEnv) SetExecutionService(e interfaces.ExecutionService) {
	r.executionService = e
}
func (r *RealEnv) GetExecutionService() interfaces.ExecutionService {
	return r.executionService
}
func (r *RealEnv) GetRepoDownloader() interfaces.RepoDownloader {
	return r.repoDownloader
}
func (r *RealEnv) SetRepoDownloader(d interfaces.RepoDownloader) {
	r.repoDownloader = d
}
func (r *RealEnv) GetWorkflowService() interfaces.WorkflowService {
	return r.workflowService
}
func (r *RealEnv) SetWorkflowService(wf interfaces.WorkflowService) {
	r.workflowService = wf
}

func (r *RealEnv) SetCacheRedisClient(redisClient *redis.Client) {
	r.cacheRedisClient = redisClient
}

func (r *RealEnv) GetCacheRedisClient() *redis.Client {
	return r.cacheRedisClient
}

func (r *RealEnv) SetRemoteExecutionRedisClient(redisClient *redis.Client) {
	r.remoteExecutionRedisClient = redisClient
}

func (r *RealEnv) GetRemoteExecutionRedisClient() *redis.Client {
	return r.remoteExecutionRedisClient
}
