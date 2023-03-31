package real_environment

import (
	"context"
	"io/fs"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc"

	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	bspb "google.golang.org/genproto/googleapis/bytestream"
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
	schedulerService                 interfaces.SchedulerService
	taskRouter                       interfaces.TaskRouter
	taskSizer                        interfaces.TaskSizer
	healthChecker                    interfaces.HealthChecker
	serverContext                    context.Context
	workflowService                  interfaces.WorkflowService
	runnerService                    interfaces.RunnerService
	gitProviders                     interfaces.GitProviders
	gitHubApp                        interfaces.GitHubApp
	staticFilesystem                 fs.FS
	appFilesystem                    fs.FS
	blobstore                        interfaces.Blobstore
	invocationDB                     interfaces.InvocationDB
	authenticator                    interfaces.Authenticator
	repoDownloader                   interfaces.RepoDownloader
	executionService                 interfaces.ExecutionService
	executionSearchService           interfaces.ExecutionSearchService
	cache                            interfaces.Cache
	userDB                           interfaces.UserDB
	authDB                           interfaces.AuthDB
	buildEventHandler                interfaces.BuildEventHandler
	invocationSearchService          interfaces.InvocationSearchService
	invocationStatService            interfaces.InvocationStatService
	usageService                     interfaces.UsageService
	usageTracker                     interfaces.UsageTracker
	splashPrinter                    interfaces.SplashPrinter
	actionCacheClient                repb.ActionCacheClient
	byteStreamClient                 bspb.ByteStreamClient
	schedulerClient                  scpb.SchedulerClient
	capabilitiesClient               repb.CapabilitiesClient
	remoteExecutionClient            repb.ExecutionClient
	contentAddressableStorageClient  repb.ContentAddressableStorageClient
	metricsCollector                 interfaces.MetricsCollector
	keyValStore                      interfaces.KeyValStore
	APIService                       interfaces.ApiService
	fileCache                        interfaces.FileCache
	remoteExecutionService           interfaces.RemoteExecutionService
	executionClients                 map[string]*executionClientConfig
	cacheRedisClient                 redis.UniversalClient
	defaultRedisClient               redis.UniversalClient
	remoteExecutionRedisClient       redis.UniversalClient
	dbHandle                         interfaces.DBHandle
	remoteExecutionRedisPubSubClient redis.UniversalClient
	buildEventProxyClients           []pepb.PublishBuildEventClient
	webhooks                         []interfaces.Webhook
	xcodeLocator                     interfaces.XcodeLocator
	fileResolver                     fs.FS
	internalHTTPMux                  interfaces.HttpServeMux
	mux                              interfaces.HttpServeMux
	listenAddr                       string
	buildbuddyServer                 interfaces.BuildBuddyServer
	sslService                       interfaces.SSLService
	quotaManager                     interfaces.QuotaManager
	buildEventServer                 pepb.PublishBuildEventServer
	casServer                        repb.ContentAddressableStorageServer
	byteStreamServer                 bspb.ByteStreamServer
	actionCacheServer                repb.ActionCacheServer
	pushServer                       rapb.PushServer
	fetchServer                      rapb.FetchServer
	capabilitiesServer               repb.CapabilitiesServer
	internalGRPCServer               *grpc.Server
	internalGRPCSServer              *grpc.Server
	grpcServer                       *grpc.Server
	grpcsServer                      *grpc.Server
	registryServer                   rgpb.RegistryServer
	olapDBHandle                     interfaces.OLAPDBHandle
	kms                              interfaces.KMS
	secretService                    interfaces.SecretService
	executionCollector               interfaces.ExecutionCollector
	suggestionService                interfaces.SuggestionService
	crypterService                   interfaces.Crypter
}

func NewRealEnv(h interfaces.HealthChecker) *RealEnv {
	return &RealEnv{
		healthChecker:    h,
		serverContext:    context.Background(),
		executionClients: make(map[string]*executionClientConfig, 0),
	}
}

// Required -- no SETTERs for these.
func (r *RealEnv) GetHealthChecker() interfaces.HealthChecker {
	return r.healthChecker
}

func (r *RealEnv) GetServerContext() context.Context {
	return r.serverContext
}

// Optional -- may not be set depending on environment configuration.
func (r *RealEnv) SetDBHandle(h interfaces.DBHandle) {
	r.dbHandle = h
}
func (r *RealEnv) GetDBHandle() interfaces.DBHandle {
	return r.dbHandle
}

func (r *RealEnv) SetStaticFilesystem(staticFS fs.FS) {
	r.staticFilesystem = staticFS
}
func (r *RealEnv) GetStaticFilesystem() fs.FS {
	return r.staticFilesystem
}

func (r *RealEnv) SetAppFilesystem(staticFS fs.FS) {
	r.appFilesystem = staticFS
}
func (r *RealEnv) GetAppFilesystem() fs.FS {
	return r.appFilesystem
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

func (r *RealEnv) GetUsageService() interfaces.UsageService {
	return r.usageService
}
func (r *RealEnv) SetUsageService(s interfaces.UsageService) {
	r.usageService = s
}

func (r *RealEnv) GetUsageTracker() interfaces.UsageTracker {
	return r.usageTracker
}
func (r *RealEnv) SetUsageTracker(t interfaces.UsageTracker) {
	r.usageTracker = t
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
func (r *RealEnv) GetCapabilitiesClient() repb.CapabilitiesClient {
	return r.capabilitiesClient
}
func (r *RealEnv) SetCapabilitiesClient(cc repb.CapabilitiesClient) {
	r.capabilitiesClient = cc
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
func (r *RealEnv) SetTaskRouter(tr interfaces.TaskRouter) {
	r.taskRouter = tr
}
func (r *RealEnv) GetTaskRouter() interfaces.TaskRouter {
	return r.taskRouter
}
func (r *RealEnv) GetTaskSizer() interfaces.TaskSizer {
	return r.taskSizer
}
func (r *RealEnv) SetTaskSizer(val interfaces.TaskSizer) {
	r.taskSizer = val
}
func (r *RealEnv) SetMetricsCollector(c interfaces.MetricsCollector) {
	r.metricsCollector = c
}
func (r *RealEnv) GetMetricsCollector() interfaces.MetricsCollector {
	return r.metricsCollector
}
func (r *RealEnv) SetKeyValStore(c interfaces.KeyValStore) {
	r.keyValStore = c
}
func (r *RealEnv) GetKeyValStore() interfaces.KeyValStore {
	return r.keyValStore
}
func (r *RealEnv) SetExecutionService(e interfaces.ExecutionService) {
	r.executionService = e
}
func (r *RealEnv) GetExecutionService() interfaces.ExecutionService {
	return r.executionService
}
func (r *RealEnv) SetExecutionSearchService(e interfaces.ExecutionSearchService) {
	r.executionSearchService = e
}
func (r *RealEnv) GetExecutionSearchService() interfaces.ExecutionSearchService {
	return r.executionSearchService
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
func (r *RealEnv) GetRunnerService() interfaces.RunnerService {
	return r.runnerService
}
func (r *RealEnv) SetRunnerService(wf interfaces.RunnerService) {
	r.runnerService = wf
}
func (r *RealEnv) GetGitProviders() interfaces.GitProviders {
	return r.gitProviders
}
func (r *RealEnv) SetGitProviders(gp interfaces.GitProviders) {
	r.gitProviders = gp
}
func (r *RealEnv) SetGitHubApp(val interfaces.GitHubApp) {
	r.gitHubApp = val
}
func (r *RealEnv) GetGitHubApp() interfaces.GitHubApp {
	return r.gitHubApp
}
func (r *RealEnv) GetXcodeLocator() interfaces.XcodeLocator {
	return r.xcodeLocator
}
func (r *RealEnv) SetXcodeLocator(xl interfaces.XcodeLocator) {
	r.xcodeLocator = xl
}

func (r *RealEnv) SetDefaultRedisClient(redisClient redis.UniversalClient) {
	r.defaultRedisClient = redisClient
}

func (r *RealEnv) GetDefaultRedisClient() redis.UniversalClient {
	return r.defaultRedisClient
}

func (r *RealEnv) SetRemoteExecutionRedisClient(redisClient redis.UniversalClient) {
	r.remoteExecutionRedisClient = redisClient
}

func (r *RealEnv) GetRemoteExecutionRedisClient() redis.UniversalClient {
	return r.remoteExecutionRedisClient
}

func (r *RealEnv) SetRemoteExecutionRedisPubSubClient(client redis.UniversalClient) {
	r.remoteExecutionRedisPubSubClient = client
}

func (r *RealEnv) GetRemoteExecutionRedisPubSubClient() redis.UniversalClient {
	return r.remoteExecutionRedisPubSubClient
}

func (r *RealEnv) GetFileResolver() fs.FS {
	return r.fileResolver
}

func (r *RealEnv) SetFileResolver(fr fs.FS) {
	r.fileResolver = fr
}

func (r *RealEnv) GetMux() interfaces.HttpServeMux {
	return r.mux
}

func (r *RealEnv) SetMux(mux interfaces.HttpServeMux) {
	r.mux = mux
}

func (r *RealEnv) GetInternalHTTPMux() interfaces.HttpServeMux {
	return r.internalHTTPMux
}

func (r *RealEnv) SetInternalHTTPMux(mux interfaces.HttpServeMux) {
	r.internalHTTPMux = mux
}

func (r *RealEnv) GetListenAddr() string {
	return r.listenAddr
}

func (r *RealEnv) SetListenAddr(listenAddr string) {
	r.listenAddr = listenAddr
}

func (r *RealEnv) GetBuildBuddyServer() interfaces.BuildBuddyServer {
	return r.buildbuddyServer
}

func (r *RealEnv) SetBuildBuddyServer(buildbuddyServer interfaces.BuildBuddyServer) {
	r.buildbuddyServer = buildbuddyServer
}

func (r *RealEnv) GetSSLService() interfaces.SSLService {
	return r.sslService
}

func (r *RealEnv) SetSSLService(sslService interfaces.SSLService) {
	r.sslService = sslService
}

func (r *RealEnv) GetQuotaManager() interfaces.QuotaManager {
	return r.quotaManager
}

func (r *RealEnv) SetQuotaManager(quotaManager interfaces.QuotaManager) {
	r.quotaManager = quotaManager
}

func (r *RealEnv) GetBuildEventServer() pepb.PublishBuildEventServer {
	return r.buildEventServer
}

func (r *RealEnv) SetBuildEventServer(buildEventServer pepb.PublishBuildEventServer) {
	r.buildEventServer = buildEventServer
}

func (r *RealEnv) GetCASServer() repb.ContentAddressableStorageServer {
	return r.casServer
}

func (r *RealEnv) SetCASServer(casServer repb.ContentAddressableStorageServer) {
	r.casServer = casServer
}

func (r *RealEnv) GetByteStreamServer() bspb.ByteStreamServer {
	return r.byteStreamServer
}

func (r *RealEnv) SetByteStreamServer(byteStreamServer bspb.ByteStreamServer) {
	r.byteStreamServer = byteStreamServer
}

func (r *RealEnv) GetActionCacheServer() repb.ActionCacheServer {
	return r.actionCacheServer
}

func (r *RealEnv) SetActionCacheServer(actionCacheServer repb.ActionCacheServer) {
	r.actionCacheServer = actionCacheServer
}

func (r *RealEnv) GetPushServer() rapb.PushServer {
	return r.pushServer
}

func (r *RealEnv) SetPushServer(pushServer rapb.PushServer) {
	r.pushServer = pushServer
}

func (r *RealEnv) GetFetchServer() rapb.FetchServer {
	return r.fetchServer
}

func (r *RealEnv) SetFetchServer(fetchServer rapb.FetchServer) {
	r.fetchServer = fetchServer
}

func (r *RealEnv) GetCapabilitiesServer() repb.CapabilitiesServer {
	return r.capabilitiesServer
}

func (r *RealEnv) SetCapabilitiesServer(capabilitiesServer repb.CapabilitiesServer) {
	r.capabilitiesServer = capabilitiesServer
}

func (r *RealEnv) GetInternalGRPCServer() *grpc.Server {
	return r.internalGRPCServer
}

func (r *RealEnv) SetInternalGRPCServer(server *grpc.Server) {
	r.internalGRPCServer = server
}

func (r *RealEnv) GetInternalGRPCSServer() *grpc.Server {
	return r.internalGRPCSServer
}

func (r *RealEnv) SetInternalGRPCSServer(server *grpc.Server) {
	r.internalGRPCSServer = server
}

func (r *RealEnv) GetGRPCServer() *grpc.Server {
	return r.grpcServer
}

func (r *RealEnv) SetGRPCServer(grpcServer *grpc.Server) {
	r.grpcServer = grpcServer
}

func (r *RealEnv) GetGRPCSServer() *grpc.Server {
	return r.grpcsServer
}

func (r *RealEnv) SetGRPCSServer(grpcsServer *grpc.Server) {
	r.grpcsServer = grpcsServer
}

func (r *RealEnv) GetRegistryServer() rgpb.RegistryServer {
	return r.registryServer
}

func (r *RealEnv) SetRegistryServer(server rgpb.RegistryServer) {
	r.registryServer = server
}

func (r *RealEnv) GetOLAPDBHandle() interfaces.OLAPDBHandle {
	return r.olapDBHandle
}

func (r *RealEnv) SetOLAPDBHandle(dbh interfaces.OLAPDBHandle) {
	r.olapDBHandle = dbh
}

func (r *RealEnv) GetKMS() interfaces.KMS {
	return r.kms
}

func (r *RealEnv) SetKMS(k interfaces.KMS) {
	r.kms = k
}

func (r *RealEnv) GetSecretService() interfaces.SecretService {
	return r.secretService
}
func (r *RealEnv) SetSecretService(s interfaces.SecretService) {
	r.secretService = s
}

func (r *RealEnv) GetExecutionCollector() interfaces.ExecutionCollector {
	return r.executionCollector
}

func (r *RealEnv) SetExecutionCollector(c interfaces.ExecutionCollector) {
	r.executionCollector = c
}

func (r *RealEnv) GetSuggestionService() interfaces.SuggestionService {
	return r.suggestionService
}
func (r *RealEnv) SetSuggestionService(s interfaces.SuggestionService) {
	r.suggestionService = s
}

func (r *RealEnv) GetCrypter() interfaces.Crypter {
	return r.crypterService
}
func (r *RealEnv) SetCrypter(c interfaces.Crypter) {
	r.crypterService = c
}
