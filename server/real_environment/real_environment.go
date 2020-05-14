package real_environment

import (
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_proxy"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

type RealEnv struct {
	configurator  *config.Configurator
	healthChecker *healthcheck.HealthChecker

	dbHandle                        *db.DBHandle
	blobstore                       interfaces.Blobstore
	invocationDB                    interfaces.InvocationDB
	authenticator                   interfaces.Authenticator
	webhooks                        []interfaces.Webhook
	buildEventProxyClients          []*build_event_proxy.BuildEventProxyClient
	cache                           interfaces.Cache
	userDB                          interfaces.UserDB
	authDB                          interfaces.AuthDB
	invocationSearchService         interfaces.InvocationSearchService
	invocationStatService           interfaces.InvocationStatService
	splashPrinter                   interfaces.SplashPrinter
	actionCacheClient               repb.ActionCacheClient
	byteStreamClient                bspb.ByteStreamClient
	contentAddressableStorageClient repb.ContentAddressableStorageClient
	remoteExecutionClient           repb.ExecutionClient
	APIService                      interfaces.ApiService
}

func NewRealEnv(c *config.Configurator, h *healthcheck.HealthChecker) *RealEnv {
	return &RealEnv{
		configurator:  c,
		healthChecker: h,
	}
}

// Required -- no SETTERs for these.
func (r *RealEnv) GetConfigurator() *config.Configurator {
	return r.configurator
}

func (r *RealEnv) GetHealthChecker() *healthcheck.HealthChecker {
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

func (r *RealEnv) GetInvocationSearchService() interfaces.InvocationSearchService {
	return r.invocationSearchService
}
func (r *RealEnv) SetInvocationSearchService(s interfaces.InvocationSearchService) {
	r.invocationSearchService = s
}

func (r *RealEnv) GetBuildEventProxyClients() []*build_event_proxy.BuildEventProxyClient {
	return r.buildEventProxyClients
}
func (r *RealEnv) SetBuildEventProxyClients(clients []*build_event_proxy.BuildEventProxyClient) {
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

func (r *RealEnv) SetContentAddressableStorageClient(c repb.ContentAddressableStorageClient) {
	r.contentAddressableStorageClient = c
}
func (r *RealEnv) GetContentAddressableStorageClient() repb.ContentAddressableStorageClient {
	return r.contentAddressableStorageClient
}

func (r *RealEnv) SetExecutionClient(e repb.ExecutionClient) {
	r.remoteExecutionClient = e
}
func (r *RealEnv) GetExecutionClient() repb.ExecutionClient {
	return r.remoteExecutionClient
}

func (r *RealEnv) SetAPIService(s interfaces.ApiService) {
	r.APIService = s
}
func (r *RealEnv) GetAPIService() interfaces.ApiService {
	return r.APIService
}
