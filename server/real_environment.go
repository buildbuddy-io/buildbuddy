package real_environment

import (
	"log"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore"
	"github.com/buildbuddy-io/buildbuddy/server/backends/cachedb"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/invocationdb"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/slack"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_proxy"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
)

type RealEnv struct {
	c            *config.Configurator
	dbHandle     *db.DBHandle
	bs           interfaces.Blobstore
	invocationDB interfaces.InvocationDB
	cacheDB      interfaces.CacheDB
	h            *healthcheck.HealthChecker
	a            interfaces.Authenticator

	webhooks                []interfaces.Webhook
	buildEventProxyClients  []*build_event_proxy.BuildEventProxyClient
	cache                   interfaces.Cache
	userDB                  interfaces.UserDB
	authDB                  interfaces.AuthDB
	invocationSearchService interfaces.InvocationSearchService
	invocationStatService   interfaces.InvocationStatService
}

func (r *RealEnv) GetConfigurator() *config.Configurator {
	return r.c
}
func (r *RealEnv) SetConfigurator(c *config.Configurator) {
	r.c = c
}
func (r *RealEnv) GetDBHandle() *db.DBHandle {
	return r.dbHandle
}

func (r *RealEnv) GetHealthChecker() *healthcheck.HealthChecker {
	return r.h
}
func (r *RealEnv) SetHealthChecker(h *healthcheck.HealthChecker) {
	r.h = h
}

func (r *RealEnv) GetBlobstore() interfaces.Blobstore {
	return r.bs
}
func (r *RealEnv) SetBlobstore(bs interfaces.Blobstore) {
	r.bs = bs
}

func (r *RealEnv) GetInvocationDB() interfaces.InvocationDB {
	return r.invocationDB
}
func (r *RealEnv) SetInvocationDB(idb interfaces.InvocationDB) {
	r.invocationDB = idb
}

func (r *RealEnv) GetCacheDB() interfaces.CacheDB {
	return r.cacheDB
}
func (r *RealEnv) SetCacheDB(cdb interfaces.CacheDB) {
	r.cacheDB = cdb
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
	return r.a
}
func (r *RealEnv) SetAuthenticator(a interfaces.Authenticator) {
	r.a = a
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

// Normally this code would live in main.go -- we put it here for now because
// the environments used by the open-core version and the enterprise version are
// not substantially different enough yet to warrant the extra complexity of
// always updating both main files.
func GetConfiguredEnvironmentOrDie(configurator *config.Configurator, checker *healthcheck.HealthChecker) *RealEnv {
	bs, err := blobstore.GetConfiguredBlobstore(configurator)
	if err != nil {
		log.Fatalf("Error configuring blobstore: %s", err)
	}
	dbHandle, err := db.GetConfiguredDatabase(configurator)
	if err != nil {
		log.Fatalf("Error configuring database: %s", err)
	}

	realEnv := &RealEnv{
		// REQUIRED
		c:        configurator,
		dbHandle: dbHandle,
		bs:       bs,
		cacheDB:  cachedb.NewCacheDB(dbHandle),
		h:        checker,
		a:        &nullauth.NullAuthenticator{},
	}
	realEnv.SetInvocationDB(invocationdb.NewInvocationDB(realEnv, dbHandle))

	webhooks := make([]interfaces.Webhook, 0)
	appURL := configurator.GetAppBuildBuddyURL()
	if sc := configurator.GetIntegrationsSlackConfig(); sc != nil {
		if sc.WebhookURL != "" {
			webhooks = append(webhooks, slack.NewSlackWebhook(sc.WebhookURL, appURL))
		}
	}
	realEnv.SetWebhooks(webhooks)

	buildEventProxyClients := make([]*build_event_proxy.BuildEventProxyClient, 0)
	for _, target := range configurator.GetBuildEventProxyHosts() {
		// NB: This can block for up to a second on connecting. This would be a
		// great place to have our health checker and mark these as optional.
		buildEventProxyClients = append(buildEventProxyClients, build_event_proxy.NewBuildEventProxyClient(target))
		log.Printf("Proxy: forwarding build events to: %s", target)
	}
	realEnv.SetBuildEventProxyClients(buildEventProxyClients)

	// If configured, enable the cache.
	var cache interfaces.Cache
	if configurator.GetCacheInMemory() {
		maxSizeBytes := configurator.GetCacheMaxSizeBytes()
		if maxSizeBytes == 0 {
			log.Fatalf("Cache size must be greater than 0 if in_memory cache is enabled!")
		}
		c, err := memory_cache.NewMemoryCache(maxSizeBytes)
		if err != nil {
			log.Fatalf("Error configuring in-memory cache: %s", err)
		}
		cache = c
	} else if configurator.GetCacheDiskConfig() != nil {
		diskConfig := configurator.GetCacheDiskConfig()
		c, err := disk_cache.NewDiskCache(diskConfig.RootDirectory, configurator.GetCacheMaxSizeBytes())
		if err != nil {
			log.Fatalf("Error configuring cache: %s", err)
		}
		cache = c
	}
	if cache != nil {
		cache.Start()
		realEnv.SetCache(cache)
		log.Printf("Cache: BuildBuddy cache API enabled!")
	}
	return realEnv
}
