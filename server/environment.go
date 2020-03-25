package environment

import (
	"log"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/database"
	"github.com/buildbuddy-io/buildbuddy/server/backends/simplesearcher"
	"github.com/buildbuddy-io/buildbuddy/server/backends/slack"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_proxy"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
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
	GetBlobstore() interfaces.Blobstore
	GetDatabase() interfaces.Database
	GetHealthChecker() *healthcheck.HealthChecker

	// Optional dependencies below here. For example: enterprise-only things,
	// or services that may not always be configured, like webhooks.
	GetWebhooks() []interfaces.Webhook
	GetSearcher() interfaces.Searcher
	GetBuildEventProxyClients() []*build_event_proxy.BuildEventProxyClient
	GetCache() interfaces.Cache
}

type RealEnv struct {
	c  *config.Configurator
	bs interfaces.Blobstore
	db interfaces.Database
	h  *healthcheck.HealthChecker

	webhooks               []interfaces.Webhook
	searcher               interfaces.Searcher
	buildEventProxyClients []*build_event_proxy.BuildEventProxyClient
	cache                  interfaces.Cache
}

func (r *RealEnv) GetConfigurator() *config.Configurator {
	return r.c
}
func (r *RealEnv) SetConfigurator(c *config.Configurator) {
	r.c = c
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

func (r *RealEnv) GetDatabase() interfaces.Database {
	return r.db
}
func (r *RealEnv) SetDatabase(db interfaces.Database) {
	r.db = db
}

func (r *RealEnv) GetWebhooks() []interfaces.Webhook {
	return r.webhooks
}
func (r *RealEnv) SetWebhooks(wh []interfaces.Webhook) {
	r.webhooks = wh
}

func (r *RealEnv) GetSearcher() interfaces.Searcher {
	return r.searcher
}
func (r *RealEnv) SetSearcher(s interfaces.Searcher) {
	r.searcher = s
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

// Normally this code would live in main.go -- we put it here for now because
// the environments used by the open-core version and the enterprise version are
// not substantially different enough yet to warrant the extra complexity of
// always updating both main files.
func GetConfiguredEnvironmentOrDie(configurator *config.Configurator, checker *healthcheck.HealthChecker) Env {
	bs, err := blobstore.GetConfiguredBlobstore(configurator)
	if err != nil {
		log.Fatalf("Error configuring blobstore: %s", err)
	}
	rawDB, err := database.GetConfiguredDatabase(configurator)
	if err != nil {
		log.Fatalf("Error configuring database: %s", err)
	}

	realEnv := &RealEnv{
		// REQUIRED
		c:  configurator,
		bs: bs,
		db: rawDB,
		h:  checker,
	}

	realEnv.SetSearcher(simplesearcher.NewSimpleSearcher(rawDB))
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

	cacheBlobstore, err := blobstore.GetOptionalCacheBlobstore(configurator)
	if err != nil {
		log.Fatalf("Error configuring cache blobstore: %s", err)
	}
	if cacheBlobstore != nil {
		ttl := time.Duration(configurator.GetCacheTTLSeconds()) * time.Second
		cache, err := disk_cache.NewDiskCache(cacheBlobstore, rawDB, ttl, configurator.GetCacheMaxSizeBytes())
		if err != nil {
			log.Fatalf("Error configuring cache: %s", err)
		}
		cache.Start()
		realEnv.SetCache(cache)
		log.Printf("Cache: BuildBuddy cache API enabled!")
	}
	return realEnv
}
