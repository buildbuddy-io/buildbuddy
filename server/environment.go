package environment

import (
	"log"

	"github.com/buildbuddy-io/buildbuddy/server/blobstore"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/database"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/simplesearcher"
	"github.com/buildbuddy-io/buildbuddy/server/slack"
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

	// Optional dependencies below here. For example: enterprise-only things,
	// or services that may not always be configured, like webhooks.
	GetWebhooks() []interfaces.Webhook
	GetSearcher() interfaces.Searcher
}

type RealEnv struct {
	c  *config.Configurator
	bs interfaces.Blobstore
	db interfaces.Database

	webhooks []interfaces.Webhook
	searcher interfaces.Searcher
}

func (r *RealEnv) GetConfigurator() *config.Configurator {
	return r.c
}
func (r *RealEnv) SetConfigurator(c *config.Configurator) {
	r.c = c
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

func GetConfiguredEnvironmentOrDie(configurator *config.Configurator) Env {
	bs, err := blobstore.GetConfiguredBlobstore(configurator)
	if err != nil {
		log.Fatalf("Error configuring blobstore: %s", err)
	}
	rawDB, err := database.GetConfiguredDatabase(configurator)
	if err != nil {
		log.Fatalf("Error configuring database: %s", err)
	}

	searcher := simplesearcher.NewSimpleSearcher(rawDB)
	appURL := configurator.GetAppBuildBuddyURL()
	webhooks := make([]interfaces.Webhook, 0)
	if sc := configurator.GetIntegrationsSlackConfig(); sc != nil {
		if sc.WebhookURL != "" {
			webhooks = append(webhooks, slack.NewSlackWebhook(sc.WebhookURL, appURL))
		}
	}
	return &RealEnv{
		// REQUIRED
		c:  configurator,
		bs: bs,
		db: rawDB,

		// OPTIONAL
		webhooks: webhooks,
		searcher: searcher,
	}
}
