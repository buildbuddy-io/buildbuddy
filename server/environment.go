package environment

import (
	"log"

	"github.com/tryflame/buildbuddy/server/blobstore"
	"github.com/tryflame/buildbuddy/server/config"
	"github.com/tryflame/buildbuddy/server/database"
	"github.com/tryflame/buildbuddy/server/interfaces"
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
// gracefully handle missing optional dependencies.
//
// Do not put anything in the environment that would not be broadly useful
// across handlers.

type Env interface {
	// The following dependencies are required.
	GetConfigurator() *config.Configurator
	GetBlobstore() interfaces.Blobstore
	GetDatabase() interfaces.Database

	// Optional dependencies below here. For example: enterprise-only things.
}

type RealEnv struct {
	c  *config.Configurator
	bs interfaces.Blobstore
	db interfaces.Database
}

func (r *RealEnv) GetConfigurator() *config.Configurator {
	return r.c
}
func (r *RealEnv) GetBlobstore() interfaces.Blobstore {
	return r.bs
}
func (r *RealEnv) GetDatabase() interfaces.Database {
	return r.db
}

func GetConfiguredEnvironmentOrDie(configurator *config.Configurator) Env {
	bs, err := blobstore.GetConfiguredBlobstore(configurator)
	if err != nil {
		log.Fatalf("Error configuring blobstore: %s", err)
	}
	db, err := database.GetConfiguredDatabase(configurator)
	if err != nil {
		log.Fatalf("Error configuring database: %s", err)
	}

	return &RealEnv{
		c:  configurator,
		bs: bs,
		db: db,
	}
}
