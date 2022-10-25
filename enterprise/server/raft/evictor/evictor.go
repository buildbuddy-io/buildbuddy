package evictor

import (
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/usagegossiper"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
)

type Evictor struct {
	store rfspb.ApiServer
}

func New(store rfspb.ApiServer, usageGossiper *usagegossiper.Gossiper) *Evictor {
	e := &Evictor{}
	usageGossiper.AddObserver(e)
	return e
}

func (e *Evictor) OnNodeUsage(usage *rfpb.NodeUsage) {
	if len(usage.PartitionUsage) > 0 {
		log.Infof("node usage:\n%s", usage)
	}
}

func (e *Evictor) Start() error {
	return nil
}

func (e *Evictor) Stop() error {
	return nil
}
