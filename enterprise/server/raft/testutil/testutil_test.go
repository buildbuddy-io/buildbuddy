package testutil_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/testutil"
	"github.com/jonboulle/clockwork"
)

func TestStartShard(t *testing.T) {
	sf := testutil.NewStoreFactoryWithClock(t, clockwork.NewRealClock())
	s1 := sf.NewStore(t)
	ctx := context.Background()

	sf.StartShard(t, ctx, s1)
}
