package testutil_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/testutil"
)

func TestStartShard(t *testing.T) {
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	testutil.StartShard(t, ctx, s1, s2, s3)
}
