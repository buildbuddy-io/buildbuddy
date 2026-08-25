package groupstatus_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/groupstatus"
	"github.com/stretchr/testify/require"

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
)

func TestCheckAllowed(t *testing.T) {
	ctx := context.Background()
	checker := groupstatus.New()
	for _, tc := range []struct {
		name    string
		claims  claims.Claims
		allowed bool
	}{
		{name: "blocked", claims: claims.Claims{APIKeyID: "AK1", UserID: "US1", GroupID: "GR1", GroupStatus: grpb.Group_BLOCKED_GROUP_STATUS}, allowed: false},
		{name: "free tier", claims: claims.Claims{APIKeyID: "AK1", UserID: "US1", GroupID: "GR1", GroupStatus: grpb.Group_FREE_TIER_GROUP_STATUS}, allowed: true},
		{name: "blocked without API key", claims: claims.Claims{UserID: "US1", GroupID: "GR1", GroupStatus: grpb.Group_BLOCKED_GROUP_STATUS}, allowed: true},
		{name: "blocked while impersonating", claims: claims.Claims{APIKeyID: "AK1", UserID: "US1", GroupID: "GR1", GroupStatus: grpb.Group_BLOCKED_GROUP_STATUS, Impersonating: true}, allowed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checker.CheckAllowed(testauth.WithAuthenticatedUserInfo(ctx, &tc.claims))
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
	require.NoError(t, checker.CheckAllowed(ctx))
}
