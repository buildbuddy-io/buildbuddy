package groupstatus_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/groupstatus"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
	"github.com/stretchr/testify/require"

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
)

const (
	casRead = "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs"
	execute = "/build.bazel.remote.execution.v2.Execution/Execute"
	fetch   = "/build.bazel.remote.asset.v1.Fetch/FetchBlob"
	bes     = "/google.devtools.build.v1.PublishBuildEvent/PublishBuildToolEventStream"
	getUser = "/buildbuddy.service.BuildBuddyService/GetUser"
)

func TestCheckAllowed(t *testing.T) {
	env := testenv.GetTestEnv(t)
	ctx := context.Background()
	checker := groupstatus.New(env)
	blocked := claims.Claims{APIKeyID: "AK1", UserID: "US1", GroupID: "GR1", GroupStatus: grpb.Group_BLOCKED_GROUP_STATUS}
	exceeded := claims.Claims{APIKeyID: "AK1", UserID: "US1", GroupID: "GR1", GroupStatus: grpb.Group_FREE_TIER_GROUP_STATUS, BillingStatus: grpb.Group_USAGE_LIMIT_EXCEEDED_BILLING_STATUS}

	require.Error(t, checker.CheckAllowed(testauth.WithAuthenticatedUserInfo(ctx, &blocked), casRead))
	require.NoError(t, checker.CheckAllowed(testauth.WithAuthenticatedUserInfo(ctx, &exceeded), casRead))

	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"groupstatus.restrict_free_tier_exceeded": {State: memprovider.Enabled, DefaultVariant: "on", Variants: map[string]any{"on": true}},
	})))
	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)

	for _, tc := range []struct {
		name    string
		claims  claims.Claims
		method  string
		allowed bool
	}{
		{name: "blocked", claims: blocked, method: casRead, allowed: false},
		{name: "blocked BES", claims: blocked, method: bes, allowed: false},
		{name: "blocked without API key", claims: claims.Claims{UserID: "US1", GroupID: "GR1", GroupStatus: grpb.Group_BLOCKED_GROUP_STATUS}, method: casRead, allowed: true},
		{name: "blocked while impersonating", claims: claims.Claims{APIKeyID: "AK1", UserID: "US1", GroupID: "GR1", GroupStatus: grpb.Group_BLOCKED_GROUP_STATUS, Impersonating: true}, method: casRead, allowed: true},
		{name: "free tier", claims: claims.Claims{APIKeyID: "AK1", UserID: "US1", GroupID: "GR1", GroupStatus: grpb.Group_FREE_TIER_GROUP_STATUS}, method: casRead, allowed: true},
		{name: "exceeded cache", claims: exceeded, method: casRead, allowed: false},
		{name: "exceeded execution", claims: exceeded, method: execute, allowed: false},
		{name: "exceeded remote asset", claims: exceeded, method: fetch, allowed: false},
		{name: "exceeded BES", claims: exceeded, method: bes, allowed: true},
		{name: "exceeded app RPC", claims: exceeded, method: getUser, allowed: true},
		{name: "exceeded enterprise", claims: claims.Claims{APIKeyID: "AK1", UserID: "US1", GroupID: "GR1", GroupStatus: grpb.Group_ENTERPRISE_GROUP_STATUS, BillingStatus: grpb.Group_USAGE_LIMIT_EXCEEDED_BILLING_STATUS}, method: casRead, allowed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checker.CheckAllowed(testauth.WithAuthenticatedUserInfo(ctx, &tc.claims), tc.method)
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
	require.NoError(t, checker.CheckAllowed(ctx, casRead))
}
