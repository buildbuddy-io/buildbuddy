// Package groupstatus rejects API requests from groups whose status does not
// allow them.
package groupstatus

import (
	"context"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// restrictFreeTierExceededFlag enables rejecting requests from free tier
	// groups that have exceeded their usage limit.
	restrictFreeTierExceededFlag = "groupstatus.restrict_free_tier_exceeded"
)

var (
	errBlocked          = status.UnavailableError("there was an issue with your request - please contact support at https://buildbuddy.io/contact")
	errFreeTierExceeded = status.FailedPreconditionError("free tier usage limit reached - add a payment method at https://app.buildbuddy.io/settings/ to continue")

	// usageLimitedServices are unavailable to groups that have exceeded
	// their usage limit.
	usageLimitedServices = []string{
		"/google.bytestream.ByteStream/",
		"/" + repb.ContentAddressableStorage_ServiceDesc.ServiceName + "/",
		"/" + repb.ActionCache_ServiceDesc.ServiceName + "/",
		"/" + repb.Execution_ServiceDesc.ServiceName + "/",
		"/" + rapb.Fetch_ServiceDesc.ServiceName + "/",
		"/" + rapb.Push_ServiceDesc.ServiceName + "/",
	}
)

type checker struct {
	env environment.Env
}

func New(env environment.Env) interfaces.GroupStatusChecker {
	return &checker{env: env}
}

// CheckAllowed returns an error if the authenticated group's status does not
// allow it to call the given RPC method. Requests from the web UI and
// impersonating requests are always allowed.
func (c *checker) CheckAllowed(ctx context.Context, method string) error {
	cl, err := claims.ClaimsFromContext(ctx)
	if err != nil {
		return nil
	}
	if cl.IsImpersonating() || cl.GetAPIKeyInfo().ID == "" {
		return nil
	}
	if cl.GetGroupStatus() == grpb.Group_BLOCKED_GROUP_STATUS {
		return errBlocked
	}
	if cl.GetGroupStatus() == grpb.Group_FREE_TIER_GROUP_STATUS && cl.GetBillingStatus() == grpb.Group_USAGE_LIMIT_EXCEEDED_BILLING_STATUS && isUsageLimited(method) && c.restrictFreeTierExceeded(ctx) {
		return errFreeTierExceeded
	}
	return nil
}

func (c *checker) restrictFreeTierExceeded(ctx context.Context) bool {
	fp := c.env.GetExperimentFlagProvider()
	return fp != nil && fp.Boolean(ctx, restrictFreeTierExceededFlag, false)
}

func isUsageLimited(method string) bool {
	for _, prefix := range usageLimitedServices {
		if strings.HasPrefix(method, prefix) {
			return true
		}
	}
	return false
}
