// Package groupstatus rejects API requests from groups whose status does not
// allow them.
package groupstatus

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
)

var (
	errBlocked = status.UnavailableError("there was an issue with your request - please contact support at https://buildbuddy.io/contact")
)

type checker struct{}

func New() interfaces.GroupStatusChecker {
	return &checker{}
}

// CheckAllowed returns an error if the authenticated group's status does not
// allow it to make API requests. Requests from the web UI and impersonating
// requests are always allowed.
func (c *checker) CheckAllowed(ctx context.Context) error {
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
	return nil
}
