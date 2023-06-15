package authutil

import (
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"

	gstatus "google.golang.org/grpc/status"
)

const (
	// missingCredentialsErrorReason is the error reason constant used to
	// identify errors that are due to missing credentials.
	missingCredentialsErrorReason = "MISSING_CREDENTIALS"

	// The key any error is stored under if the user could not be
	// authenticated.
	contextUserErrorKey = "auth.error"

	APIKeyHeader = "x-buildbuddy-api-key"

	// The key the JWT token string is stored under.
	// NB: This value must match the value in
	// bb/server/rpc/interceptors/interceptors.go which copies/reads this value
	// to/from the outgoing/incoming request contexts.
	ContextTokenStringKey = "x-buildbuddy-jwt"

	// WARNING: app/auth/auth_service.ts depends on these messages matching.
	UserNotFoundMsg   = "User not found"
	LoggedOutMsg      = "User logged out"
	ExpiredSessionMsg = "User session expired"
)

// AuthorizeGroupRole checks whether the given user has any of the allowed roles
// within the given group.
func AuthorizeGroupRole(u interfaces.UserInfo, groupID string, allowedRoles role.Role) error {
	if groupID == "" {
		return status.PermissionDeniedError("A group ID is required")
	}

	r := role.None
	for _, m := range u.GetGroupMemberships() {
		if m.GroupID == groupID {
			r = m.Role
			break
		}
	}
	if r == role.None {
		// User is not a member of the group at all; they were probably removed from
		// their org during their current UI session.
		return status.PermissionDeniedError("You do not have access to the requested organization")
	}
	if r&allowedRoles == 0 {
		return status.PermissionDeniedError("You do not have the appropriate role within this organization")
	}
	return nil
}

// AnonymousUserError returns an error indicating that the user is not
// authenticated due to credentials being missing from the request.
func AnonymousUserError(msg string) error {
	info := &errdetails.ErrorInfo{Reason: missingCredentialsErrorReason}
	status := gstatus.New(codes.Unauthenticated, msg)
	if d, err := status.WithDetails(info); err != nil {
		alert.UnexpectedEvent("failed_to_set_status_details", "Failed to set gRPC status details for AnonymousUserError")
		return status.Err()
	} else {
		return d.Err()
	}
}

// AnonymousUserError returns an error indicating that the user is not
// authenticated due to credentials being missing from the request.
func AnonymousUserErrorf(format string, args ...any) error {
	return AnonymousUserError(fmt.Sprintf(format, args...))
}

// IsAnonymousUserError can be used to check whether an error returned by
// functions which return the authenticated user (such as AuthenticatedUser or
// AuthenticateSelectedGroupID) is due to an anonymous user accessing the
// service. This is useful for allowing anonymous users to proceed, in cases
// where anonymous usage is explicitly enabled in the app config, and we support
// anonymous usage for the part of the service where this is used.
func IsAnonymousUserError(err error) bool {
	for _, detail := range gstatus.Convert(err).Proto().GetDetails() {
		info := &errdetails.ErrorInfo{}
		if err := detail.UnmarshalTo(info); err != nil {
			// not an ErrorInfo detail; ignore.
			continue
		}
		if info.GetReason() == missingCredentialsErrorReason {
			return true
		}
	}
	return false
}

func AuthContextWithError(ctx context.Context, err error) context.Context {
	return context.WithValue(ctx, contextUserErrorKey, err)
}

func AuthErrorFromContext(ctx context.Context) (error, bool) {
	err, ok := ctx.Value(contextUserErrorKey).(error)
	return err, ok
}
