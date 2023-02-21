package authutil

import (
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	gcodes "google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

const (
	// invalidCredentialsReason is the constant ErrorInfo.Reason field applied
	// to invalid credentials errors.
	invalidCredentialsReason = "INVALID_CREDENTIALS"
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

// InvalidCredentialsError returns an error caused by invalid or missing
// credentials.
func InvalidCredentialsError(message string) error {
	info := &errdetails.ErrorInfo{Reason: invalidCredentialsReason}
	st := gstatus.Newf(gcodes.Unauthenticated, message)
	if st, err := st.WithDetails(info); err != nil {
		return status.UnauthenticatedError(message)
	} else {
		return st.Err()
	}
}

// InvalidCredentialsErrorf returns an error caused by invalid or missing
// credentials.
func InvalidCredentialsErrorf(format string, args ...any) error {
	return InvalidCredentialsError(fmt.Sprintf(format, args...))
}

// IsInvalidCredentialsError returns whether the given error was due to missing
// or invalid credentials.
func IsInvalidCredentialsError(err error) bool {
	s := gstatus.Convert(err)
	if s == nil {
		return false
	}
	for _, detailsAny := range s.Proto().GetDetails() {
		info := &errdetails.ErrorInfo{}
		if err := detailsAny.UnmarshalTo(info); err != nil {
			continue
		}
		if info.Reason == invalidCredentialsReason {
			return true
		}
	}
	return false
}
