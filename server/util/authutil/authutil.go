package authutil

import (
	"context"
	"fmt"
	"regexp"
	"slices"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/blocklist"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
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

	// The context key under which client-identity information is stored.
	ClientIdentityHeaderName = "x-buildbuddy-client-identity"

	// WARNING: app/auth/auth_service.ts depends on these messages matching.
	UserNotFoundMsg   = "User not found"
	LoggedOutMsg      = "User logged out"
	ExpiredSessionMsg = "User session expired"
)

var (
	apiKeyRegex = regexp.MustCompile(APIKeyHeader + "=([a-zA-Z0-9]*)")
)

// AuthorizeOrgAdmin checks whether the given user has ORG_ADMIN capability
// within the given group ID. This is required for any org-level administrative
// operations such as changing org details or viewing and updating users.
func AuthorizeOrgAdmin(u interfaces.UserInfo, groupID string) error {
	for _, m := range u.GetGroupMemberships() {
		if m.GroupID != groupID {
			continue
		}
		if slices.Contains(m.Capabilities, akpb.ApiKey_ORG_ADMIN_CAPABILITY) {
			return nil
		} else {
			return status.PermissionDeniedError("missing required capabilities")
		}
	}
	return status.PermissionDeniedError("you are not a member of the requested organization")
}

// AuthorizeGroupAccess checks whether the user is a member of the given group.
// Where applicable, make sure to check the user's capabilities within the group
// as well.
func AuthorizeGroupAccess(ctx context.Context, env environment.Env, groupID string) error {
	if groupID == "" {
		return status.InvalidArgumentError("group ID is required")
	}
	user, err := env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return err
	}
	for _, gm := range user.GetGroupMemberships() {
		if gm.GroupID == groupID {
			return nil
		}
	}
	return status.PermissionDeniedError("You do not have access to the requested group")
}

func AuthorizeGroupAccessForStats(ctx context.Context, env environment.Env, groupID string) error {
	if err := AuthorizeGroupAccess(ctx, env, groupID); err != nil {
		return err
	}
	if blocklist.IsBlockedForStatsQuery(groupID) {
		return status.ResourceExhaustedError("Too many rows.")
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
// functions which return the authenticated user (such as AuthenticatedUser) is
// due to an anonymous user accessing the service. This is useful for allowing
// anonymous users to proceed, in cases where anonymous usage is explicitly
// enabled in the app config, and we support anonymous usage for the part of the
// service where this is used.
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

// Parses and returns a BuildBuddy API key from the given string.
func ParseAPIKeyFromString(input string) (string, error) {
	matches := apiKeyRegex.FindAllStringSubmatch(input, -1)
	l := len(matches)
	if l == 0 {
		// The api key header is not present
		return "", nil
	}
	lastMatch := matches[l-1]
	if len(lastMatch) != 2 {
		return "", status.UnauthenticatedError("failed to parse API key: invalid input")
	}
	if apiKey := lastMatch[1]; apiKey != "" {
		return apiKey, nil
	}
	return "", status.UnauthenticatedError("failed to parse API key: missing API Key")
}

func AuthContextWithError(ctx context.Context, err error) context.Context {
	return context.WithValue(ctx, contextUserErrorKey, err)
}

func AuthErrorFromContext(ctx context.Context) (error, bool) {
	err, ok := ctx.Value(contextUserErrorKey).(error)
	return err, ok
}

func EncryptionEnabled(ctx context.Context, authenticator interfaces.Authenticator) bool {
	u, err := authenticator.AuthenticatedUser(ctx)
	if err != nil {
		return false
	}
	return u.GetCacheEncryptionEnabled()
}

// Extracts auth headers from the provided context and returns them as a map.
// This function is intended for use along with AddAuthHeadersToContext when
// auth headers must be copied between contexts.
func GetAuthHeaders(ctx context.Context, authenticator interfaces.Authenticator) map[string][]string {
	headers := map[string][]string{}

	keys := metadata.ValueFromIncomingContext(ctx, ClientIdentityHeaderName)
	if len(keys) > 0 {
		if len(keys) > 1 {
			log.Warningf("Expected at most 1 client-identity header (found %d)", len(keys))
		}
		headers[ClientIdentityHeaderName] = keys
	}

	if jwt := authenticator.TrustedJWTFromAuthContext(ctx); jwt != "" {
		headers[ContextTokenStringKey] = []string{jwt}
	}
	return headers
}

// Adds the provided auth headers into the provided context and returns a new
// context containng them. This function is intended for use along with
// GetAuthHeaders when auth headers must be copied between contexts.
func AddAuthHeadersToContext(ctx context.Context, headers map[string][]string, authenticator interfaces.Authenticator) context.Context {
	for key, values := range headers {
		for _, value := range values {
			if key == ClientIdentityHeaderName {
				ctx = metadata.AppendToOutgoingContext(ctx, ClientIdentityHeaderName, value)
			} else if key == ContextTokenStringKey {
				ctx = authenticator.AuthContextFromTrustedJWT(ctx, value)
			} else {
				log.Warningf("Ignoring unrecognized auth header: %s", key)
			}
		}
	}
	return ctx
}
