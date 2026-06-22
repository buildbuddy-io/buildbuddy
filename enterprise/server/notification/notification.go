package notification

import (
	"context"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/email"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/lib/set"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	npb "github.com/buildbuddy-io/buildbuddy/proto/notification"
)

var _ interfaces.NotificationService = (*Service)(nil)

type emailSender interface {
	Send(ctx context.Context, msg *email.Message) error
}

type Service struct {
	env   environment.Env
	email emailSender
}

func Register(env *real_environment.RealEnv) {
	env.SetNotificationService(New(env, email.NewClient(email.ClientConfig{})))
}

func New(env environment.Env, sender emailSender) *Service {
	return &Service{
		env:   env,
		email: sender,
	}
}

func (s *Service) SendNotification(ctx context.Context, req *npb.SendNotificationRequest) (*npb.SendNotificationResponse, error) {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}

	if !u.HasCapability(cappb.Capability_SEND_NOTIFICATION) {
		return nil, status.PermissionDeniedError("API key is missing notification capability")
	}

	switch req.GetNotificationEvent() {
	case npb.NotificationEvent_UNKNOWN_NOTIFICATION_EVENT:
		return nil, status.InvalidArgumentError("notification event is required")
	default:
		return nil, status.InvalidArgumentErrorf("unsupported notification event: %s", req.GetNotificationEvent())
	}
}

type emailRecipients struct {
	groupID            string
	groupName          string
	groupURLIdentifier string
	recipients         []email.Address
}

func (s *Service) getAdminEmails(ctx context.Context, groupID string) (*emailRecipients, error) {
	type recipientRow struct {
		GroupName          string
		GroupURLIdentifier string
		RecipientEmail     string
		RecipientFirstName string
		RecipientLastName  string
	}
	recipientMembershipsQuery := `
		SELECT ug.group_group_id AS group_id,
		ug.user_user_id AS user_id
		FROM "UserGroups" AS ug
		WHERE ug.membership_status = ?
		AND (ug.role & ?) = ?
		AND ug.group_group_id = ?
	`
	args := []any{
		int32(grpb.GroupMembershipStatus_MEMBER),
		uint32(role.Admin),
		uint32(role.Admin),
		groupID,
	}
	if authutil.UserListsEnabled() {
		recipientMembershipsQuery += `
		UNION
		SELECT ulg.group_group_id AS group_id,
		uul.user_user_id AS user_id
		FROM "UserListGroups" AS ulg
		JOIN "UserUserLists" AS uul
			ON uul.user_list_user_list_id = ulg.user_list_user_list_id
		WHERE (ulg.role & ?) = ?
		AND ulg.group_group_id = ?
		`
		args = append(args, uint32(role.Admin), uint32(role.Admin), groupID)
	}

	rq := s.env.GetDBHandle().NewQuery(ctx, "notification_query_recipients").Raw(`
		SELECT
		COALESCE(g.name, '') AS group_name,
		COALESCE(g.url_identifier, '') AS group_url_identifier,
		u.email AS recipient_email,
		u.first_name AS recipient_first_name,
		u.last_name AS recipient_last_name
		FROM (`+recipientMembershipsQuery+`) AS recipient_memberships
		LEFT JOIN "Groups" AS g ON g.group_id = recipient_memberships.group_id
		JOIN "Users" AS u ON u.user_id = recipient_memberships.user_id
		WHERE TRIM(u.email) <> ''
		ORDER BY u.user_id ASC
	`, args...)
	rows, err := db.ScanAll(rq, &recipientRow{})
	if err != nil {
		return nil, err
	}

	result := &emailRecipients{
		groupID:    groupID,
		recipients: make([]email.Address, 0, len(rows)),
	}
	seen := make(set.Set[string], len(rows))
	for _, row := range rows {
		if result.groupName == "" {
			result.groupName = row.GroupName
		}
		if result.groupURLIdentifier == "" {
			result.groupURLIdentifier = row.GroupURLIdentifier
		}
		recipientEmail := strings.TrimSpace(row.RecipientEmail)
		recipientKey := strings.ToLower(recipientEmail)
		if seen.Contains(recipientKey) {
			continue
		}
		seen.Add(recipientKey)
		result.recipients = append(result.recipients, email.Address{
			Name:  strings.TrimSpace(row.RecipientFirstName + " " + row.RecipientLastName),
			Email: recipientEmail,
		})
	}
	if len(result.recipients) == 0 {
		return nil, status.FailedPreconditionError("no admin email recipients found")
	}
	return result, nil
}

func groupDisplayName(r *emailRecipients) string {
	name := strings.TrimSpace(r.groupName)
	if name == "" {
		name = r.groupURLIdentifier
	}
	if name == "" {
		name = r.groupID
	}
	return name
}
