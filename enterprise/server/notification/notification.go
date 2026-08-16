package notification

import (
	"context"
	"errors"
	"fmt"
	"html"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/email"
	"github.com/buildbuddy-io/buildbuddy/server/backends/slack"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
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

type slackSender interface {
	Post(ctx context.Context, webhookURL string, payload *slack.Payload) error
}

type Service struct {
	env   environment.Env
	email emailSender
	slack slackSender
}

func Register(env *real_environment.RealEnv) {
	env.SetNotificationService(New(env, email.NewClient(email.ClientConfig{}), slack.NewClient()))
}

func New(env environment.Env, emailSender emailSender, slackSender slackSender) *Service {
	return &Service{
		env:   env,
		email: emailSender,
		slack: slackSender,
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

	if len(req.GetChannels()) == 0 {
		return nil, status.InvalidArgumentError("at least one notification channel is required")
	}

	switch event := req.GetEvent().(type) {
	case *npb.SendNotificationRequest_NondeterminismDetected:
		return s.sendNondeterminismDetected(ctx, u.GetGroupID(), event.NondeterminismDetected, req.GetChannels())
	default:
		return nil, status.InvalidArgumentError("notification event is required")
	}
}

func (s *Service) sendNondeterminismDetected(ctx context.Context, groupID string, event *npb.NondeterminismDetected, channels []*npb.NotificationChannel) (*npb.SendNotificationResponse, error) {
	if len(event.GetBuildInvocationIds()) != 2 {
		return nil, status.InvalidArgumentError("expected 2 build invocation IDs")
	}
	var workflowURL string
	if parentInvocationID := event.GetParentInvocationId(); parentInvocationID != "" {
		workflowURL = build_buddy_url.WithPath("/invocation/" + parentInvocationID).String()
	}

	errs := make([]error, 0, len(channels))
	for _, channel := range channels {
		switch c := channel.GetChannel().(type) {
		case *npb.NotificationChannel_Email:
			if err := s.sendNondeterminismEmail(ctx, groupID, event.GetBuildInvocationIds(), workflowURL); err != nil {
				errs = append(errs, err)
			}
		case *npb.NotificationChannel_Slack:
			if err := s.sendNondeterminismSlack(ctx, groupID, c.Slack.GetWebhookUrlSecretName(), event.GetBuildInvocationIds(), workflowURL); err != nil {
				errs = append(errs, err)
			}
		default:
			errs = append(errs, status.InvalidArgumentErrorf("unsupported notification channel type %T", c))
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return &npb.SendNotificationResponse{}, nil
}

func (s *Service) sendNondeterminismEmail(ctx context.Context, groupID string, buildIIDs []string, workflowURL string) error {
	recipients, err := s.getAdminEmails(ctx, groupID)
	if err != nil {
		return err
	}
	if err := s.email.Send(ctx, &email.Message{
		ToAddresses: recipients.recipients,
		Subject:     "Nondeterminism detected in your build",
		Body:        nondeterminismEmailBody(buildIIDs, workflowURL),
	}); err != nil {
		return status.WrapErrorf(err, "send nondeterminism notification email")
	}
	return nil
}

func (s *Service) sendNondeterminismSlack(ctx context.Context, groupID, secretName string, buildIIDs []string, workflowURL string) error {
	webhookURL, err := s.resolveSlackWebhookURL(ctx, groupID, secretName)
	if err != nil {
		return err
	}
	if err := s.slack.Post(ctx, webhookURL, nondeterminismSlackPayload(buildIIDs, workflowURL)); err != nil {
		return status.WrapErrorf(err, "post nondeterminism notification to Slack")
	}
	return nil
}

func (s *Service) resolveSlackWebhookURL(ctx context.Context, groupID, secretName string) (string, error) {
	if secretName == "" {
		return "", status.InvalidArgumentError("Slack webhook URL secret name is required")
	}
	secretService := s.env.GetSecretService()
	if secretService == nil {
		return "", status.FailedPreconditionError("secret service is not enabled")
	}
	secrets, err := secretService.GetSecretEnvVars(ctx, groupID, secretName)
	if err != nil {
		return "", status.WrapErrorf(err, "look up secret %q", secretName)
	}
	for _, secret := range secrets {
		if secret.GetValue() != "" {
			return secret.GetValue(), nil
		}
	}
	return "", status.NotFoundErrorf("Slack webhook secret %q is not set", secretName)
}

func nondeterminismSlackPayload(buildIIDs []string, workflowURL string) *slack.Payload {
	red := "#ad1411"
	a := slack.Attachment{
		Color: &red,
	}
	a.AddField(slack.Field{
		Title: "Nondeterminism detected",
		Value: "Running the same command twice produced different outputs.",
		Short: false,
	})
	actionURL := workflowURL
	if actionURL == "" {
		actionURL = build_buddy_url.WithPath(fmt.Sprintf("/compare/%s...%s", buildIIDs[0], buildIIDs[1])).String()
	}
	a.AddAction(slack.Action{
		Type: "button",
		Text: "See on BuildBuddy",
		Url:  actionURL,
	})
	return &slack.Payload{Attachments: []slack.Attachment{a}}
}

func nondeterminismEmailBody(buildIIDs []string, parentURL string) string {
	var b strings.Builder
	b.WriteString(`<p>BuildBuddy detected nondeterminism in your repository. Running the same command twice produced different outputs.</p>`)
	if parentURL != "" {
		fmt.Fprintf(&b, "\n"+`<p><a href="%s">See the affected actions.</a></p>`,
			html.EscapeString(parentURL))
	} else {
		compareURL := build_buddy_url.WithPath(fmt.Sprintf("/compare/%s...%s", buildIIDs[0], buildIIDs[1])).String()
		fmt.Fprintf(&b, "\n"+`<p><a href="%s">Compare the two builds.</a></p>`, html.EscapeString(compareURL))
		b.WriteString("To see the affected actions, click `Run bb explain` at the top of the Compare page.")
	}
	return b.String()
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
