package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

var (
	webhookURL         = flag.String("integrations.slack.webhook_url", "", "A Slack webhook url to post build update messages to.")
	feedbackWebhookURL = flag.String("integrations.slack.feedback_webhook_url", "", "A Slack webhook url to post feedback messages to.")
)

type Field struct {
	Title string `json:"title"`
	Value string `json:"value"`
	Short bool   `json:"short"`
}

type Action struct {
	Type  string `json:"type"`
	Text  string `json:"text"`
	Url   string `json:"url"`
	Style string `json:"style"`
}

type Attachment struct {
	Fallback     *string   `json:"fallback"`
	Color        *string   `json:"color"`
	PreText      *string   `json:"pretext"`
	AuthorName   *string   `json:"author_name"`
	AuthorLink   *string   `json:"author_link"`
	AuthorIcon   *string   `json:"author_icon"`
	Title        *string   `json:"title"`
	TitleLink    *string   `json:"title_link"`
	Text         *string   `json:"text"`
	ImageUrl     *string   `json:"image_url"`
	CallbackID   *string   `json:"callback_id"`
	Footer       *string   `json:"footer"`
	FooterIcon   *string   `json:"footer_icon"`
	Timestamp    *int64    `json:"ts"`
	MarkdownIn   *[]string `json:"mrkdwn_in"`
	ThumbnailUrl *string   `json:"thumb_url"`
	Actions      []*Action `json:"actions"`
	Fields       []*Field  `json:"fields"`
}

type Payload struct {
	Parse       string       `json:"parse,omitempty"`
	Username    string       `json:"username,omitempty"`
	IconUrl     string       `json:"icon_url,omitempty"`
	IconEmoji   string       `json:"icon_emoji,omitempty"`
	Channel     string       `json:"channel,omitempty"`
	Text        string       `json:"text,omitempty"`
	LinkNames   string       `json:"link_names,omitempty"`
	Attachments []Attachment `json:"attachments,omitempty"`
	UnfurlLinks bool         `json:"unfurl_links,omitempty"`
	UnfurlMedia bool         `json:"unfurl_media,omitempty"`
	Markdown    bool         `json:"mrkdwn,omitempty"`
}

func (attachment *Attachment) AddField(field Field) *Attachment {
	attachment.Fields = append(attachment.Fields, &field)
	return attachment
}

func (attachment *Attachment) AddAction(action Action) *Attachment {
	attachment.Actions = append(attachment.Actions, &action)
	return attachment
}

type SlackWebhook struct {
	client        *http.Client
	callbackURL   string
	buildBuddyURL string
	authenticator interfaces.Authenticator
}

func Register(env *real_environment.RealEnv) error {
	if *webhookURL != "" {
		env.SetWebhooks(
			append(env.GetWebhooks(), NewSlackWebhook(*webhookURL, build_buddy_url.String())),
		)
	}
	if *feedbackWebhookURL != "" {
		env.SetFeedbackReporter(NewFeedbackWebhook(*feedbackWebhookURL, build_buddy_url.String(), env.GetAuthenticator()))
	}
	return nil
}

func NewSlackWebhook(callbackURL string, bbURL string) *SlackWebhook {
	return &SlackWebhook{
		callbackURL:   callbackURL,
		buildBuddyURL: bbURL,
		client:        &http.Client{},
	}
}

// NewFeedbackWebhook returns a Slack webhook configured for feedback reports.
func NewFeedbackWebhook(callbackURL string, bbURL string, authenticator interfaces.Authenticator) *SlackWebhook {
	w := NewSlackWebhook(callbackURL, bbURL)
	w.authenticator = authenticator
	return w
}

func (w *SlackWebhook) slackPayloadFromInvocation(invocation *inpb.Invocation) *Payload {
	a := Attachment{}

	statusText := ""
	if invocation.Success {
		statusText = "✅ Succeeded"
	} else {
		statusText = "❌ Failed"
	}
	durationString := (time.Duration(invocation.DurationUsec) * time.Microsecond).String()
	a.AddField(Field{
		Title: "Status",
		Value: statusText,
	})
	a.AddAction(Action{
		Type:  "button",
		Text:  "See on BuildBuddy!",
		Url:   w.buildBuddyURL + "/invocation/" + invocation.InvocationId,
		Style: "primary",
	})
	return &Payload{
		Text:        invocation.User + "@" + invocation.Host + " completed a build in " + durationString,
		Attachments: []Attachment{a},
	}
}

func (w *SlackWebhook) NotifyComplete(ctx context.Context, invocation *inpb.Invocation) error {
	payload := w.slackPayloadFromInvocation(invocation)
	buf := new(bytes.Buffer)
	json.NewEncoder(buf).Encode(payload)
	_, err := http.Post(w.callbackURL, "application/json; charset=utf-8", buf)
	return err
}

func (w *SlackWebhook) slackPayloadFromFeedback(user interfaces.UserInfo, feedback string) *Payload {
	messagePrefix := feedbackMessagePrefix(user)
	fallback := messagePrefix
	a := Attachment{
		Fallback: &fallback,
		Text:     &feedback,
	}
	a.AddField(Field{
		Title: "Group ID",
		Value: user.GetGroupID(),
		Short: true,
	})
	if user.GetUserID() != "" {
		a.AddField(Field{
			Title: "User ID",
			Value: user.GetUserID(),
			Short: true,
		})
	}
	if apiKeyID := user.GetAPIKeyInfo().ID; apiKeyID != "" {
		a.AddField(Field{
			Title: "API key ID",
			Value: apiKeyID,
			Short: true,
		})
	}
	return &Payload{
		Text:        messagePrefix,
		Attachments: []Attachment{a},
	}
}

func feedbackMessagePrefix(user interfaces.UserInfo) string {
	parts := []string{"[MCP server]"}
	if groupIdentifier := feedbackGroupIdentifier(user); groupIdentifier != "" {
		parts = append(parts, "["+groupIdentifier+"]")
	}
	if userEmail := strings.TrimSpace(user.GetUserEmail()); userEmail != "" {
		parts = append(parts, "["+userEmail+"]")
	}
	return strings.Join(parts, " ")
}

func feedbackGroupIdentifier(user interfaces.UserInfo) string {
	if groupURLIdentifier := strings.TrimSpace(user.GetGroupURLIdentifier()); groupURLIdentifier != "" {
		return groupURLIdentifier
	}
	if groupName := strings.TrimSpace(user.GetGroupName()); groupName != "" {
		return groupName
	}
	return strings.TrimSpace(user.GetGroupID())
}

// ReportFeedback posts an authenticated feedback report to Slack.
func (w *SlackWebhook) ReportFeedback(ctx context.Context, feedback string) error {
	if w.authenticator == nil {
		return status.FailedPreconditionError("Slack feedback reporter requires an authenticator")
	}
	if strings.TrimSpace(feedback) == "" {
		return status.InvalidArgumentError("feedback is required")
	}
	user, err := w.authenticator.AuthenticatedUser(ctx)
	if err != nil {
		return err
	}
	return w.post(ctx, w.slackPayloadFromFeedback(user, feedback))
}

func (w *SlackWebhook) post(ctx context.Context, payload *Payload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return status.WrapErrorf(err, "marshal Slack payload")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.callbackURL, bytes.NewReader(body))
	if err != nil {
		return status.WrapErrorf(err, "build Slack request")
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := w.client.Do(req)
	if err != nil {
		return status.WrapErrorf(err, "post to Slack webhook")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return status.UnavailableErrorf("Slack webhook returned HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// Client posts messages to Slack webhook URLs.
type Client struct {
	httpClient *http.Client
}

func NewClient() *Client {
	return &Client{httpClient: &http.Client{Timeout: 30 * time.Second}}
}

func (c *Client) Post(ctx context.Context, webhookURL string, payload *Payload) error {
	if webhookURL == "" {
		return status.InvalidArgumentError("Slack webhook URL is required")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return status.WrapErrorf(err, "marshal Slack payload")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(body))
	if err != nil {
		return status.WrapErrorf(err, "build Slack request")
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return status.WrapErrorf(err, "post to Slack webhook")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return status.UnavailableErrorf("Slack webhook returned HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}
