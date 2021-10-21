package webhooks

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"golang.org/x/oauth2"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	googleoauth "golang.org/x/oauth2/google"
)

const (
	gcsDomain         = "storage.googleapis.com"
	gcsReadWriteScope = "https://www.googleapis.com/auth/devstorage.read_write"
)

type invocationUploadHook struct {
	env environment.Env
}

// NewInvocationUploadHook returns a webhook that uploads the invocation proto
// contents to the webhook target using an HTTP PUT request.
func NewInvocationUploadHook(env environment.Env) interfaces.Webhook {
	return &invocationUploadHook{env}
}

func (h *invocationUploadHook) NotifyComplete(ctx context.Context, in *inpb.Invocation) error {
	groupID := in.GetAcl().GetGroupId()
	if groupID == "" {
		return nil
	}
	db := h.env.GetDBHandle()
	row := &struct{ InvocationWebhookURL string }{}
	err := db.Raw(
		`SELECT invocation_webhook_url FROM Groups WHERE group_id = ?`,
		groupID,
	).Take(row).Error
	if err != nil {
		return err
	}
	if row.InvocationWebhookURL == "" {
		return nil
	}

	u, err := url.Parse(row.InvocationWebhookURL)
	if err != nil {
		return err
	}
	if !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}
	u.Path += in.GetInvocationId()

	payload, err := proto.Marshal(in)
	if err != nil {
		return err
	}
	tokenSource, err := h.getTokenSource(ctx, u)
	if err != nil {
		return err
	}
	client := oauth2.NewClient(ctx, tokenSource)
	req, err := http.NewRequest(http.MethodPut, u.String(), bytes.NewReader(payload))
	if err != nil {
		return err
	}
	md5Sum := md5.Sum(payload)
	req.Header.Add("Content-Type", "application/octet-stream")
	req.Header.Add("Content-MD5", base64.StdEncoding.EncodeToString(md5Sum[:]))

	log.Infof("Uploading invocation proto to: %s", u.String())
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode >= 400 {
		// Include a snippet of the response body in the error message for easier
		// debugging.
		msg := ""
		if len(b) > 1000 {
			msg = string(b[:1000]) + "..."
		} else {
			msg = string(b)
		}
		return status.UnknownErrorf("HTTP %d while calling webhook: %s", res.StatusCode, msg)
	}

	return nil
}

// getTokenSource returns an OAuth token source for the webhook URL. For GCS,
// this returns a token source that generates tokens for the configured service
// account. Otherwise, it returns nil, which is properly handled by the oauth
// client.
func (h *invocationUploadHook) getTokenSource(ctx context.Context, u *url.URL) (oauth2.TokenSource, error) {
	if !strings.HasSuffix(u.Host, "."+gcsDomain) {
		return nil, nil
	}
	credsJSON := h.env.GetConfigurator().GetIntegrationsInvocationUploadConfig().GCSCredentialsJSON
	if credsJSON == "" {
		return nil, nil
	}
	cfg, err := googleoauth.JWTConfigFromJSON([]byte(credsJSON), gcsReadWriteScope)
	if err != nil {
		return nil, err
	}
	return cfg.TokenSource(ctx), nil
}
