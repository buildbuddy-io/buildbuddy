package webhooks

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/oauth2"
	"google.golang.org/protobuf/encoding/protojson"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	googleoauth "golang.org/x/oauth2/google"
)

var (
	enabled            = flag.Bool("integrations.invocation_upload.enabled", false, "Whether to upload webhook data to the webhook URL configured per-Group. ** Enterprise only **")
	gcsCredentialsJSON = flag.String("integrations.invocation_upload.gcs_credentials", "", "Credentials JSON for the Google service account used to authenticate when GCS is used as the invocation upload target. ** Enterprise only **", flag.Secret)
)

const (
	gcsDomain         = "storage.googleapis.com"
	gcsReadWriteScope = "https://www.googleapis.com/auth/devstorage.read_write"
)

type invocationUploadHook struct {
	env environment.Env
}

func Register(env *real_environment.RealEnv) error {
	if *enabled {
		env.SetWebhooks(
			append(env.GetWebhooks(), NewInvocationUploadHook(env)),
		)
	}
	return nil
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
	dbh := h.env.GetDBHandle()
	row := &struct{ InvocationWebhookURL string }{}
	err := dbh.RawWithOptions(
		ctx, db.Opts().WithStaleReads(),
		`SELECT invocation_webhook_url FROM "Groups" WHERE group_id = ?`,
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
	u.Path += in.GetInvocationId() + ".json"

	tokenSource, err := h.getTokenSource(ctx, u)
	if err != nil {
		return err
	}
	client := oauth2.NewClient(ctx, tokenSource)

	// Set up a pipeline of proto -> jsonpb -> gzip -> request body
	jsonpbPipeReader, jsonpbPipeWriter := io.Pipe()
	go func() {
		jsonBytes, err := protojson.Marshal(in)
		if err != nil {
			jsonpbPipeWriter.CloseWithError(err)
			return
		}
		_, err = jsonpbPipeWriter.Write(jsonBytes)
		jsonpbPipeWriter.CloseWithError(err)
	}()

	gzipPipeReader, gzipPipeWriter := io.Pipe()
	go func() {
		gzw := gzip.NewWriter(gzipPipeWriter)
		_, err := io.Copy(gzw, jsonpbPipeReader)
		if err != nil {
			gzipPipeWriter.CloseWithError(err)
			return
		}
		err = gzw.Close()
		gzipPipeWriter.CloseWithError(err)
	}()

	req, err := http.NewRequest(http.MethodPut, u.String(), gzipPipeReader)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Encoding", "gzip")

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
	if *gcsCredentialsJSON == "" {
		return nil, nil
	}
	cfg, err := googleoauth.JWTConfigFromJSON([]byte(*gcsCredentialsJSON), gcsReadWriteScope)
	if err != nil {
		return nil, err
	}
	return cfg.TokenSource(ctx), nil
}
