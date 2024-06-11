package webhooks

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/oauth2"
	"google.golang.org/protobuf/encoding/protojson"

	awscreds "github.com/aws/aws-sdk-go-v2/credentials"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	googleoauth "golang.org/x/oauth2/google"
)

var (
	enabled            = flag.Bool("integrations.invocation_upload.enabled", false, "Whether to upload webhook data to the webhook URL configured per-Group. ** Enterprise only **")
	gcsCredentialsJSON = flag.String("integrations.invocation_upload.gcs_credentials", "", "Credentials JSON for the Google service account used to authenticate when GCS is used as the invocation upload target. ** Enterprise only **", flag.Secret)
	awsCredentials     = flag.String("integrations.invocation_upload.aws_credentials", "", "Credentials CSV file for Amazon s3 invocation upload webhook. ** Enterprise only **", flag.Secret)
)

const (
	gcsDomain         = "storage.googleapis.com"
	gcsReadWriteScope = "https://www.googleapis.com/auth/devstorage.read_write"
	awsDomain         = "amazonaws.com"
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
	err := dbh.NewQueryWithOpts(ctx, "webhooks_get_url", db.Opts().WithStaleReads()).Raw(
		`SELECT invocation_webhook_url FROM "Groups" WHERE group_id = ?`,
		groupID,
	).Take(row)
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

	// Set up a pipeline of proto -> jsonpb -> gzip -> request body
	jsonpbPipeReader, jsonpbPipeWriter := io.Pipe()
	defer jsonpbPipeReader.Close()
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
	defer gzipPipeReader.Close()
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

	log.CtxInfof(ctx, "Uploading invocation events JSON to %s", u)
	if strings.HasSuffix(u.Host, "."+gcsDomain) {
		return h.gcsUpload(ctx, u, gzipPipeReader)
	} else if strings.HasSuffix(u.Host, "."+awsDomain) {
		return h.s3Upload(ctx, u, gzipPipeReader)
	} else {
		// Fall back to generic PUT request.
		return h.put(ctx, http.DefaultClient, u, gzipPipeReader)
	}
}

func (h *invocationUploadHook) gcsUpload(ctx context.Context, u *url.URL, gzr io.Reader) error {
	client := http.DefaultClient
	if *gcsCredentialsJSON != "" {
		cfg, err := googleoauth.JWTConfigFromJSON([]byte(*gcsCredentialsJSON), gcsReadWriteScope)
		if err != nil {
			return err
		}
		client = oauth2.NewClient(ctx, cfg.TokenSource(ctx))
	}
	return h.put(ctx, client, u, gzr)
}

func (h *invocationUploadHook) put(ctx context.Context, client *http.Client, u *url.URL, gzr io.Reader) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), gzr)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Encoding", "gzip")

	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	const limit = 1000
	b, err := io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil {
		return err
	}
	if res.StatusCode >= 400 {
		// Include a snippet of the response body in the error message for easier
		// debugging.
		msg := ""
		if len(b) > limit {
			msg = string(b[:limit]) + "..."
		} else {
			msg = string(b)
		}
		return status.UnknownErrorf("HTTP %d while calling webhook: %s", res.StatusCode, msg)
	}
	return nil
}

func (h *invocationUploadHook) s3Upload(ctx context.Context, u *url.URL, gzr io.Reader) error {
	// Parse bucket and region from URL
	parts := strings.Split(u.Host, ".")
	if len(parts) != 5 || parts[1] != "s3" {
		return status.InvalidArgumentErrorf("malformed s3 bucket domain %q: expected '{bucket}.s3.{region}.amazonaws.com'", u.Host)
	}
	bucket := parts[0]
	region := parts[2]
	key := u.Path

	// TODO: only create this config/client once per region?
	creds, err := getAWSCreds()
	if err != nil {
		return status.FailedPreconditionErrorf("get AWS creds: %s", err)
	}
	client := s3.NewFromConfig(aws.Config{
		Region:      region,
		Credentials: &awscreds.StaticCredentialsProvider{Value: *creds},
	})
	uploader := s3manager.NewUploader(client)
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		Body:            gzr,
		ContentEncoding: aws.String("gzip"),
	})
	if err != nil {
		return status.UnknownErrorf("upload: %s", err)
	}
	return nil
}

func getAWSCreds() (*aws.Credentials, error) {
	rs, err := csv.NewReader(strings.NewReader(*awsCredentials)).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rs) != 2 {
		return nil, status.FailedPreconditionErrorf("credentials file not in valid format (expected 2 rows, got %d)", len(rs))
	}
	if len(rs[1]) != 2 {
		return nil, status.FailedPreconditionErrorf("credential file not in valid format (expected 2 columns, got %d", len(rs[0]))
	}
	return &aws.Credentials{
		AccessKeyID:     rs[1][0],
		SecretAccessKey: rs[1][1],
	}, nil
}
