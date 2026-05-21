// Package awsoidc exchanges BuildBuddy OIDC tokens for AWS credentials.
package awsoidc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ecr"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// DefaultAudience is the AWS STS OIDC token audience.
	DefaultAudience = "sts.amazonaws.com"

	// DefaultRegion is used for STS calls when a region cannot be inferred.
	DefaultRegion = "us-east-1"

	// AccessKeyIDEnvVar is the standard AWS access key ID environment variable.
	AccessKeyIDEnvVar = "AWS_ACCESS_KEY_ID"

	// SecretAccessKeyEnvVar is the standard AWS secret access key environment
	// variable.
	SecretAccessKeyEnvVar = "AWS_SECRET_ACCESS_KEY"

	// SessionTokenEnvVar is the standard AWS session token environment variable.
	SessionTokenEnvVar = "AWS_SESSION_TOKEN"

	// RegionEnvVar is the standard AWS region environment variable.
	RegionEnvVar = "AWS_REGION"
)

var ecrRegistryHostPattern = regexp.MustCompile(`^([0-9]{12})\.dkr\.ecr(?:-fips)?\.([a-z0-9-]+)\.amazonaws\.com(?:\.cn)?$`)

// Config contains AWS token exchange settings.
type Config struct {
	// RoleARN is the AWS IAM role ARN to assume.
	RoleARN string

	// Region is the AWS region used for STS and ECR clients. If empty, Exchange
	// uses DefaultRegion.
	Region string

	// RegistryID is the optional AWS account ID used to request ECR registry
	// credentials.
	RegistryID string

	// STSEndpoint overrides the AWS STS endpoint.
	STSEndpoint string

	// ECREndpoint overrides the AWS ECR endpoint.
	ECREndpoint string

	// TokenLifetime is the requested AWS STS credential lifetime.
	TokenLifetime time.Duration
}

// Credentials contains AWS credentials exchanged from a BuildBuddy OIDC token.
type Credentials struct {
	// Proto contains the env vars and optional registry credentials to pass to
	// the executor.
	Proto *repb.OIDCCredentials

	// ExpiresAt is the earliest expiration time for the exchanged AWS
	// credentials.
	ExpiresAt time.Time
}

// Exchange exchanges idToken for AWS credentials. If cfg.RegistryID is set, it
// also exchanges those credentials for an ECR authorization token.
func Exchange(ctx context.Context, cfg Config, idToken, groupID string) (*Credentials, error) {
	if cfg.RoleARN == "" {
		return nil, status.InvalidArgumentError("AWS role ARN is required")
	}
	if cfg.Region == "" {
		cfg.Region = DefaultRegion
	}
	durationSeconds := int64(cfg.TokenLifetime.Seconds())
	if durationSeconds < 900 {
		durationSeconds = 900
	}
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(cfg.Region),
	})
	if err != nil {
		return nil, status.UnavailableErrorf("create AWS session: %s", err)
	}
	stsConfig := &aws.Config{}
	if cfg.STSEndpoint != "" {
		stsConfig.Endpoint = aws.String(cfg.STSEndpoint)
	}
	stsResp, err := sts.New(sess, stsConfig).AssumeRoleWithWebIdentityWithContext(ctx, &sts.AssumeRoleWithWebIdentityInput{
		RoleArn:          aws.String(cfg.RoleARN),
		RoleSessionName:  aws.String(roleSessionName(groupID)),
		WebIdentityToken: aws.String(idToken),
		DurationSeconds:  aws.Int64(durationSeconds),
	})
	if err != nil {
		return nil, status.UnavailableErrorf("assume AWS role with web identity: %s", err)
	}
	if stsResp.Credentials == nil || stsResp.Credentials.AccessKeyId == nil || stsResp.Credentials.SecretAccessKey == nil || stsResp.Credentials.SessionToken == nil || stsResp.Credentials.Expiration == nil {
		return nil, status.UnavailableError("AWS STS response did not include complete credentials")
	}
	accessKeyID := aws.StringValue(stsResp.Credentials.AccessKeyId)
	secretAccessKey := aws.StringValue(stsResp.Credentials.SecretAccessKey)
	sessionToken := aws.StringValue(stsResp.Credentials.SessionToken)
	expiresAt := aws.TimeValue(stsResp.Credentials.Expiration)
	creds := &repb.OIDCCredentials{
		EnvVars: []string{
			fmt.Sprintf("%s=%s", RegionEnvVar, cfg.Region),
		},
		SecretEnvVars: []string{
			fmt.Sprintf("%s=%s", AccessKeyIDEnvVar, accessKeyID),
			fmt.Sprintf("%s=%s", SecretAccessKeyEnvVar, secretAccessKey),
			fmt.Sprintf("%s=%s", SessionTokenEnvVar, sessionToken),
		},
	}
	result := &Credentials{
		Proto:     creds,
		ExpiresAt: expiresAt,
	}
	if cfg.RegistryID != "" {
		registryPassword, registryExpiresAt, err := exchangeECR(ctx, cfg, accessKeyID, secretAccessKey, sessionToken)
		if err != nil {
			return nil, err
		}
		creds.ContainerRegistryUsername = "AWS"
		creds.ContainerRegistryPassword = registryPassword
		if registryExpiresAt.Before(result.ExpiresAt) {
			result.ExpiresAt = registryExpiresAt
		}
	}
	return result, nil
}

// ParseECRRegistryHost returns the AWS account ID and region encoded in a
// private ECR registry host.
func ParseECRRegistryHost(host string) (registryID, region string, ok bool) {
	matches := ecrRegistryHostPattern.FindStringSubmatch(host)
	if matches == nil {
		return "", "", false
	}
	return matches[1], matches[2], true
}

func exchangeECR(ctx context.Context, cfg Config, accessKeyID, secretAccessKey, sessionToken string) (string, time.Time, error) {
	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(accessKeyID, secretAccessKey, sessionToken),
		Region:      aws.String(cfg.Region),
	})
	if err != nil {
		return "", time.Time{}, status.UnavailableErrorf("create AWS ECR session: %s", err)
	}
	ecrConfig := &aws.Config{}
	if cfg.ECREndpoint != "" {
		ecrConfig.Endpoint = aws.String(cfg.ECREndpoint)
	}
	resp, err := ecr.New(sess, ecrConfig).GetAuthorizationTokenWithContext(ctx, &ecr.GetAuthorizationTokenInput{
		RegistryIds: []*string{aws.String(cfg.RegistryID)},
	})
	if err != nil {
		return "", time.Time{}, status.UnavailableErrorf("get AWS ECR authorization token: %s", err)
	}
	if len(resp.AuthorizationData) == 0 || resp.AuthorizationData[0] == nil || resp.AuthorizationData[0].AuthorizationToken == nil || resp.AuthorizationData[0].ExpiresAt == nil {
		return "", time.Time{}, status.UnavailableError("AWS ECR response did not include an authorization token")
	}
	decoded, err := base64.StdEncoding.DecodeString(aws.StringValue(resp.AuthorizationData[0].AuthorizationToken))
	if err != nil {
		return "", time.Time{}, status.UnavailableErrorf("decode AWS ECR authorization token: %s", err)
	}
	username, password, ok := strings.Cut(string(decoded), ":")
	if !ok || username == "" || password == "" {
		return "", time.Time{}, status.UnavailableError("AWS ECR authorization token was malformed")
	}
	return password, aws.TimeValue(resp.AuthorizationData[0].ExpiresAt), nil
}

func roleSessionName(groupID string) string {
	var b strings.Builder
	b.WriteString("buildbuddy-rbe-")
	for _, r := range groupID {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || strings.ContainsRune("_+=,.@-", r) {
			b.WriteRune(r)
		} else {
			b.WriteRune('-')
		}
	}
	sessionName := b.String()
	if len(sessionName) > 64 {
		sum := sha256.Sum256([]byte(sessionName))
		suffix := base64.RawURLEncoding.EncodeToString(sum[:])[:12]
		sessionName = sessionName[:64-len(suffix)-1] + "-" + suffix
	}
	return sessionName
}
