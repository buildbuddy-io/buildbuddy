// Package certauth authenticates gateway users using certs.
package certauth

import (
	"context"
	"os"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/gatewayauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/relayauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"google.golang.org/grpc/metadata"
)

var (
	certAuthCA       = flag.String("gateway.cert_auth.ca", "", "PEM encoded CA certificate that issues certificates.")
	certAuthCAFile   = flag.String("gateway.cert_auth.ca_file", "", "Path to a PEM encoded CA certificate that issues certificates.")
	certAuthAudience = flag.String("gateway.cert_auth.audience", "", "This gateway's identity, which clients must name in their credential. Required.")
)

type certAuthenticator struct {
	verifier *relayauth.Verifier
}

func New() (gatewayauth.Authenticator, error) {
	if *certAuthCA == "" && *certAuthCAFile == "" {
		return nil, status.FailedPreconditionError("one of gateway.cert_auth.ca and gateway.cert_auth.ca_file is required")
	}
	caPEM := []byte(*certAuthCA)
	if *certAuthCAFile != "" {
		if *certAuthCA != "" {
			return nil, status.FailedPreconditionError("set only one of gateway.cert_auth.ca and gateway.cert_auth.ca_file")
		}
		b, err := os.ReadFile(*certAuthCAFile)
		if err != nil {
			return nil, status.FailedPreconditionErrorf("read gateway.cert_auth.ca_file: %s", err)
		}
		caPEM = b
	}
	if *certAuthAudience == "" {
		return nil, status.FailedPreconditionError("gateway.cert_auth.audience is required")
	}
	v, err := relayauth.NewVerifier(caPEM, *certAuthAudience)
	if err != nil {
		return nil, status.FailedPreconditionErrorf("configure tunnel certificate auth: %s", err)
	}
	log.Infof("Tunnel certificate auth enabled for audience %q", *certAuthAudience)
	return &certAuthenticator{verifier: v}, nil
}

// Authenticate verifies the cert-based credentials in ctx.
func (c *certAuthenticator) Authenticate(ctx context.Context, wgPublicKey string) (*gatewayauth.Principal, error) {
	values := metadata.ValueFromIncomingContext(ctx, relayauth.CredentialHeader)
	if len(values) == 0 {
		return nil, status.UnauthenticatedError("no tunnel credential provided")
	}
	if len(values) > 1 {
		return nil, status.UnauthenticatedError("multiple tunnel credentials provided")
	}

	id, err := c.verifier.Verify(values[0])
	if err != nil {
		log.Warningf("Rejected tunnel credential: %s", err)
		return nil, status.UnauthenticatedErrorf("tunnel credential is not valid: %s", err)
	}

	// The signed data contains the wireguard public key.
	// For RPCs that contain the public key (i.e. Connect), sanity-check that
	// the verified public key matches the provided public key.
	if wgPublicKey != "" && !strings.EqualFold(id.WireGuardPublicKey, wgPublicKey) {
		return nil, status.PermissionDeniedError("tunnel credential is bound to a different WireGuard public key")
	}

	return &gatewayauth.Principal{
		User: id.Email,
		// In the relay gateway, users never connect to each other so we give
		// each user their own namespace.
		Namespace: "cert:" + strings.ToLower(id.Email),
		ExpiresAt: id.CertNotAfter,
	}, nil
}
