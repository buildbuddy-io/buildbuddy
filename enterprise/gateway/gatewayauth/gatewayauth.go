// Package gatewayauth defines how the gateway identifies its callers.
package gatewayauth

import (
	"context"
	"time"
)

// Principal identifies who a gateway RPC acts on behalf of.
type Principal struct {
	// User identifies the user for audit/logging purposes.
	// API key auth: BuildBuddy User ID
	// Cert auth: e-mail address
	User string

	// Namespace keys network and IP allocation, and scopes what List reveals. It
	// is a group ID for API-key callers and "cert:<email>" for certificate
	// callers.
	// Since the gateway drops packets that cross networks,
	// principals with different namespaces can never reach each other's peers.
	Namespace string

	// ExpiresAt bounds a registered peer's lifetime to the credential's own.
	// Zero means the credential imposes no deadline.
	ExpiresAt time.Time
}

type Authenticator interface {
	Authenticate(ctx context.Context, wgPublicKey string) (*Principal, error)
}
