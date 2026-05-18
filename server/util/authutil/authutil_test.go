package authutil

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockEnv satisfies environment.Env for the subset used by ValidateRestrictedACAccess.
type mockEnv struct {
	environment.Env
	cis interfaces.ClientIdentityService
}

func (m *mockEnv) GetClientIdentityService() interfaces.ClientIdentityService { return m.cis }

// mockCIS returns a fixed identity (or error) from IdentityFromContext.
type mockCIS struct {
	identity *interfaces.ClientIdentity
	err      error
}

func (m *mockCIS) IdentityFromContext(context.Context) (*interfaces.ClientIdentity, error) {
	return m.identity, m.err
}
func (m *mockCIS) AddIdentityToContext(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (m *mockCIS) IdentityHeader(*interfaces.ClientIdentity, time.Duration) (string, error) {
	return "", nil
}
func (m *mockCIS) ValidateIncomingIdentity(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func TestValidateRestrictedACAccess(t *testing.T) {
	noIdentityErr := status.NotFoundError("identity not presented")

	for _, tc := range []struct {
		name         string
		instanceName string
		cis          interfaces.ClientIdentityService
		wantErr      bool
	}{
		{
			name:         "unrestricted instance name is always allowed",
			instanceName: "normal-instance",
			cis:          nil,
			wantErr:      false,
		},
		{
			name:         "empty instance name is always allowed",
			instanceName: "",
			cis:          nil,
			wantErr:      false,
		},
		{
			name:         "restricted prefix, no identity service",
			instanceName: interfaces.OCIImageInstanceNamePrefix,
			cis:          nil,
			wantErr:      true,
		},
		{
			name:         "restricted prefix with suffix, no identity service",
			instanceName: interfaces.OCIImageInstanceNamePrefix + "_manifest_content_",
			cis:          nil,
			wantErr:      true,
		},
		{
			name:         "restricted prefix, no identity in context",
			instanceName: interfaces.OCIImageInstanceNamePrefix,
			cis:          &mockCIS{err: noIdentityErr},
			wantErr:      true,
		},
		{
			name:         "restricted prefix, app identity",
			instanceName: interfaces.OCIImageInstanceNamePrefix,
			cis:          &mockCIS{identity: &interfaces.ClientIdentity{Client: interfaces.ClientIdentityApp}},
			wantErr:      false,
		},
		{
			name:         "restricted prefix, executor identity",
			instanceName: interfaces.OCIImageInstanceNamePrefix,
			cis:          &mockCIS{identity: &interfaces.ClientIdentity{Client: interfaces.ClientIdentityExecutor}},
			wantErr:      false,
		},
		{
			name:         "restricted prefix, cache-proxy identity",
			instanceName: interfaces.OCIImageInstanceNamePrefix,
			cis:          &mockCIS{identity: &interfaces.ClientIdentity{Client: interfaces.ClientIdentityCacheProxy}},
			wantErr:      false,
		},
		{
			name:         "restricted prefix, workflow identity",
			instanceName: interfaces.OCIImageInstanceNamePrefix,
			cis:          &mockCIS{identity: &interfaces.ClientIdentity{Client: interfaces.ClientIdentityWorkflow}},
			wantErr:      true,
		},
		{
			name:         "restricted prefix, empty client identity",
			instanceName: interfaces.OCIImageInstanceNamePrefix,
			cis:          &mockCIS{identity: &interfaces.ClientIdentity{Client: ""}},
			wantErr:      true,
		},
		{
			name:         "restricted prefix, unknown client identity",
			instanceName: interfaces.OCIImageInstanceNamePrefix,
			cis:          &mockCIS{identity: &interfaces.ClientIdentity{Client: "untrusted"}},
			wantErr:      true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := &mockEnv{cis: tc.cis}
			err := ValidateRestrictedACAccess(context.Background(), env, tc.instanceName)
			if tc.wantErr {
				require.Error(t, err)
				assert.True(t, status.IsUnauthenticatedError(err), "expected UnauthenticatedError, got: %v", err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestParseAPIKeyFromString(t *testing.T) {
	testCases := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name:    "non-empty key",
			input:   "--bes_results_url=http://localhost:8080/invocation/ --remote_header='x-buildbuddy-api-key=abc123'",
			want:    "abc123",
			wantErr: false,
		},
		{
			name:    "empty key",
			input:   "--bes_results_url=http://localhost:8080/invocation/ --remote_header='x-buildbuddy-api-key='",
			want:    "",
			wantErr: true,
		},
		{
			name:    "empty key in the middle",
			input:   "--bes_results_url=http://localhost:8080/invocation/ --remote_header='x-buildbuddy-api-key=' --bes_backend=grpc://localhost:1985",
			want:    "",
			wantErr: true,
		},
		{
			name:    "key not set",
			input:   "--bes_results_url=http://localhost:8080/invocation/",
			want:    "",
			wantErr: false,
		},
		{
			name:    "multiple API keys",
			input:   "--bes_results_url=http://localhost:8080/invocation/ --remote_header='x-buildbuddy-api-key=abc123' --remote_header='x-buildbuddy-api-key=abc456",
			want:    "abc456",
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			output, err := ParseAPIKeyFromString(tc.input)
			assert.Equal(t, tc.want, output)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
