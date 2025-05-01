package authutil_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
)

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
			output, err := authutil.ParseAPIKeyFromString(tc.input)
			assert.Equal(t, tc.want, output)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestHasCapabilityForSelectedGroup(t *testing.T) {
	for _, test := range []struct {
		name            string
		authenticator   interfaces.Authenticator
		authUserID      string
		checkCapability akpb.ApiKey_Capability
		wantGranted     bool
		wantErr         error
	}{
		{
			name:            "anon user has cache write capability",
			authenticator:   testauth.NewTestAuthenticator(testauth.TestUsers()),
			authUserID:      "",
			checkCapability: akpb.ApiKey_CACHE_WRITE_CAPABILITY,
			wantGranted:     true,
			wantErr:         nil,
		},
		{
			name:            "anon user has cache write capability with nullauth (OSS)",
			authenticator:   &nullauth.NullAuthenticator{},
			authUserID:      "",
			checkCapability: akpb.ApiKey_CACHE_WRITE_CAPABILITY,
			wantGranted:     true,
			wantErr:         nil,
		},
		{
			// TODO: is this a valid setup?
			name:            "anon user does not have cache write capability with nullauth with anon usage disabled",
			authenticator:   nullauth.NewNullAuthenticator(false, "" /*adminGroupID*/),
			authUserID:      "",
			checkCapability: akpb.ApiKey_CACHE_WRITE_CAPABILITY,
			wantGranted:     false,
			wantErr:         nil,
		},
		{
			name: "authenticated user has cache write capability for selected group",
			authenticator: testauth.NewTestAuthenticator(map[string]interfaces.UserInfo{
				"US1": &testauth.TestUser{
					UserID:       "US1",
					GroupID:      "GR1",
					Capabilities: []akpb.ApiKey_Capability{akpb.ApiKey_CACHE_WRITE_CAPABILITY},
				},
			}),
			authUserID:      "US1",
			checkCapability: akpb.ApiKey_CACHE_WRITE_CAPABILITY,
			wantGranted:     true,
			wantErr:         nil,
		},
		{
			name: "authenticated user does not have cache write capability for selected group",
			authenticator: testauth.NewTestAuthenticator(map[string]interfaces.UserInfo{
				"US1": &testauth.TestUser{
					UserID:       "US1",
					GroupID:      "GR1",
					Capabilities: []akpb.ApiKey_Capability{},
				},
			}),
			authUserID:      "US1",
			checkCapability: akpb.ApiKey_CACHE_WRITE_CAPABILITY,
			wantGranted:     false,
			wantErr:         nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			if test.authUserID != "" {
				c, err := test.authenticator.(*testauth.TestAuthenticator).WithAuthenticatedUser(ctx, test.authUserID)
				require.NoError(t, err)
				ctx = c
			}

			granted, err := authutil.HasCapabilityForSelectedGroup(ctx, test.authenticator, test.checkCapability)

			assert.Equal(t, test.wantGranted, granted)
			assert.Equal(t, test.wantErr, err)
		})
	}
}
