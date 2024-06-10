package authutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
