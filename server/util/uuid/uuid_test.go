package uuid_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/stretchr/testify/assert"
)

func TestBase64StringToString(t *testing.T) {
	tests := []struct {
		input   string
		output  string
		wantErr bool
	}{
		{
			input:   "cd86c9a3354f4e47b84e6357a945ff7f",
			output:  "cd86c9a3-354f-4e47-b84e-6357a945ff7f",
			wantErr: false,
		},
		{
			input:   "abcd",
			output:  "",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := uuid.Base64StringToString(tc.input)
			assert.Equal(t, got, tc.output)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestParseInvocationID(t *testing.T) {
	const id = "0f8fad5b-d9cb-469f-a165-70867728950e"
	const url = "https://app.buildbuddy.io/invocation/" + id

	for _, tc := range []struct {
		name  string
		input string
		want  string
		ok    bool
	}{
		{"bare ID", id, id, true},
		{"invocation URL", url, id, true},
		{"self-hosted URL", "https://buildbuddy.mycorp.internal/invocation/" + id, id, true},
		{"trailing slash", url + "/", id, true},
		{"query string", url + "?target=%2F%2Ffoo%3Abar", id, true},
		{"fragment", url + "#targets", id, true},
		{"trailing slash and query string", url + "/?target=x", id, true},
		{"uppercase is normalized", "0F8FAD5B-D9CB-469F-A165-70867728950E", id, true},
		{"mixed case URL is normalized", "https://app.buildbuddy.io/invocation/0F8FAD5B-d9cb-469F-a165-70867728950E", id, true},
		{"empty", "", "", false},
		{"not a uuid", "not-a-uuid", "", false},
		{"truncated uuid", "0f8fad5b-d9cb-469f-a165", "", false},
		{"uuid that is too long", id + "a", "", false},
		{"file path", "/tmp/execution_log.binpb", "", false},
		{"target label", "//foo:bar_test", "", false},
		{"trailing junk", id + "junk", "", false},
		{"leading junk", "junk" + id, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := uuid.ParseInvocationID(tc.input)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}
