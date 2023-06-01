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
