package tables

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseMySQLVersion(t *testing.T) {
	testCases := []struct {
		name    string
		input   string
		want    *MySQLVersion
		wantErr bool
	}{
		{
			name:    "without suffix",
			input:   "5.7.60",
			want:    &MySQLVersion{major: 5, minor: 7},
			wantErr: false,
		},
		{
			name:    "with suffix",
			input:   "5.7.60-log",
			want:    &MySQLVersion{major: 5, minor: 7},
			wantErr: false,
		},
		{
			name:    "with suffix",
			input:   "ab5.7.60-log",
			want:    nil,
			wantErr: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseMySQLVersion(tc.input)
			assert.Equal(t, tc.want, got)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}

}
