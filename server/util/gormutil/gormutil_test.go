package gormutil_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/gormutil"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

type LogEntry struct {
	Level   string `json:"level"`
	Time    string `json:"time"`
	Message string `json:"message"`
}

func TestLoggerError(t *testing.T) {
	tests := []struct {
		name   string
		format string
		args   []any
		want   string
	}{
		{
			name:   "simple error message",
			format: "failed to connect: %v",
			args:   []any{"connection refused"},
			want:   "server/util/gormutil/gormutil_test.go:58: failed to connect: connection refused",
		},
		{
			name:   "multiple arguments",
			format: "query failed: %s with error: %v",
			args:   []any{"SELECT * FROM users", "table not found"},
			want:   "server/util/gormutil/gormutil_test.go:58: query failed: SELECT * FROM users with error: table not found",
		},
		{
			name:   "no arguments",
			format: "simple error message",
			args:   []any{},
			want:   "server/util/gormutil/gormutil_test.go:58: simple error message",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			testLog := log.Output(&buf)

			log.Logger = testLog

			l := &gormutil.Logger{}

			ctx := context.Background()

			l.Error(ctx, tt.format, tt.args...)
			var logEntry LogEntry
			err := json.Unmarshal(buf.Bytes(), &logEntry)
			require.NoError(t, err)

			require.Equal(t, tt.want, logEntry.Message)

		})
	}
}
