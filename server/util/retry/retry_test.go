package retry_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/stretchr/testify/assert"
)

func TestMaxRetryFromBackoff(t *testing.T) {
	tests := map[string]struct {
		inputOptions        *retry.Options
		expectedMaxAttempts int
	}{
		"exact backoff boundary": {
			inputOptions: &retry.Options{
				InitialBackoff: 3,
				MaxBackoff:     24,
				Multiplier:     2,
				MaxRetries:     0,
			},
			expectedMaxAttempts: 4,
		},
		"above exact backoff boundary": {
			inputOptions: &retry.Options{
				InitialBackoff: 3,
				MaxBackoff:     25,
				Multiplier:     2,
				MaxRetries:     0,
			},
			expectedMaxAttempts: 5,
		},
		"below exact backoff boundary": {
			inputOptions: &retry.Options{
				InitialBackoff: 3,
				MaxBackoff:     23,
				Multiplier:     2,
				MaxRetries:     0,
			},
			expectedMaxAttempts: 4,
		},
		"manually set maximum above calculated backoff": {
			inputOptions: &retry.Options{
				InitialBackoff: 3,
				MaxBackoff:     23,
				Multiplier:     2,
				MaxRetries:     7,
			},
			expectedMaxAttempts: 7,
		},
		"manually set maximum below calculated backoff": {
			inputOptions: &retry.Options{
				InitialBackoff: 3,
				MaxBackoff:     23,
				Multiplier:     2,
				MaxRetries:     2,
			},
			expectedMaxAttempts: 2,
		},
		"initial backoff greater than maximum": {
			inputOptions: &retry.Options{
				InitialBackoff: 3,
				MaxBackoff:     1,
				Multiplier:     2,
				MaxRetries:     0,
			},
			expectedMaxAttempts: 1,
		},
		"multiplier less than or equal to one": {
			inputOptions: &retry.Options{
				InitialBackoff: 3,
				MaxBackoff:     7,
				Multiplier:     0.5,
				MaxRetries:     0,
			},
			expectedMaxAttempts: 1,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expectedMaxAttempts, retry.New(context.Background(), tc.inputOptions).MaxAttempts())
		})
	}
}
