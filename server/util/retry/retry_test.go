package retry_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaxRetryFromBackoff(t *testing.T) {
	tests := map[string]struct {
		inputOptions          *retry.Options
		expectedMaxAttempts   int
		expectedMaxTotalDelay time.Duration
	}{
		"exact backoff boundary": {
			inputOptions: &retry.Options{
				InitialBackoff: 3,
				MaxBackoff:     24,
				Multiplier:     2,
				MaxRetries:     0,
			},
			expectedMaxTotalDelay: 45 * time.Nanosecond,
		},
		"above exact backoff boundary": {
			inputOptions: &retry.Options{
				InitialBackoff: 3,
				MaxBackoff:     25,
				Multiplier:     2,
				MaxRetries:     0,
			},
			expectedMaxTotalDelay: 70 * time.Nanosecond,
		},
		"below exact backoff boundary": {
			inputOptions: &retry.Options{
				InitialBackoff: 3,
				MaxBackoff:     23,
				Multiplier:     2,
				MaxRetries:     0,
			},
			expectedMaxTotalDelay: 44 * time.Nanosecond,
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
			r := retry.New(context.Background(), tc.inputOptions)
			if tc.expectedMaxTotalDelay > 0 {
				assert.Equal(t, tc.expectedMaxTotalDelay, r.MaxTotalDelay())
				assert.Equal(t, 0, r.MaxAttempts())
			} else {
				assert.Equal(t, tc.expectedMaxAttempts, r.MaxAttempts())
				assert.Equal(t, 0*time.Second, r.MaxTotalDelay())
			}
		})
	}
}

func TestRetryInterval(t *testing.T) {
	r := retry.New(context.Background(), &retry.Options{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     3 * time.Second,
		Multiplier:     2,
		MaxRetries:     0, // unlimited, time based max by default.
	})
	var delays []time.Duration
	for {
		d, valid := r.NextDelay()
		if !valid {
			break
		}
		delays = append(delays, d)
	}
	expected := []time.Duration{
		0 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		3000 * time.Millisecond,
	}
	require.Equal(t, expected, delays)
}

func TestRetryWithFixedDelay(t *testing.T) {
	r := retry.New(context.Background(), &retry.Options{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     3 * time.Second,
		Multiplier:     2,
		MaxRetries:     0, // unlimited, time based max by default.
	})
	var delays []time.Duration
	for i := 0; ; i++ {
		d, valid := r.NextDelay()
		if !valid {
			break
		}
		delays = append(delays, d)
		if i == 3 || i == 4 {
			r.FixedDelayOnce(150 * time.Millisecond)
		}
	}
	expected := []time.Duration{
		0 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		150 * time.Millisecond,
		150 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		3000 * time.Millisecond,
	}
	require.Equal(t, expected, delays)
}
