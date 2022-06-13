package retry_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/stretchr/testify/assert"
)

func TestMaxRetryFromBackoff(t *testing.T) {
	r := retry.New(
		context.Background(),
		&retry.Options{
			InitialBackoff: 3,
			MaxBackoff:     24,
			Multiplier:     2,
			MaxRetries:     0,
		})
	assert.Equal(t, 4, r.MaxAttempts())
	r = retry.New(
		context.Background(),
		&retry.Options{
			InitialBackoff: 3,
			MaxBackoff:     25,
			Multiplier:     2,
			MaxRetries:     0,
		})
	assert.Equal(t, 5, r.MaxAttempts())
	r = retry.New(
		context.Background(),
		&retry.Options{
			InitialBackoff: 3,
			MaxBackoff:     23,
			Multiplier:     2,
			MaxRetries:     0,
		})
	assert.Equal(t, 4, r.MaxAttempts())
	r = retry.New(
		context.Background(),
		&retry.Options{
			InitialBackoff: 3,
			MaxBackoff:     23,
			Multiplier:     2,
			MaxRetries:     0,
		})
	assert.Equal(t, 4, r.MaxAttempts())
	assert.Equal(t, 4, r.MaxAttempts())
	r = retry.New(
		context.Background(),
		&retry.Options{
			InitialBackoff: 3,
			MaxBackoff:     23,
			Multiplier:     2,
			MaxRetries:     7,
		})
	assert.Equal(t, 7, r.MaxAttempts())
	r = retry.New(
		context.Background(),
		&retry.Options{
			InitialBackoff: 3,
			MaxBackoff:     23,
			Multiplier:     2,
			MaxRetries:     2,
		})
	assert.Equal(t, 2, r.MaxAttempts())
	r = retry.New(
		context.Background(),
		&retry.Options{
			InitialBackoff: 3,
			MaxBackoff:     23,
			Multiplier:     2,
			MaxRetries:     2,
		})
	assert.Equal(t, 2, r.MaxAttempts())
	r = retry.New(
		context.Background(),
		&retry.Options{
			InitialBackoff: 3,
			MaxBackoff:     1,
			Multiplier:     2,
			MaxRetries:     0,
		})
	assert.Equal(t, 1, r.MaxAttempts())
	r = retry.New(
		context.Background(),
		&retry.Options{
			InitialBackoff: 3,
			MaxBackoff:     7,
			Multiplier:     0.5,
			MaxRetries:     0,
		})
	assert.Equal(t, 1, r.MaxAttempts())
}
