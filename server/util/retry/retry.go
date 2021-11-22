//Package retry implements a exponential weighted backoff retry function.
package retry

import (
	"context"
	"math"
	"time"
)

// This code was inspired by cocroach's retry util.
type Options struct {
	InitialBackoff time.Duration // How long to wait after the first request
	MaxBackoff     time.Duration // Max amount of time to wait for a single request
	Multiplier     float64       // Next backoff is this * previous backoff
	MaxRetries     int           // Max number of retries; 0 is inf
}

type Retry struct {
	opts *Options
	ctx  context.Context

	currentAttempt int
	isReset        bool
}

func (r *Retry) Reset() {
	r.currentAttempt = 0
	r.isReset = true
}

// DefaultWithContext returns a new retry.Retry object that is ready to use.
// Example usage:
// ...
// retrier := retry.DefaultWithContext(ctx)
// for retrier.Next() {
//   doSomething()
// }
func DefaultWithContext(ctx context.Context) *Retry {
	r := &Retry{
		ctx: ctx,
		opts: &Options{
			InitialBackoff: 50 * time.Millisecond,
			MaxBackoff:     3 * time.Second,
			Multiplier:     2,
			MaxRetries:     0, // unlimited, time based max by default.
		},
	}
	r.Reset()
	return r
}

func (r *Retry) delay() time.Duration {
	backoff := float64(r.opts.InitialBackoff) * math.Pow(r.opts.Multiplier, float64(r.currentAttempt))
	if maxBackoff := float64(r.opts.MaxBackoff); backoff > maxBackoff {
		backoff = maxBackoff
	}
	return time.Duration(backoff)
}

func (r *Retry) Next() bool {
	// Run once, initially, always.
	if r.isReset {
		r.isReset = false
		return true
	}

	// If we're out of retries, exit.
	if r.opts.MaxRetries > 0 && r.currentAttempt == r.opts.MaxRetries {
		return false
	}

	select {
	case <-time.After(r.delay()):
		r.currentAttempt++
		return true
	case <-r.ctx.Done():
		return false
	}
}
