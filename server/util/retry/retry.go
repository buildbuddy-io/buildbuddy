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
	MaxRetries     int           // Max number of retries; 0 is based on time
}

type Retry struct {
	opts *Options
	ctx  context.Context

	currentAttempt int
	maxAttempts    int
	isReset        bool
}

func New(ctx context.Context, opts *Options) *Retry {
	maxAttempts := opts.MaxRetries
	if maxAttempts <= 0 {
		// always try at least once
		maxAttempts = 1
		if opts.Multiplier > 1 && opts.MaxBackoff > opts.InitialBackoff {
			maxAttempts = 1 + int(math.Ceil(
				math.Log(
					float64(opts.MaxBackoff)/float64(opts.InitialBackoff),
				)/math.Log(opts.Multiplier),
			))
		}
	}

	r := &Retry{
		ctx:         ctx,
		opts:        opts,
		maxAttempts: maxAttempts,
	}
	r.Reset()
	return r
}

// DefaultWithContext returns a new retry.Retry object that is ready to use.
// Example usage:
// ...
// retrier := retry.DefaultWithContext(ctx)
// for retrier.Next() {
//   doSomething()
// }
func DefaultWithContext(ctx context.Context) *Retry {
	return New(
		ctx,
		&Options{
			InitialBackoff: 50 * time.Millisecond,
			MaxBackoff:     3 * time.Second,
			Multiplier:     2,
			MaxRetries:     0, // unlimited, time based max by default.
		},
	)
}

func (r *Retry) Reset() {
	r.currentAttempt = 0
	r.isReset = true
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
	if r.currentAttempt >= r.maxAttempts {
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

func (r *Retry) AttemptNumber() int {
	return r.currentAttempt + 1
}

func (r *Retry) MaxAttempts() int {
	return r.maxAttempts
}
