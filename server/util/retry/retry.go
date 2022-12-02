// Package retry implements a exponential weighted backoff retry function.
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

	delayed time.Duration
	maxTime time.Duration

	isReset   bool
	nextDelay time.Duration
}

func New(ctx context.Context, opts *Options) *Retry {
	maxTime := 0 * time.Millisecond
	maxAttempts := opts.MaxRetries
	if maxAttempts <= 0 {
		if opts.Multiplier > 1 && opts.MaxBackoff > opts.InitialBackoff {
			tries := 1 + int(math.Ceil(
				math.Log(
					float64(opts.MaxBackoff)/float64(opts.InitialBackoff),
				)/math.Log(opts.Multiplier),
			))
			b := opts.InitialBackoff
			for i := 0; i < tries; i++ {
				maxTime += b
				b = time.Duration(math.Min(float64(b)*opts.Multiplier, float64(opts.MaxBackoff)))
			}
		} else {
			// always try at least once
			maxAttempts = 1
		}
	}

	r := &Retry{
		ctx:         ctx,
		opts:        opts,
		maxAttempts: maxAttempts,
		maxTime:     maxTime,
	}
	r.Reset()
	return r
}

// DefaultWithContext returns a new retry.Retry object that is ready to use.
// Example usage:
// ...
// retrier := retry.DefaultWithContext(ctx)
//
//	for retrier.Next() {
//	  doSomething()
//	}
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
	r.nextDelay = 0
}

func (r *Retry) updateNextDelay() {
	r.nextDelay = r.delay()
}

func (r *Retry) delay() time.Duration {
	backoff := float64(r.opts.InitialBackoff) * math.Pow(r.opts.Multiplier, float64(r.currentAttempt))
	if maxBackoff := float64(r.opts.MaxBackoff); backoff > maxBackoff {
		backoff = maxBackoff
	}
	return time.Duration(backoff)
}

func (r *Retry) NextDelay() (time.Duration, bool) {
	// Run once, initially, always.
	if r.isReset {
		r.isReset = false
		r.updateNextDelay()
		return 0, true
	}

	// If we're out of retries, exit.
	if r.maxAttempts > 0 && r.currentAttempt >= r.maxAttempts {
		return 0, false
	}

	// If we're out of time, exit.
	if r.maxTime > 0 && r.delayed >= r.maxTime {
		return 0, false
	}

	delay := r.nextDelay
	r.currentAttempt++
	r.delayed += delay
	r.updateNextDelay()
	return delay, true
}

// FixedDelayOnce causes the next retry to be attempted after a fixed delay.
// Subsequent retries go back to use the normal backoff delays.
func (r *Retry) FixedDelayOnce(delay time.Duration) {
	r.nextDelay = delay
	if r.currentAttempt > 0 {
		r.currentAttempt -= 1
	}
}

func (r *Retry) Next() bool {
	d, valid := r.NextDelay()
	if !valid {
		return false
	}

	select {
	case <-time.After(d):
		return true
	case <-r.ctx.Done():
		return false
	}
}

func (r *Retry) AttemptNumber() int {
	return r.currentAttempt + 1
}

// MaxAttempts the maximum numer of retry attempts.
// Only valid if retrier was created with MaxRetries set.
func (r *Retry) MaxAttempts() int {
	return r.maxAttempts
}

// MaxTotalDelay returns the bound on total time that the retrier will sleep
// across all attempts.
// Only valid if MaxRetries was not set.
func (r *Retry) MaxTotalDelay() time.Duration {
	return r.maxTime
}
