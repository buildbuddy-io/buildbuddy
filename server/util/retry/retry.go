// Package retry implements a exponential weighted backoff retry function.
package retry

import (
	"context"
	"math"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/jonboulle/clockwork"
)

// This code was inspired by cocroach's retry util.
type Options struct {
	// Max number of retries.
	//
	// If unset, the retry will be stopped once either the MaxBackoff has been
	// reached, or the context is done - whichever comes first.
	//
	// To only stop when the context is done, set to MaxInt.
	MaxRetries int

	InitialBackoff time.Duration // How long to wait after the first request
	MaxBackoff     time.Duration // Max amount of time to wait for a single request
	Multiplier     float64       // Next backoff is this * previous backoff

	Clock clockwork.Clock // Optional clock implementation to use.

	// Below options are only applicable to the Do function.
	Name                  string // Optional operation name for logging
	DontLogFailedAttempts bool   // If true, failed attempts will not be logged.
}

type Retry struct {
	opts  *Options
	ctx   context.Context
	clock clockwork.Clock

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

	clock := opts.Clock
	if clock == nil {
		clock = clockwork.NewRealClock()
	}

	r := &Retry{
		ctx:         ctx,
		opts:        opts,
		maxAttempts: maxAttempts,
		maxTime:     maxTime,
		clock:       clock,
	}
	r.Reset()
	return r
}

func DefaultOptions() *Options {
	return &Options{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     3 * time.Second,
		Multiplier:     2,
		MaxRetries:     0, // retry until backoff reaches max backoff
	}
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
	return New(ctx, DefaultOptions())
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
	case <-r.clock.After(d):
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

// Do executes the given function with a retry loop.
//
// The caller can indicate that an error should not be retried by wrapping it
// using NonRetryableError(err).
func Do[T any](ctx context.Context, opts *Options, fn func(ctx context.Context) (T, error)) (T, error) {
	r := New(ctx, opts)
	var lastError error
	for r.Next() {
		rsp, err := fn(ctx)
		if err != nil {
			lastError = err
			continue
		}
		if lastError != nil && !opts.DontLogFailedAttempts {
			name := opts.Name
			if name != "" {
				name += " "
			}
			log.CtxWarningf(ctx, "%sattempt failed, but succeeded on retry: %s", name, err)
		}
		return rsp, nil
	}
	return *new(T), lastError
}

// DoVoid is a convenience wrapper around Do for functions that do not return a
// value.
func DoVoid(ctx context.Context, opts *Options, fn func(ctx context.Context) error) error {
	_, err := Do(ctx, opts, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
	return err
}

type nonRetryableError struct {
	err error
}

func (e *nonRetryableError) Error() string {
	return e.err.Error()
}

func (e *nonRetryableError) Unwrap() error {
	return e.err
}

// NonRetryableError is used in conjunction with Do to indicate that a
// particular error should not be retried. Instead of returning the original
// error, returned the result of calling NonRetryableError(err).
func NonRetryableError(err error) error {
	return &nonRetryableError{err}
}
