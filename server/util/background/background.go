package background

import (
	"context"
	"sync"
	"time"
)

type disconnectedContext struct {
	parent context.Context
	done   chan struct{}

	mu  sync.Mutex
	err error
}

func newDisconnectedContext(parent context.Context) *disconnectedContext {
	return &disconnectedContext{
		parent: parent,
		done:   make(chan struct{}),
	}
}

// Deadline cannot be accurately computed for contexts that are extended
// for finalization, because the timeout takes effect once the parent
// context is canceled. This means that the deadline is variable. For now,
// just report no deadline.
// TODO: this behavior seems potentially problematic, maybe figure out
// something better.
func (ctx *disconnectedContext) Deadline() (deadline time.Time, ok bool) {
	return
}

func (ctx *disconnectedContext) Done() <-chan struct{} {
	return ctx.done
}

func (ctx *disconnectedContext) Err() error {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	return ctx.err
}

func (ctx *disconnectedContext) cancel(err error) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	// Once err is set, it cannot be re-set
	if ctx.err != nil {
		return
	}
	ctx.err = err
	close(ctx.done)
}
func (ctx *disconnectedContext) Value(key interface{}) interface{} {
	return ctx.parent.Value(key)
}

// ToBackground returns a background context from the given context, removing
// any cancellation and deadlines associated with the context, but preserving
// all context values such as auth info and outgoing gRPC metadata.
func ToBackground(ctx context.Context) context.Context {
	return newDisconnectedContext(ctx)
}

// Long story short: sometimes you need just a little more time to do a write
// or clean things up, even after a client has cancelled the request (and
// therefore the context) but you still need all the auth credentials and
// other values stored in the context. For that, we have this beauty. Use it
// to make a copy of your expired context and do your cleanup work.
func ExtendContextForFinalization(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx := newDisconnectedContext(parent)
	go func() {
		// Wait for the parent context to be canceled, then after the grace
		// period, cancel the extended context.
		select {
		case <-ctx.Done():
			return // cancelled manually
		case <-parent.Done():
		}
		t := time.NewTimer(timeout)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return // cancelled manually
		case <-t.C:
		}
		ctx.cancel(context.DeadlineExceeded)
	}()
	return ctx, func() { ctx.cancel(context.Canceled) }
}
