package background

import (
	"context"
	"time"
)

type disconnectedContext struct {
	parent context.Context
}

func (ctx disconnectedContext) Deadline() (deadline time.Time, ok bool) {
	return
}
func (ctx disconnectedContext) Done() <-chan struct{} {
	return nil
}
func (ctx disconnectedContext) Err() error {
	return nil
}
func (ctx disconnectedContext) Value(key interface{}) interface{} {
	return ctx.parent.Value(key)
}

// Long story short: sometimes you need just a little more time to do a write
// or clean things up, even after a client has cancelled the request (and
// therefore the context) but you still need all the auth credentials and
// other values stored in the context. For that, we have this beauty. Use it
// to make a copy of your expired context and do your cleanup work.
func ExtendContextForFinalization(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx := disconnectedContext{parent: parent}
	return context.WithTimeout(ctx, timeout)
}
