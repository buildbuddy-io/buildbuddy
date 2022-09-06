package errorcache

import (
	"context"
	"errors"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

type ErrorCache struct {
	interfaces.Cache
}

func (c *ErrorCache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	return errors.New("error cache set err")
}
