package keyval

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
)

func GetProto(ctx context.Context, store interfaces.KeyValStore, key string, msg proto.Message) error {
	marshaled, err := store.Get(ctx, key)
	if err != nil {
		return err
	}

	return proto.Unmarshal(marshaled, msg)
}

func SetProto(ctx context.Context, store interfaces.KeyValStore, key string, msg proto.Message) error {
	if msg == nil {
		return store.Set(ctx, key, nil)
	}

	marshaled, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	store.Set(ctx, key, marshaled)
	return nil
}
