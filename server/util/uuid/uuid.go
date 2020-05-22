package uuid

import (
	"context"
	"fmt"
	
	guuid "github.com/google/uuid"
)

const (
	uuidContextKey = "uuid"
)

func GetFromContext(ctx context.Context) (string, error) {
	u, ok := ctx.Value(uuidContextKey).(string)
	if ok {
		return u, nil
	}
	return "", fmt.Errorf("UUID not present in context")
}

func SetInContext(ctx context.Context) (context.Context, error) {
	u, err := guuid.NewRandom()
	if err != nil {
		return nil, err
	}
	ou, ok := ctx.Value(uuidContextKey).(string)
	if ok {
		return nil, fmt.Errorf("UUID %q already set in context!", ou)
	}
	return context.WithValue(ctx, uuidContextKey, u.String()), nil
}
