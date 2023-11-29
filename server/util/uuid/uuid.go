package uuid

import (
	"context"
	"encoding/hex"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	guuid "github.com/google/uuid"
)

func SetInContext(ctx context.Context) (context.Context, error) {
	u, err := guuid.NewRandom()
	if err != nil {
		return nil, err
	}
	return log.EnrichContext(ctx, "request_id", u.String()), nil
}

// Base64StringToString converts a base64 encoding of the binary form of a UUID
// into its string representation,
// i.e. xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
func Base64StringToString(text string) (string, error) {
	decoded, err := hex.DecodeString(text)
	if err != nil {
		return "", status.InvalidArgumentErrorf("failed to parse uuid %q: %s", text, err)
	}
	uuid, err := guuid.FromBytes(decoded)
	if err != nil {
		return "", status.InvalidArgumentErrorf("failed to parse uuid %q: %s", text, err)
	}
	return uuid.String(), nil
}

// StringToBytes converts a string in the form
// of xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx into a binary form.
func StringToBytes(text string) ([]byte, error) {
	uuid, err := guuid.Parse(text)
	if err != nil {
		return nil, err
	}
	uuidBytes, err := uuid.MarshalBinary()
	if err != nil {
		return nil, status.InvalidArgumentErrorf("failed to parse uuid into bytes: %s", err)
	}
	return uuidBytes, nil
}

func New() string {
	return guuid.New().String()
}
