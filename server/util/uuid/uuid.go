package uuid

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	guuid "github.com/google/uuid"
)

const (
	uuidContextKey = "uuid"

	// Name of the configuration directory in os.UserConfigDir
	configDirName = "buildbuddy"

	hostIDFilename = "host_id"
)

var (
	hostID      string
	hostIDError error
	hostIDOnce  sync.Once
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

func getOrCreateHostId() (string, error) {
	userConfigDir, err := os.UserConfigDir()
	if err != nil {
		// Home dir is not defined
		return "", err
	}
	configDirPath := path.Join(userConfigDir, configDirName)
	err = os.MkdirAll(configDirPath, 0755)
	if err != nil {
		return "", err
	}
	hostIDFilepath := path.Join(configDirPath, hostIDFilename)
	// try to create the file to write a new ID, if it already exists this will fail
	hostIDFile, err := os.OpenFile(hostIDFilepath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if err != os.ErrExist {
			// some other I/O error ocurred when creating the file, we can't write the ID down
			return "", err
		}
		// the file exists, read the file to get the host ID
		hostIDFile, err := os.Open(hostIDFilepath)
		if err != nil {
			return "", err
		}
		id, err := io.ReadAll(hostIDFile)
		if err != nil {
			return "", err
		}
		return string(id), nil
	}
	// we successfully opened the file, generate and record the host id
	id, err := guuid.NewRandom()
	if err != nil {
		// read failed from rand.Reader; basically this should never happen
		return "", err
	}
	if _, err = io.WriteString(hostIDFile, id.String()); err != nil {
		return "", err
	}
	return id.String(), nil
}

func GetHostID() (string, error) {
	hostIDOnce.Do(
		func() {
			hostID, hostIDError = getOrCreateHostId()
		},
	)
	return hostID, hostIDError
}
