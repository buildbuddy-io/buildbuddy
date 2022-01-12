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

func GetHostID() (string, error) {
	hostIDOnce.Do(
		func() {
			userConfigDir, hostIDError := os.UserConfigDir()
			if hostIDError != nil {
				// Home dir is not defined
				return
			}
			configDirPath := path.Join(userConfigDir, configDirName)
			hostIDError = os.MkdirAll(configDirPath, 0755)
			if hostIDError != nil {
				return
			}
			hostIDFilepath := path.Join(configDirPath, hostIDFilename)
			// try to create the file to write a new ID, if it already exists this will fail
			hostIDFile, hostIDError := os.OpenFile(hostIDFilepath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
			if hostIDError != nil {
				if hostIDError != os.ErrExist {
					// some other I/O error ocurred when creating the file, we can't write the ID down
					return
				}
				// the file exists, read the file to get the host ID
				hostIDFile, hostIDError := os.Open(hostIDFilepath)
				if hostIDError != nil {
					return
				}
				id, hostIDError := io.ReadAll(hostIDFile)
				if hostIDError != nil {
					return
				}
				hostID = string(id)
				return
			}
			// we successfully opened the file, generate and record the host id
			id, hostIDError := guuid.NewRandom()
			if hostIDError != nil {
				// read failed from rand.Reader; basically this should never happen
				return
			}
			if _, hostIDError = io.WriteString(hostIDFile, id.String()); hostIDError != nil {
				return
			}
			hostID = id.String()
		},
	)
	return hostID, hostIDError
}
