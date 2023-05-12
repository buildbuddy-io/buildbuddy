package uuid

import (
	"context"
	"encoding/hex"
	"io"
	"os"
	"path"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	guuid "github.com/google/uuid"
)

const (
	// Name of the configuration directory in os.UserConfigDir
	configDirName  = "buildbuddy"
	hostIDFilename = "host_id"
)

var (
	hostID      string
	hostIDError error
	hostIDOnce  sync.Once

	failsafeID     string
	failsafeIDOnce sync.Once
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

func configDir() (string, error) {
	// HOME and XDG_CONFIG_HOME may not be defined when running with `bazel test`.
	if testTmpDir := os.Getenv("TEST_TMPDIR"); testTmpDir != "" {
		return os.MkdirTemp(testTmpDir, "buildbuddy-config-*")
	}

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
	return configDirPath, nil
}

func getOrCreateHostId() (string, error) {
	configDirPath, err := configDir()
	if err != nil {
		return "", err
	}
	hostIDFilepath := path.Join(configDirPath, hostIDFilename)
	// try to create the file to write a new ID, if it already exists this will fail
	hostIDFile, err := os.OpenFile(hostIDFilepath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if !os.IsExist(err) {
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

// GetFailsafeHostID is the "failsafe" version of GetHostID. If GetHostID fails
// then callers can use this method to generate a process-wide stable GUID like
// identifier. NB: If HostID is available this will return it.
func GetFailsafeHostID() string {
	failsafeIDOnce.Do(func() {
		hostID, err := GetHostID()
		if err == nil {
			failsafeID = hostID
			return
		}
		uuid, err := guuid.NewRandom()
		if err == nil {
			failsafeID = uuid.String()
			return
		}
		failsafe := guuid.New()
		failsafeID = string(failsafe.NodeID())
	})
	return failsafeID
}

func New() string {
	return guuid.New().String()
}
