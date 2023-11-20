package hostid

import (
	"flag"
	"io"
	"os"
	"path"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
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

	manualHostID = flag.String("executor.host_id", "", "Optional: Allows for manual specification of an executor's host id. If not set, a random UUID will be used.")
)

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

func getOrCreateHostId(dir string) (string, error) {
	if *manualHostID != "" {
		return *manualHostID, nil
	}
	if dir == "" {
		d, err := configDir()
		if err != nil {
			return "", err
		}
		dir = d
	} else {
		// migrate the host_id, if possible
		// TODO (zoey): remove this after we expect most migrations to be done
		if oldConfigDir, err := configDir(); err == nil {
			oldHostIDFilepath := path.Join(oldConfigDir, hostIDFilename)
			newHostIDFilepath := path.Join(dir, hostIDFilename)
			// This call will move an old-style host id file (if it exists) to the new
			// host id file location.
			_ = disk.CopyViaTmpSibling(oldHostIDFilepath, newHostIDFilepath)
		}
	}
	hostIDFilepath := path.Join(dir, hostIDFilename)
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

func GetHostID(dir string) (string, error) {
	hostIDOnce.Do(
		func() {
			hostID, hostIDError = getOrCreateHostId(dir)
		},
	)
	return hostID, hostIDError
}

// GetFailsafeHostID is the "failsafe" version of GetHostID. If GetHostID fails
// then callers can use this method to generate a process-wide stable GUID like
// identifier. NB: If HostID is available this will return it.
func GetFailsafeHostID(dir string) string {
	failsafeIDOnce.Do(func() {
		hostID, err := GetHostID(dir)
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
