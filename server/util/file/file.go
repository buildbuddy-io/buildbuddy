package file

import (
	"fmt"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

func ReadFile(configFile string) ([]byte, error) {
	_, err := os.Stat(configFile)
	if os.IsNotExist(err) {
		return nil, status.NotFoundErrorf("no file found at %s", configFile)
	}

	fileBytes, err := os.ReadFile(configFile)
	if err != nil {
		return nil, fmt.Errorf("error reading file: %s", err)
	}
	return fileBytes, nil
}
