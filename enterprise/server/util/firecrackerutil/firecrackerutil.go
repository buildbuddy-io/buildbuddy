package firecrackerutil

import (
	"fmt"
	"io"
	"net/http"
)

const mmdsURL = "http://169.254.169.254/"

// FetchMMDSKey fetches data from MMDS (the microVM Metadata Service), a mutable
// data store supported in firecracker to share data between the host and guest.
// Note that a network interface must be enabled in the guest to use MMDS.
func FetchMMDSKey(key string) ([]byte, error) {
	resp, err := http.Get(mmdsURL + key)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("MMDS request failed with status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}
