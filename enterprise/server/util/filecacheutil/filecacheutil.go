package filecacheutil

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// Read atomically reads a file from filecache.
func Read(fc interfaces.FileCache, node *repb.FileNode) ([]byte, error) {
	tmp, err := tempPath(fc, node.GetDigest().GetHash())
	if err != nil {
		return nil, err
	}
	if !fc.FastLinkFile(node, tmp) {
		return nil, status.NotFoundErrorf("digest %s not found", node.GetDigest().GetHash())
	}
	defer func() {
		if err := os.Remove(tmp); err != nil {
			log.Warningf("Failed to remove filecache temp file: %s", err)
		}
	}()
	return os.ReadFile(tmp)
}

// Write atomically writes the given bytes to filecache.
func Write(fc interfaces.FileCache, node *repb.FileNode, b []byte) (n int, err error) {
	tmp, err := tempPath(fc, node.GetDigest().GetHash())
	if err != nil {
		return 0, err
	}
	f, err := os.Create(tmp)
	if err != nil {
		return 0, status.InternalErrorf("filecache temp file creation failed: %s", err)
	}
	defer func() {
		if err := os.Remove(tmp); err != nil {
			log.Warningf("Failed to remove filecache temp file: %s", err)
		}
	}()
	n, err = f.Write(b)
	if err != nil {
		return n, err
	}
	if err := fc.AddFile(node, tmp); err != nil {
		return 0, err
	}
	return n, nil
}

func tempPath(fc interfaces.FileCache, name string) (string, error) {
	randStr, err := random.RandomString(10)
	if err != nil {
		return "", err
	}
	return filepath.Join(fc.TempDir(), fmt.Sprintf("%s.%s.tmp", name, randStr)), nil
}
