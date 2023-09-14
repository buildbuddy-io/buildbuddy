package nbdserver_test

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/blockio"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/nbd/nbdserver"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"

	nbdpb "github.com/buildbuddy-io/buildbuddy/proto/nbd"
)

func TestNBDServer(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	// Make a blockio.Store from an empty file
	const fileSizeBytes = 1000
	tmp := testfs.MakeTempDir(t)
	path := filepath.Join(tmp, "f")
	err := os.WriteFile(path, make([]byte, fileSizeBytes), 0644)
	require.NoError(t, err)
	f, err := blockio.NewMmap(path)
	require.NoError(t, err)
	defer f.Close()
	// Create a server hosting a device backed by the file
	const deviceName = "test"
	dev := &nbdserver.Device{
		Store:    f,
		Metadata: &nbdpb.DeviceMetadata{Name: deviceName},
	}
	s, err := nbdserver.New(ctx, env, dev)
	require.NoError(t, err)

	// Test a few read/write operations
	{
		req := &nbdpb.ReadRequest{
			Name:   deviceName,
			Offset: 0,
			Length: fileSizeBytes,
		}
		res, err := s.Read(ctx, req)
		require.NoError(t, err)
		require.Equal(t, make([]byte, fileSizeBytes), res.Data, "should be all 0")
	}
	randBytes := make([]byte, 50)
	_, err = rand.Read(randBytes)
	require.NoError(t, err)
	{
		req := &nbdpb.WriteRequest{
			Name:   deviceName,
			Offset: 20,
			Data:   randBytes,
		}
		_, err := s.Write(ctx, req)
		require.NoError(t, err)
	}
	{
		req := &nbdpb.ReadRequest{
			Name:   deviceName,
			Offset: 5,
			Length: 40,
		}
		res, err := s.Read(ctx, req)
		require.NoError(t, err)
		expected := append(make([]byte, 15), randBytes[:25]...)
		require.Equal(t, expected, res.Data)
	}
}
