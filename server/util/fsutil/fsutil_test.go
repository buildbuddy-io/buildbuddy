package fsutil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestFileSystemUtilization_UsesAvailableBytes(t *testing.T) {
	availableBytes := new(int64(0))
	fsu := &repb.UsageStats_FileSystemUsage{
		UsedBytes:      95,
		TotalBytes:     100,
		AvailableBytes: availableBytes,
	}
	assert.Equal(t, 1.0, FileSystemUtilization(fsu))
}

func TestFileSystemUtilization_FallsBackToTotalBytes(t *testing.T) {
	fsu := &repb.UsageStats_FileSystemUsage{
		UsedBytes:  95,
		TotalBytes: 100,
	}
	assert.Equal(t, 0.95, FileSystemUtilization(fsu))
}

func TestFileSystemUtilization_UsesUsedPlusAvailableDenominator(t *testing.T) {
	availableBytes := new(int64(5))
	fsu := &repb.UsageStats_FileSystemUsage{
		UsedBytes:      90,
		TotalBytes:     100,
		AvailableBytes: availableBytes,
	}
	assert.InDelta(t, float64(90)/float64(95), FileSystemUtilization(fsu), 1e-9)
}

func TestFileSystemUtilization_HandlesZeroDenominator(t *testing.T) {
	assert.Equal(t, 0.0, FileSystemUtilization(&repb.UsageStats_FileSystemUsage{}))
}
