package fsutil

import (
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// FileSystemUtilization returns the utilization ratio for a filesystem usage
// sample. It prefers "used / (used + available)" which reflects user-visible
// capacity, accounting for reserved ext4 blocks not writable by unprivileged
// users. Falls back to "used / total" when available bytes are not reported.
//
// Sources:
//   - https://man7.org/linux/man-pages/man3/statvfs.3.html (f_bavail)
//   - https://man7.org/linux/man-pages/man8/tune2fs.8.html (-m reserved blocks)
func FileSystemUtilization(fsu *repb.UsageStats_FileSystemUsage) float64 {
	if fsu == nil {
		return 0
	}
	if fsu.AvailableBytes != nil {
		usedPlusAvailable := float64(fsu.GetUsedBytes()) + float64(fsu.GetAvailableBytes())
		if usedPlusAvailable > 0 {
			return float64(fsu.GetUsedBytes()) / usedPlusAvailable
		}
	}
	if fsu.GetTotalBytes() > 0 {
		return float64(fsu.GetUsedBytes()) / float64(fsu.GetTotalBytes())
	}
	return 0
}
