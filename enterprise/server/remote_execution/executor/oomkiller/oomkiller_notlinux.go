//go:build !linux

package oomkiller

func memoryPressureConfigFromFlags() memoryPressureConfig {
	return memoryPressureConfig{}
}

// NewMemoryMonitor returns the default executor memory monitor. cgroupPath is
// unused on non-linux platforms; the monitor measures system memory usage.
func NewMemoryMonitor(cgroupPath string) (MemoryMonitor, error) {
	return newSystemMemoryMonitor()
}
