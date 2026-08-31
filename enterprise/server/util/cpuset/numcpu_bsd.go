//go:build freebsd || openbsd

package cpuset

import "runtime"

func GetCPUs() ([]CPUInfo, error) {
	n := runtime.NumCPU()
	processors := make([]int, n)
	for i := range processors {
		processors[i] = i
	}
	return toCPUInfos(processors, 0), nil
}
