//go:build linux && !android

package cpuset

import (
	"github.com/prometheus/procfs"
)

func GetCPUs() []int {
	fs, err := procfs.NewDefaultFS()
	if err != nil {
		return nil
	}
	cpuInfos, err := fs.CPUInfo()
	if err != nil {
		return nil
	}
	nodes := make([]int, len(cpuInfos))
	for i, cpuInfo := range cpuInfos {
		nodes[i] = int(cpuInfo.Processor)
	}
	return nodes
}
