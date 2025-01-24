//go:build linux && !android

package cpuset

import (
	"github.com/prometheus/procfs"
	"strconv"
)

func GetCPUs() []cpuInfo {
	fs, err := procfs.NewDefaultFS()
	if err != nil {
		return nil
	}
	cpuInfos, err := fs.CPUInfo()
	if err != nil {
		return nil
	}

	nodes := make([]cpuInfo, len(cpuInfos))
	for i, info := range cpuInfos {
		c := cpuInfo{
			processor: int(info.Processor),
		}
		if i, err := strconv.Atoi(info.PhysicalID); err == nil {
			c.physicalID = int(i)
		}
		nodes[i] = c
	}
	return nodes
}
