//go:build linux && !android

package cpuset

import (
	"strconv"

	"github.com/prometheus/procfs"
)

func GetCPUs() ([]CPUInfo, error) {
	fs, err := procfs.NewDefaultFS()
	if err != nil {
		return nil, err
	}
	cpuInfos, err := fs.CPUInfo()
	if err != nil {
		return nil, err
	}

	nodes := make([]CPUInfo, len(cpuInfos))
	for i, info := range cpuInfos {
		c := CPUInfo{
			Processor: int(info.Processor),
		}
		if i, err := strconv.Atoi(info.PhysicalID); err == nil {
			c.PhysicalID = int(i)
		}
		nodes[i] = c
	}
	return nodes, nil
}
