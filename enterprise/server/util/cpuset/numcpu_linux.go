//go:build linux && !android

package cpuset

import (
	"github.com/google/cadvisor/utils/sysfs"
	"github.com/google/cadvisor/utils/sysinfo"
)

func GetCPUs() ([]CPUInfo, error) {
	sys := sysfs.NewRealSysFs()
	nodeInfos, _, err := sysinfo.GetNodesInfo(sys)
	if err != nil {
		return nil, err
	}

	cpus := make([]CPUInfo, 0)
	for _, nodeInfo := range nodeInfos {
		for _, coreInfo := range nodeInfo.Cores {
			for _, thread := range coreInfo.Threads {
				c := CPUInfo{
					Processor: int(thread),
					NumaNode:  int(nodeInfo.Id),
				}
				cpus = append(cpus, c)
			}
		}
	}
	return cpus, nil
}
