//go:build darwin && !ios

package cpuset

import (
	"github.com/elastic/gosigar"
)

func GetCPUs() ([]CPUInfo, error) {
	cpuList := gosigar.CpuList{}
	if err := cpuList.Get(); err != nil {
		return nil, err
	}

	nodes := make([]int, len(cpuList.List))
	for i := 0; i < len(cpuList.List); i++ {
		nodes[i] = i
	}
	return toCPUInfos(nodes, 0), nil
}
