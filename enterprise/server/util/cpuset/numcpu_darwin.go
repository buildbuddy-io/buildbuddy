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
	for i := range len(cpuList.List) {
		nodes[i] = i
	}
	return toCPUInfos(nodes, 0), nil
}

func getNodeMemoryBytes() (map[int]int64, error) {
	mem := gosigar.Mem{}
	if err := mem.Get(); err != nil {
		return nil, err
	}
	// GetCPUs puts every CPU on node 0, so all memory is on node 0 too.
	return map[int]int64{0: int64(mem.Total)}, nil
}
