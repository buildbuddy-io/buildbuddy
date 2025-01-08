//go:build windows

package cpuset

import (
	"github.com/elastic/gosigar"
)

func GetCPUs() []int {
	cpuList := gosigar.CpuList{}
	cpuList.Get()

	nodes := make([]int, len(cpuList.List))
	for i := 0; i < len(cpuList.List); i++ {
		nodes[i] = i
	}
	return nodes
}
