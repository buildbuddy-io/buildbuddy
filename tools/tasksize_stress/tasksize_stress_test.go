package tasksize_stress_test

import (
	"flag"
	"sync"
	"testing"
	"time"
)

var (
	duration = flag.Duration("duration", 15*time.Second, "How long to run each test.")
)

func TestMemory1G(t *testing.T) {
	holdMemory(1e9, *duration)
}

func TestMemory2G(t *testing.T) {
	holdMemory(2e9, *duration)
}

func TestMemory4G(t *testing.T) {
	holdMemory(4e9, *duration)
}

func TestCPU1(t *testing.T) {
	fullyUtilizeCPUCores(1, *duration)
}

func TestCPU2(t *testing.T) {
	fullyUtilizeCPUCores(2, *duration)
}

func TestCPU4(t *testing.T) {
	fullyUtilizeCPUCores(4, *duration)
}

func TestCPU8(t *testing.T) {
	fullyUtilizeCPUCores(8, *duration)
}

func fullyUtilizeCPUCores(numGoroutines int, dur time.Duration) {
	var wg sync.WaitGroup
	end := time.Now().Add(dur)
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(end) {
			}
		}()
	}
	wg.Wait()
}

func holdMemory(size int64, dur time.Duration) {
	arr := make([]byte, size)
	// sparsely fill the array so that we physically use all the allocated
	// memory pages without using a ton of CPU.
	for i := 0; i < len(arr); i += 4096 {
		arr[i] = 1
	}
	time.Sleep(dur)
}
