package perf

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBenchmark(t *testing.T) {
	// fib(45) = 1134903170, takes several seconds
	require.Equal(t, 1134903170, fib(45))
}

func fib(n int) int {
	if n <= 1 {
		return n
	}
	return fib(n-1) + fib(n-2)
}
