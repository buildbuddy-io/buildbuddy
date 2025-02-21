package qps

import (
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestQPS(t *testing.T) {
	counter := NewCounter(5*time.Second, clockwork.NewFakeClock())
	defer counter.Stop()

	// Test cases where the ring buffer is not full.
	require.Equal(t, float64(0), counter.Get())
	counter.Inc()
	require.Equal(t, float64(12), counter.Get())
	for i := 0; i < 59; i++ {
		counter.Inc()
	}
	require.Equal(t, float64(720), counter.Get())
	for i := 0; i < 29; i++ {
		counter.update()
	}
	require.Equal(t, float64(24), counter.Get())

	// Test cases where the ring buffer is  full.
	for i := 0; i < 100; i++ {
		counter.update()
	}
	require.Equal(t, float64(0), counter.Get())
	for i := 0; i < 61; i++ {
		counter.update()
		counter.Inc()
	}
	require.Equal(t, float64(12), counter.Get())
	for i := 0; i < 60; i++ {
		counter.update()
		for j := 0; j < i; j++ {
			counter.Inc()
		}
	}
	// Sum(0, n) = n * (n-1) / 2
	// So, sum(0, 60) = 60 * 59 / 2 = 1,770
	// 1,770 Queries / 5s = 354 QPS.
	require.Equal(t, float64(354), counter.Get())
}

func TestRaciness(t *testing.T) {
	counter := NewCounter(5*time.Second, clockwork.NewFakeClock())
	defer counter.Stop()

	for i := 1; i < 1000; i++ {
		go func() {
			for j := i; j < 1000; j++ {
				counter.Inc()
				if i%2 == 0 {
					counter.Get()
				}
				if j%3 == 0 {
					counter.update()
				}
			}
		}()
	}
}
