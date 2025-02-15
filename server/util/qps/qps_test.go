package qps_test

import (
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/qps"
	"github.com/stretchr/testify/require"
)

func TestQPS(t *testing.T) {
	ticker := make(chan time.Time, 1)
	counter := qps.NewCounterForTesting(5*time.Second, ticker)

	// Test cases where the ring buffer is not full.
	require.Equal(t, float64(0), counter.Get())
	counter.Inc()
	require.Equal(t, float64(12), counter.Get())
	for i := 0; i < 59; i++ {
		counter.Inc()
	}
	require.Equal(t, float64(720), counter.Get())
	for i := 0; i < 29; i++ {
		ticker <- time.Now()
	}
	// The current bin is incremented asynchronously upon receiving a tick via
	// the ticker channel. Give it some time to run.
	time.Sleep(10 * time.Millisecond)
	require.Equal(t, float64(24), counter.Get())

	// Test cases where the ring buffer is  full.
	for i := 0; i < 100; i++ {
		ticker <- time.Now()
	}
	time.Sleep(10 * time.Millisecond)
	require.Equal(t, float64(0), counter.Get())
	for i := 0; i < 61; i++ {
		ticker <- time.Now()
		time.Sleep(10 * time.Millisecond)
		counter.Inc()
	}
	require.Equal(t, float64(12), counter.Get())
	for i := 0; i < 60; i++ {
		ticker <- time.Now()
		time.Sleep(10 * time.Millisecond)
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
	ticker := make(chan time.Time, 1)
	counter := qps.NewCounterForTesting(5*time.Second, ticker)

	for i := 1; i < 1000; i++ {
		go func() {
			for j := i; j < 1000; j++ {
				counter.Inc()
				if i%2 == 0 {
					counter.Get()
				}
			}
			ticker <- time.Now()
		}()
	}
}
