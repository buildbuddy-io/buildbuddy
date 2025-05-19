package lockmap

import (
	"math/rand/v2"
	"strconv"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestLock(t *testing.T) {
	l := New()
	defer l.Close()

	m := make(map[string]int, 0)
	eg := errgroup.Group{}
	for i := 0; i < 100; i++ {
		eg.Go(func() error {
			ul := l.Lock("TestLock")
			m["samekey"] += 1
			ul()
			return nil
		})
	}
	eg.Wait()
	require.Equal(t, 100, m["samekey"])

	// Wait for gc. This test will time out if the single entry isn't cleaned
	// up eventually.
	for {
		if l.count() == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestRLock(t *testing.T) {
	l := New()
	defer l.Close()

	m := make(map[string]int, 0)
	eg := errgroup.Group{}
	for i := 0; i < 100; i++ {
		if i%5 == 0 {
			eg.Go(func() error {
				ul := l.Lock("TestRLock")
				m["samekey"] += 1
				ul()
				return nil
			})
		} else {
			eg.Go(func() error {
				ul := l.RLock("TestRLock")
				_ = m["samekey"]
				ul()
				return nil
			})
		}
	}
	eg.Wait()
	require.Equal(t, 20, m["samekey"])

	// Wait for gc. This test will time out if the single entry isn't cleaned
	// up eventually.
	for {
		if l.count() == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func benchmarkLock(b *testing.B, keyCount int) {
	l := New()
	defer l.Close()

	keys := make([]string, keyCount)
	var err error
	for i := range keys {
		keys[i], err = random.RandomString(64)
		require.NoError(b, err)
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := rand.IntN(len(keys))
		for pb.Next() {
			unlock := l.Lock(keys[i])
			unlock()
			i = (i + 1) % len(keys)
		}
	})
}

func BenchmarkLockQPS(b *testing.B) {
	// Fewer unique keys should have more contention.
	for _, keyCount := range []int{1, 10, 1000, 10000} {
		b.Run(strconv.Itoa(keyCount), func(b *testing.B) {
			benchmarkLock(b, keyCount)
		})
	}
}

func BenchmarkRLockQPS(b *testing.B) {
	l := New()
	defer l.Close()

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine gets a unique key, so there should be no contention.
		r, err := random.RandomString(64)
		require.Nil(b, err)

		for pb.Next() {
			unlock := l.RLock(r)
			unlock()
		}
	})
}
