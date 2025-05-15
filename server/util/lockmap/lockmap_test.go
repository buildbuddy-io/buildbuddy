package lockmap

import (
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

	// Wait for gc
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 0, l.count())
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

	// Wait for gc
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 0, l.count())
}

func BenchmarkLockQPS(b *testing.B) {
	l := New()
	defer l.Close()

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r, err := random.RandomString(64)
			require.Nil(b, err)
			unlock := l.Lock(r)
			unlock()
		}
	})
}

func BenchmarkRLockQPS(b *testing.B) {
	l := New()
	defer l.Close()

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		r, err := random.RandomString(64)
		require.Nil(b, err)

		for pb.Next() {
			unlock := l.RLock(r)
			unlock()
		}
	})
}
