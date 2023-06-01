package lockmap_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/lockmap"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestLock(t *testing.T) {
	l := lockmap.New()

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
}

func TestRLock(t *testing.T) {
	l := lockmap.New()

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
				ul := l.Lock("TestRLock")
				_, _ = m["samekey"]
				ul()
				return nil
			})
		}
	}
	eg.Wait()
	require.Equal(t, 20, m["samekey"])
}

func BenchmarkLockQPS(b *testing.B) {
	l := lockmap.New()
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
	l := lockmap.New()
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
