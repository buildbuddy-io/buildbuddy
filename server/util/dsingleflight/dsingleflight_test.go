package dsingleflight

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type workResult struct {
	data []byte
	err  error
}

func TestDo(t *testing.T) {
	rdb := testredis.Start(t).Client()
	c := New(rdb)
	ctx := context.Background()

	rand.Seed(time.Now().UnixNano())
	key := fmt.Sprintf("key-%d", rand.Int())

	wg, ctx := errgroup.WithContext(ctx)

	numWorkers := 20
	results := make(chan *workResult, numWorkers)

	var mu sync.Mutex
	numExecutions := 0
	usedWorker := ""

	for i := 0; i < numWorkers; i++ {
		i := i
		worker := fmt.Sprintf("worker_%d", i)
		wg.Go(func() error {
			res, err := c.Do(ctx, key, func() ([]byte, error) {
				mu.Lock()
				numExecutions++
				usedWorker = worker
				mu.Unlock()
				time.Sleep(2 * time.Second)
				return []byte(worker), nil
			})
			results <- &workResult{res, err}
			return nil
		})
	}

	err := wg.Wait()
	require.NoError(t, err)

	require.Equal(t, 1, numExecutions, "expected work to be done only once")
	// all callers should have gotten the same result
	close(results)
	for result := range results {
		require.NoError(t, result.err)
		require.Equal(t, usedWorker, string(result.data))
	}
}

func TestDoError(t *testing.T) {
	rdb := testredis.Start(t).Client()
	c := New(rdb)
	ctx := context.Background()

	rand.Seed(time.Now().UnixNano())
	key := fmt.Sprintf("key-%d", rand.Int())

	wg, ctx := errgroup.WithContext(ctx)

	numWorkers := 20
	results := make(chan *workResult, numWorkers)

	var mu sync.Mutex
	numExecutions := 0

	for i := 0; i < numWorkers; i++ {
		wg.Go(func() error {
			res, err := c.Do(ctx, key, func() ([]byte, error) {
				mu.Lock()
				numExecutions++
				mu.Unlock()
				time.Sleep(2 * time.Second)
				return nil, status.UnavailableError("out of puppies")
			})
			results <- &workResult{res, err}
			return nil
		})
	}

	err := wg.Wait()
	require.NoError(t, err)

	require.Equal(t, 1, numExecutions, "expected work to be done only once")
	// all callers should have gotten the same result
	close(results)
	for result := range results {
		require.ErrorContains(t, result.err, "out of puppies")
		require.True(t, status.IsUnavailableError(result.err))
	}
}
