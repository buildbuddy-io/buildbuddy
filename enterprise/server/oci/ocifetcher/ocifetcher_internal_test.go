package ocifetcher

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	ctrname "github.com/google/go-containerregistry/pkg/name"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func newInternalTestServer(t *testing.T) *ociFetcherServer {
	// withPullerRetry never touches the cache, so the clients are never used.
	server, err := NewServer(bspb.NewByteStreamClient(nil), repb.NewActionCacheClient(nil))
	require.NoError(t, err)
	return server.(*ociFetcherServer)
}

// Requests that fail on the same cached Puller should share one replacement,
// rather than evicting each other's replacements.
func TestWithPullerRetryConcurrentFailuresShareReplacement(t *testing.T) {
	s := newInternalTestServer(t)
	ref := ctrname.MustParseReference("example.com/repo:tag")
	original, err := s.getOrCreatePuller(t.Context(), ref, nil)
	require.NoError(t, err)

	const numRequests = 10
	var firstAttempts sync.WaitGroup
	firstAttempts.Add(numRequests)
	firstPullers := make([]*remote.Puller, numRequests)
	retryPullers := make([]*remote.Puller, numRequests)
	errs := make([]error, numRequests)
	var wg sync.WaitGroup
	for i := range numRequests {
		wg.Go(func() {
			attempts := 0
			_, errs[i] = withPullerRetry(t.Context(), s, ref, nil, func(puller *remote.Puller) (struct{}, error) {
				attempts++
				if attempts > 1 {
					retryPullers[i] = puller
					return struct{}{}, nil
				}
				firstPullers[i] = puller
				// Fail only once every request holds the original Puller,
				// as when they all wait on a handshake that another request
				// canceled.
				firstAttempts.Done()
				firstAttempts.Wait()
				return struct{}{}, context.Canceled
			})
		})
	}
	wg.Wait()

	for i := range numRequests {
		require.NoError(t, errs[i])
		require.Same(t, original, firstPullers[i])
		require.NotSame(t, original, retryPullers[i])
		require.Same(t, retryPullers[0], retryPullers[i])
	}
	cached, err := s.getOrCreatePuller(t.Context(), ref, nil)
	require.NoError(t, err)
	require.Same(t, retryPullers[0], cached)
}

// When the current request's context is done, withPullerRetry should return
// that context's error, even if the operation failed for another reason, and
// it should neither retry nor evict the Puller.
func TestWithPullerRetryReturnsCurrentContextError(t *testing.T) {
	for _, tc := range []struct {
		name string
		// The attempt during which the request is canceled, or 0 if the
		// request's deadline has already passed.
		cancelAttempt int
		want          error
	}{
		{name: "CanceledOnFirstAttempt", cancelAttempt: 1, want: context.Canceled},
		{name: "CanceledOnRetry", cancelAttempt: 2, want: context.Canceled},
		{name: "DeadlineExceeded", cancelAttempt: 0, want: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newInternalTestServer(t)
			ref := ctrname.MustParseReference("example.com/repo:tag")
			ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
			if tc.cancelAttempt > 0 {
				ctx, cancel = context.WithCancel(t.Context())
			}
			defer cancel()

			attempts := 0
			var lastPuller *remote.Puller
			_, err := withPullerRetry(ctx, s, ref, nil, func(puller *remote.Puller) (struct{}, error) {
				attempts++
				lastPuller = puller
				if attempts == tc.cancelAttempt {
					cancel()
				}
				return struct{}{}, errors.New("registry error")
			})

			require.Equal(t, tc.want, err)
			require.Equal(t, max(tc.cancelAttempt, 1), attempts)
			// The Puller that failed because of the canceled request is kept.
			cached, err := s.getOrCreatePuller(t.Context(), ref, nil)
			require.NoError(t, err)
			require.Same(t, lastPuller, cached)
		})
	}
}
