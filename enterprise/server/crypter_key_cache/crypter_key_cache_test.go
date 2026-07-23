package crypter_key_cache_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/crypter_key_cache"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

func setupAuth(t *testing.T, env *real_environment.RealEnv) context.Context {
	user := &testauth.TestUser{
		UserID:  "user1",
		GroupID: "group1",
	}
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1")))
	return testauth.WithAuthenticatedUserInfo(t.Context(), user)
}

func TestEncryptionKey(t *testing.T) {
	clock := clockwork.NewFakeClock()
	env := testenv.GetTestEnv(t)
	ctx := setupAuth(t, env)

	expectedKey := []byte("test-encryption-key")
	expectedMetadata := &sgpb.EncryptionMetadata{
		EncryptionKeyId: "key1",
		Version:         1,
	}

	refreshCount := 0
	refreshFn := func(ctx context.Context, ck crypter_key_cache.CacheKey) ([]byte, *sgpb.EncryptionMetadata, error) {
		refreshCount++
		require.Equal(t, "group1", ck.GroupID)
		require.Equal(t, "", ck.KeyID)
		require.EqualValues(t, 0, ck.Version)
		return expectedKey, expectedMetadata, nil
	}

	cache := crypter_key_cache.New(env, refreshFn, clock)

	// First call should trigger refresh
	key, err := cache.EncryptionKey(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedKey, key.Key)
	require.Equal(t, expectedMetadata, key.Metadata)
	require.Equal(t, 1, refreshCount)

	// Second call should use cache
	key, err = cache.EncryptionKey(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedKey, key.Key)
	require.Equal(t, expectedMetadata, key.Metadata)
	require.Equal(t, 1, refreshCount) // No additional refresh
}

func TestDecryptionKey(t *testing.T) {
	clock := clockwork.NewFakeClock()
	env := testenv.GetTestEnv(t)
	ctx := setupAuth(t, env)

	expectedKey := []byte("test-decryption-key")
	inputMetadata := &sgpb.EncryptionMetadata{
		EncryptionKeyId: "key123",
		Version:         2,
	}

	refreshCount := 0
	refreshFn := func(ctx context.Context, ck crypter_key_cache.CacheKey) ([]byte, *sgpb.EncryptionMetadata, error) {
		refreshCount++
		require.Equal(t, "group1", ck.GroupID)
		require.Equal(t, "key123", ck.KeyID)
		require.EqualValues(t, 2, ck.Version)
		return expectedKey, inputMetadata, nil
	}

	cache := crypter_key_cache.New(env, refreshFn, clock)

	// Test nil metadata
	_, err := cache.DecryptionKey(ctx, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "encryption metadata cannot be nil")

	// Test valid metadata
	key, err := cache.DecryptionKey(ctx, inputMetadata)
	require.NoError(t, err)
	require.Equal(t, expectedKey, key.Key)
	require.Equal(t, inputMetadata, key.Metadata)
	require.Equal(t, 1, refreshCount)

	// Second call should use cache
	key, err = cache.DecryptionKey(ctx, inputMetadata)
	require.NoError(t, err)
	require.Equal(t, expectedKey, key.Key)
	require.Equal(t, 1, refreshCount) // No additional refresh
}

func TestError(t *testing.T) {
	clock := clockwork.NewFakeClock()
	env := testenv.GetTestEnv(t)
	ctx := setupAuth(t, env)

	refreshCount := atomic.Int32{}
	refreshFn := func(ctx context.Context, ck crypter_key_cache.CacheKey) ([]byte, *sgpb.EncryptionMetadata, error) {
		refreshCount.Add(1)
		return nil, nil, status.UnavailableError("mainframe down")
	}

	opts := &crypter_key_cache.Opts{
		KeyRefreshScanFrequency: 10 * time.Second,
		KeyErrCacheTime:         5 * time.Second,
	}
	cache := crypter_key_cache.NewWithOpts(env, refreshFn, clock, opts)

	// Advance the clock asynchronously to keep the retrier moving along.
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(5 * time.Millisecond):
				clock.Advance(1 * time.Second)
			}
		}
	}()

	// First call should trigger refresh and cache the error
	_, err := cache.EncryptionKey(ctx)
	require.True(t, status.IsUnavailableError(err))
	refreshCount.Store(0)

	// Second call within error cache time should return cached error
	_, err = cache.EncryptionKey(ctx)
	require.True(t, status.IsUnavailableError(err))
	require.Equal(t, int32(0), refreshCount.Load())
}

func TestInvalidMetadata(t *testing.T) {
	clock := clockwork.NewFakeClock()
	env := testenv.GetTestEnv(t)
	ctx := setupAuth(t, env)

	refreshFn := func(ctx context.Context, ck crypter_key_cache.CacheKey) ([]byte, *sgpb.EncryptionMetadata, error) {
		return []byte("key"), nil, nil
	}

	cache := crypter_key_cache.New(env, refreshFn, clock)

	// Test metadata without key ID
	metadata := &sgpb.EncryptionMetadata{Version: 1}
	_, err := cache.DecryptionKey(ctx, metadata)
	require.True(t, status.IsFailedPreconditionError(err))

	// Test metadata without version
	metadata = &sgpb.EncryptionMetadata{EncryptionKeyId: "key123"}
	_, err = cache.DecryptionKey(ctx, metadata)
	require.True(t, status.IsFailedPreconditionError(err))
}

func TestNotFound(t *testing.T) {
	clock := clockwork.NewFakeClock()
	env := testenv.GetTestEnv(t)
	ctx := setupAuth(t, env)

	refreshCount := atomic.Int32{}
	refreshFn := func(ctx context.Context, ck crypter_key_cache.CacheKey) ([]byte, *sgpb.EncryptionMetadata, error) {
		refreshCount.Add(1)
		return nil, nil, status.NotFoundError("key not found")
	}

	opts := &crypter_key_cache.Opts{
		KeyRefreshScanFrequency: time.Hour,
		KeyErrCacheTime:         5 * time.Second,
	}
	cache := crypter_key_cache.NewWithOpts(env, refreshFn, clock, opts)

	// NotFound errors should not be retried
	_, err := cache.EncryptionKey(ctx)
	require.True(t, status.IsNotFoundError(err))
	require.Equal(t, int32(1), refreshCount.Load())

	// A second call within the error cache time should be served from the
	// error cache without another refresh.
	_, err = cache.EncryptionKey(ctx)
	require.True(t, status.IsNotFoundError(err))
	require.Equal(t, int32(1), refreshCount.Load())

	// Once the error cache entry expires, the key should be refreshed again.
	clock.Advance(6 * time.Second)
	_, err = cache.EncryptionKey(ctx)
	require.True(t, status.IsNotFoundError(err))
	require.Equal(t, int32(2), refreshCount.Load())
}

func TestKeyScopeDistinctness(t *testing.T) {
	clock := clockwork.NewFakeClock()
	env := testenv.GetTestEnv(t)
	ctx := setupAuth(t, env)

	backendCK := crypter_key_cache.CacheKey{GroupID: "group1"}
	customerCK := crypter_key_cache.CacheKey{GroupID: "group1", Scope: crypter_key_cache.KeyScopeCustomerDeployment}
	versionedBackendCK := crypter_key_cache.CacheKey{GroupID: "group1", KeyID: "key1", Version: 1}
	versionedCustomerCK := crypter_key_cache.CacheKey{GroupID: "group1", KeyID: "key1", Version: 1, Scope: crypter_key_cache.KeyScopeCustomerDeployment}

	// Keys differing only in scope must never collide in the cache or the
	// singleflight group, which are both keyed on String(). This must hold
	// for scopes that don't exist yet too.
	unknownScopeCK := crypter_key_cache.CacheKey{GroupID: "group1", Scope: crypter_key_cache.KeyScope(99)}
	for _, ck := range []crypter_key_cache.CacheKey{customerCK, unknownScopeCK} {
		require.NotEqual(t, backendCK.String(), ck.String())
	}
	require.NotEqual(t, customerCK.String(), unknownScopeCK.String())
	require.NotEqual(t, versionedBackendCK.String(), versionedCustomerCK.String())

	backendKey := []byte("backend-key")
	customerKey := []byte("customer-deployment-key")
	md := &sgpb.EncryptionMetadata{EncryptionKeyId: "key1", Version: 1}

	refreshCount := 0
	refreshFn := func(ctx context.Context, ck crypter_key_cache.CacheKey) ([]byte, *sgpb.EncryptionMetadata, error) {
		refreshCount++
		if ck.Scope == crypter_key_cache.KeyScopeCustomerDeployment {
			return customerKey, md, nil
		}
		return backendKey, md, nil
	}

	cache := crypter_key_cache.New(env, refreshFn, clock)

	// Loading the same group's key under each scope should trigger separate
	// refreshes and return separate keys.
	key, err := cache.LoadKey(ctx, backendCK)
	require.NoError(t, err)
	require.Equal(t, backendKey, key.Key)
	require.Equal(t, 1, refreshCount)

	key, err = cache.LoadKey(ctx, customerCK)
	require.NoError(t, err)
	require.Equal(t, customerKey, key.Key)
	require.Equal(t, 2, refreshCount)

	// Subsequent loads should be served from the cache, each scope keeping
	// its own entry.
	key, err = cache.LoadKey(ctx, backendCK)
	require.NoError(t, err)
	require.Equal(t, backendKey, key.Key)
	key, err = cache.LoadKey(ctx, customerCK)
	require.NoError(t, err)
	require.Equal(t, customerKey, key.Key)
	require.Equal(t, 2, refreshCount)

	// EncryptionKey uses the backend scope and should share the backend
	// entry.
	key, err = cache.EncryptionKey(ctx)
	require.NoError(t, err)
	require.Equal(t, backendKey, key.Key)
	require.Equal(t, 2, refreshCount)
}

func TestNonRetryableErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		checkFn func(error) bool
	}{
		{"PermissionDenied", status.PermissionDeniedError("no key for you"), status.IsPermissionDeniedError},
		{"Unauthenticated", status.UnauthenticatedError("who are you"), status.IsUnauthenticatedError},
		{"InvalidArgument", status.InvalidArgumentError("what is this"), status.IsInvalidArgumentError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := clockwork.NewFakeClock()
			env := testenv.GetTestEnv(t)
			ctx := setupAuth(t, env)

			refreshCount := atomic.Int32{}
			refreshFn := func(ctx context.Context, ck crypter_key_cache.CacheKey) ([]byte, *sgpb.EncryptionMetadata, error) {
				refreshCount.Add(1)
				return nil, nil, tc.err
			}

			opts := &crypter_key_cache.Opts{
				KeyRefreshScanFrequency: time.Hour,
				KeyErrCacheTime:         5 * time.Second,
			}
			cache := crypter_key_cache.NewWithOpts(env, refreshFn, clock, opts)

			// The error should not be retried and its status should be
			// preserved so that callers (e.g. a misconfigured customer-managed
			// deployment) can tell it apart from transient refresh failures.
			_, err := cache.EncryptionKey(ctx)
			require.True(t, tc.checkFn(err))
			require.Equal(t, int32(1), refreshCount.Load())

			// A second call within the error cache time should be served from
			// the error cache without another refresh.
			_, err = cache.EncryptionKey(ctx)
			require.True(t, tc.checkFn(err))
			require.Equal(t, int32(1), refreshCount.Load())
		})
	}
}

func TestCanceledRefreshIsNotCached(t *testing.T) {
	clock := clockwork.NewFakeClock()
	env := testenv.GetTestEnv(t)
	ctx := setupAuth(t, env)

	expectedKey := []byte("test-encryption-key")
	expectedMetadata := &sgpb.EncryptionMetadata{
		EncryptionKeyId: "key1",
		Version:         1,
	}

	refreshCount := atomic.Int32{}
	proceed := make(chan struct{})
	refreshFn := func(ctx context.Context, ck crypter_key_cache.CacheKey) ([]byte, *sgpb.EncryptionMetadata, error) {
		refreshCount.Add(1)
		select {
		case <-proceed:
			return expectedKey, expectedMetadata, nil
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}

	// Note: the fake clock never advances in this test, so if a canceled
	// refresh is incorrectly cached, the entry never expires and the second
	// lookup below observes it.
	cache := crypter_key_cache.New(env, refreshFn, clock)

	// Start a lookup, then cancel its context while the refresh is in flight.
	cancelCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		_, err := cache.EncryptionKey(cancelCtx)
		errCh <- err
	}()
	require.Eventually(t, func() bool { return refreshCount.Load() == 1 }, 5*time.Second, 1*time.Millisecond)
	cancel()
	require.Error(t, <-errCh)

	// Now let the refreshFn succeed. A subsequent refresh with a non-canceled
	// ctx should return a valid key.
	close(proceed)
	key, err := cache.EncryptionKey(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedKey, key.Key)
	require.Equal(t, expectedMetadata, key.Metadata)
}

func TestAlreadyCanceledContextDoesNotPoisonCache(t *testing.T) {
	clock := clockwork.NewFakeClock()
	env := testenv.GetTestEnv(t)
	ctx := setupAuth(t, env)

	expectedKey := []byte("test-encryption-key")
	expectedMetadata := &sgpb.EncryptionMetadata{
		EncryptionKeyId: "key1",
		Version:         1,
	}

	refreshFn := func(ctx context.Context, ck crypter_key_cache.CacheKey) ([]byte, *sgpb.EncryptionMetadata, error) {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		return expectedKey, expectedMetadata, nil
	}

	// There is some singleflighting happening internally, which involves some
	// background goroutines. Run several iterations to try to trigger different
	// race conditions.
	for i := 0; i < 50; i++ {
		// Use a fresh cache each iteration to ensure we initiate a new
		// singleflighted request. Note that the cache TTL is effectively
		// infinite since we never advance the clock.
		cache := crypter_key_cache.New(env, refreshFn, clock)

		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()
		_, err := cache.EncryptionKey(canceledCtx)
		require.Error(t, err)

		// Make sure the earlier canceled lookup didn't wind up caching the ctx
		// error or an invalid key.
		key, err := cache.EncryptionKey(ctx)
		require.NoError(t, err)
		require.Equal(t, expectedKey, key.Key)
		require.Equal(t, expectedMetadata, key.Metadata)
	}
}

func TestRefreshScan(t *testing.T) {
	clock := clockwork.NewFakeClock()
	env := testenv.GetTestEnv(t)
	ctx := setupAuth(t, env)

	refreshCount := 0
	refreshFn := func(ctx context.Context, ck crypter_key_cache.CacheKey) ([]byte, *sgpb.EncryptionMetadata, error) {
		refreshCount++
		return []byte("key"), nil, nil
	}

	opts := &crypter_key_cache.Opts{
		KeyRefreshScanFrequency: 5 * time.Second,
		KeyErrCacheTime:         10 * time.Second,
	}
	cache := crypter_key_cache.NewWithOpts(env, refreshFn, clock, opts)

	// Start refresher
	quitChan := make(chan struct{})
	defer close(quitChan)
	cache.StartRefresher(quitChan)

	// Load a key
	_, err := cache.EncryptionKey(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, refreshCount)

	// Advance time to trigger refresh scan
	clock.Advance(6 * time.Second)

	// Wait for refresh scan to complete
	for i := 0; i < 100 && cache.TestGetLastRefreshRun().IsZero(); i++ {
		time.Sleep(10 * time.Millisecond)
	}

	// Verify refresh scan ran
	require.False(t, cache.TestGetLastRefreshRun().IsZero())
}

func TestRaciness(t *testing.T) {
	clock := clockwork.NewRealClock()
	env := testenv.GetTestEnv(t)
	ctx := setupAuth(t, env)

	expectedKey := []byte("test-key")
	expectedMetadata := &sgpb.EncryptionMetadata{
		EncryptionKeyId: "key1",
		Version:         1,
	}
	refreshCount := atomic.Int32{}

	refreshFn := func(ctx context.Context, ck crypter_key_cache.CacheKey) ([]byte, *sgpb.EncryptionMetadata, error) {
		refreshCount.Add(1)
		return expectedKey, expectedMetadata, nil
	}
	opts := &crypter_key_cache.Opts{
		KeyRefreshScanFrequency: time.Hour,
		KeyErrCacheTime:         time.Hour,
	}
	cache := crypter_key_cache.NewWithOpts(env, refreshFn, clock, opts)

	// Launch multiple goroutines to access the same key
	var wg sync.WaitGroup
	numGoroutines := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key, err := cache.EncryptionKey(ctx)
			require.NoError(t, err)
			require.Equal(t, expectedKey, key.Key)
		}()
	}
	wg.Wait()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key, err := cache.DecryptionKey(ctx, expectedMetadata)
			require.NoError(t, err)
			require.Equal(t, expectedKey, key.Key)
		}()
	}
	wg.Wait()
}
