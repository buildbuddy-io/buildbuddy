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

	cache := crypter_key_cache.New(env, refreshFn, clock)

	// NotFound errors should not be retried
	_, err := cache.EncryptionKey(ctx)
	require.True(t, status.IsNotFoundError(err))
	require.Equal(t, int32(1), refreshCount.Load())
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
