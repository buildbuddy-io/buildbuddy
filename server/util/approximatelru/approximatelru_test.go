package approximatelru_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/util/approximatelru"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/stretchr/testify/require"
)

func TestContains(t *testing.T) {
	digests := make(map[digest.Key][]byte, 0)
	sizeFn := func(value interface{}) int64 {
		if buf, ok := value.([]byte); ok {
			return int64(len(buf))
		}
		return 0
	}
	evictFn := func(value interface{}) {
		buf, ok := value.([]byte)
		if !ok {
			return
		}
		for k, v := range digests {
			if string(v) == string(buf) {
				delete(digests, k)
				break
			}
		}
	}
	randomSampleFn := func() (interface{}, interface{}) {
		for k, v := range digests {
			log.Printf("randomSampleFn called! returning %s", k.Hash)
			return k.Hash, v
		}
		log.Errorf("Random sample returning nil")
		return nil, nil
	}

	l, err := approximatelru.New(&approximatelru.Config{
		MaxSize:      1000,
		OnEvict:      evictFn,
		SizeFn:       sizeFn,
		RandomSample: randomSampleFn,
	})
	require.Nil(t, err)

	for i := 0; i < 15; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		dk := digest.NewKey(d)
		digests[dk] = buf
		l.Add(d.GetHash(), buf)

		found := l.Contains(d.GetHash())
		require.True(t, found)
	}
}

func TestEviction(t *testing.T) {
	digestKeys := make([]digest.Key, 0)
	digests := make(map[digest.Key][]byte, 0)
	sizeFn := func(value interface{}) int64 {
		if buf, ok := value.([]byte); ok {
			return int64(len(buf))
		}
		return 0
	}
	evictFn := func(value interface{}) {
		buf, ok := value.([]byte)
		if !ok {
			return
		}
		var keyToDelete digest.Key
		for k, v := range digests {
			if string(v) == string(buf) {
				keyToDelete = k
				break
			}
		}
		delete(digests, keyToDelete)
		// note we don't delete from digestKeys because
		// thats used to compute a record of deletions
		// below.
	}
	randomSampleFn := func() (interface{}, interface{}) {
		for k, v := range digests {
			log.Printf("randomSampleFn called! returning %s", k.Hash)
			return k.Hash, v
		}
		log.Errorf("Random sample returning nil")
		return nil, nil
	}

	l, err := approximatelru.New(&approximatelru.Config{
		MaxSize:      100_000,
		OnEvict:      evictFn,
		SizeFn:       sizeFn,
		RandomSample: randomSampleFn,
	})
	require.Nil(t, err)

	// Fill the cache 100% full.
	for i := 0; i < 100; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, 1000)
		dk := digest.NewKey(d)
		digests[dk] = buf
		digestKeys = append(digestKeys, dk)
		l.Add(d.GetHash(), buf)

		found := l.Contains(d.GetHash())
		require.True(t, found)
	}

	// Now add 50% more stuff, ideally evicting half of
	// the keys that were previously added above.
	for i := 0; i < 50; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, 1000)
		dk := digest.NewKey(d)
		digests[dk] = buf
		digestKeys = append(digestKeys, dk)
		l.Add(d.GetHash(), buf)

		found := l.Contains(d.GetHash())
		require.True(t, found)
	}

	quartileEvictions := make([]int64, 3)
	for i, dk := range digestKeys {
		_, ok := digests[dk]
		if !ok {
			quartileEvictions[i/50] += 1
		}
	}

	// Ensure that at least 80% (40/50) of the evictions came from the
	// oldest keys.
	require.Greater(t, quartileEvictions[0], int64(40))

	// Ensure that less than 20% of the evictions came from the old, but
	// not oldest keys.
	require.Less(t, quartileEvictions[1], int64(10))

	// Ensure that less than 2% of the evictions came from the newly added
	// keys. Seems like it's usually 0.
	require.LessOrEqual(t, quartileEvictions[2], int64(1))
}
