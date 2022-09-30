package approximatelru_test

import (
	"math/rand"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/util/approximatelru"
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
		keys := make([]digest.Key, 0, len(digests))
		for k, _ := range digests {
			keys = append(keys, k)
		}
		k := keys[rand.Intn(len(keys))]
		return k.Hash, digests[k]
	}

	l, err := approximatelru.New(&approximatelru.Config{
		MaxSize:      1000,
		OnEvict:      evictFn,
		SizeFn:       sizeFn,
		RandomSample: randomSampleFn,
	})
	require.NoError(t, err)

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
		keys := make([]digest.Key, 0, len(digests))
		for k, _ := range digests {
			keys = append(keys, k)
		}
		k := keys[rand.Intn(len(keys))]
		return k.Hash, digests[k]
	}

	totalNumKeys := 1000
	keySize := int64(100)
	l, err := approximatelru.New(&approximatelru.Config{
		MaxSize:      int64(totalNumKeys) * keySize,
		OnEvict:      evictFn,
		SizeFn:       sizeFn,
		RandomSample: randomSampleFn,
	})
	require.NoError(t, err)

	// Fill the cache 100% full.
	for i := 0; i < totalNumKeys; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, keySize)
		dk := digest.NewKey(d)
		digests[dk] = buf
		digestKeys = append(digestKeys, dk)
		l.Add(d.GetHash(), buf)

		found := l.Contains(d.GetHash())
		require.True(t, found)
	}

	// Now add 50% more stuff, ideally evicting half of
	// the keys that were previously added above.
	for i := 0; i < totalNumKeys/2; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, keySize)
		dk := digest.NewKey(d)
		digests[dk] = buf
		digestKeys = append(digestKeys, dk)
		l.Add(d.GetHash(), buf)

		found := l.Contains(d.GetHash())
		require.True(t, found)
	}

	totalEvictions := 0
	quartileEvictions := make([]int64, 3)
	quartileSize := len(digestKeys) / 3
	for i, dk := range digestKeys {
		_, ok := digests[dk]
		if !ok {
			quartileEvictions[i/quartileSize] += 1
			totalEvictions += 1
		}
	}

	// Ensure that at least 80% (40/50) of the evictions came from the
	// oldest keys.
	require.Greater(t, quartileEvictions[0], int64(.80*float64(totalEvictions)))

	// Ensure that less than 20% of the evictions came from the old, but
	// not oldest keys.
	require.Less(t, quartileEvictions[1], int64(.20*float64(totalEvictions)))

	// Ensure that less than 2% of the evictions came from the newly added
	// keys. Seems like it's usually 0.
	require.LessOrEqual(t, quartileEvictions[2], int64(.02*float64(totalEvictions)))
}

func TestEvictionOutOfOrder(t *testing.T) {
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
		keys := make([]digest.Key, 0, len(digests))
		for k, _ := range digests {
			keys = append(keys, k)
		}
		k := keys[rand.Intn(len(keys))]
		return k.Hash, digests[k]
	}

	totalNumKeys := 1000
	keySize := int64(100)
	l, err := approximatelru.New(&approximatelru.Config{
		MaxSize:      int64(totalNumKeys) * keySize,
		OnEvict:      evictFn,
		SizeFn:       sizeFn,
		RandomSample: randomSampleFn,
	})
	require.NoError(t, err)

	// Fill the cache 100% full.
	for i := 0; i < totalNumKeys; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, keySize)
		dk := digest.NewKey(d)
		digests[dk] = buf
		digestKeys = append(digestKeys, dk)
		l.Add(d.GetHash(), buf)

		found := l.Contains(d.GetHash())
		require.True(t, found)
	}

	// Now use everything that was added *first*.
	for i := 0; i < totalNumKeys/2; i++ {
		dk := digestKeys[i]
		found := l.Contains(dk.Hash)
		require.True(t, found)
	}

	// Now add 50% more stuff, ideally evicting half of
	// the keys that were previously added above.
	for i := 0; i < totalNumKeys/2; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, keySize)
		dk := digest.NewKey(d)
		digests[dk] = buf
		digestKeys = append(digestKeys, dk)
		l.Add(d.GetHash(), buf)

		found := l.Contains(d.GetHash())
		require.True(t, found)
	}

	totalEvictions := 0
	quartileEvictions := make([]int64, 3)
	quartileSize := len(digestKeys) / 3
	for i, dk := range digestKeys {
		_, ok := digests[dk]
		if !ok {
			quartileEvictions[i/quartileSize] += 1
			totalEvictions += 1
		}
	}

	// Ensure that at least 80% (40/50) of the evictions came from the
	// oldest keys.
	require.Greater(t, quartileEvictions[1], int64(.80*float64(totalEvictions)))

	// Ensure that less than 20% of the evictions came from the old, but
	// not oldest keys.
	require.Less(t, quartileEvictions[0], int64(.20*float64(totalEvictions)))

	// Ensure that less than 2% of the evictions came from the newly added
	// keys. Seems like it's usually 0.
	require.LessOrEqual(t, quartileEvictions[2], int64(.02*float64(totalEvictions)))
}
