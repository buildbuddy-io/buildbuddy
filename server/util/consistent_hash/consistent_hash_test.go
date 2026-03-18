package consistent_hash_test

import (
	"bufio"
	"fmt"
	"io"
	"maps"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/consistent_hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Number of vnodes serving as a reasonable default for testing purposes.
const numVnodes = 100

func TestNodesetOrderIndependence(t *testing.T) {
	assert := assert.New(t)
	ch := consistent_hash.NewConsistentHash(consistent_hash.CRC32, numVnodes)

	hosts := make([]string, 0)
	for i := 0; i < 10; i++ {
		r, err := random.RandomString(5)
		assert.Nil(err)
		hosts = append(hosts, fmt.Sprintf("%s:%d", r, 1000+i))
	}

	if err := ch.Set(hosts...); err != nil {
		t.Fatal(err)
	}

	mappings := make(map[string]string, 0)
	for i := 0; i < 1000; i++ {
		r, err := random.RandomString(64)
		assert.Nil(err)
		mappings[r] = ch.Get(r)
	}

	rand.Shuffle(len(hosts), func(i, j int) {
		hosts[i], hosts[j] = hosts[j], hosts[i]
	})
	if err := ch.Set(hosts...); err != nil {
		t.Fatal(err)
	}

	for d, host := range mappings {
		assert.Equal(host, ch.Get(d))
		log.Debugf("d %q => host %q", d, host)
	}
}

func TestGetAllReplicas(t *testing.T) {
	ch := consistent_hash.NewConsistentHash(consistent_hash.CRC32, numVnodes)

	for _, numHosts := range []int{0, 1, 10} {
		t.Run(fmt.Sprintf("%vhosts", numHosts), func(t *testing.T) {
			assert := assert.New(t)
			hosts := make([]string, 0, numHosts)
			for i := 0; i < numHosts; i++ {
				r, err := random.RandomString(5)
				assert.Nil(err)
				hosts = append(hosts, fmt.Sprintf("%s:%d", r, 1000+i))
			}

			if err := ch.Set(hosts...); err != nil {
				t.Fatal(err)
			}

			for i := 0; i < 100; i++ {
				k, err := random.RandomString(64)
				assert.Nil(err)
				replicas := ch.GetAllReplicas(k)
				assert.Equal(numHosts, len(replicas))
			}
		})
	}
}

func TestEvenLoadDistribution(t *testing.T) {
	rng := bufio.NewReader(rand.New(rand.NewSource(time.Now().UnixNano())))

	// Record the frequency of each host returned by Get() across 1M random
	// keys, then compute the "skew" as (max_freq-min_freq)/min_freq. Roughly,
	// higher skew means less evenly balanced load.

	var hosts []string
	for i := 0; i < 16; i++ {
		r, err := random.RandomString(5)
		require.NoError(t, err)
		hosts = append(hosts, fmt.Sprintf("%s:%d", r, 1000+i))
	}
	// Use SHA256 and 10K replicas for better load balancing.
	ch := consistent_hash.NewConsistentHash(consistent_hash.SHA256, 10_000)
	ch.Set(hosts...)

	freq := map[string]int{}
	buf := make([]byte, 16)
	for i := 0; i < 1_000_000; i++ {
		_, err := io.ReadFull(rng, buf)
		require.NoError(t, err)
		host := ch.Get(string(buf))
		freq[host]++
	}
	minFreq := slices.Min(slices.Collect(maps.Values(freq)))
	maxFreq := slices.Max(slices.Collect(maps.Values(freq)))
	skew := float64(maxFreq-minFreq) / float64(minFreq)
	assert.Less(t, skew, 0.1)
}

func TestAgainstReference(t *testing.T) {
	var hosts []string
	for c := 'a'; c <= 'z'; c++ {
		hosts = append(hosts, string([]rune{c}))
	}
	real := consistent_hash.NewConsistentHash(consistent_hash.SHA256, 10000)
	assert.NoError(t, real.Set(hosts...))
	reference := newReferereferenceImpl(consistent_hash.SHA256, 10000, hosts...)

	assert.Equal(t, reference.GetAllReplicas(""), real.GetAllReplicas(""))
	assert.Equal(t, reference.GetAllReplicas("a"), real.GetAllReplicas("a"))
	for keyPrefix := 'a'; keyPrefix <= 'z'; keyPrefix++ {
		for keySuffix := 'a'; keySuffix <= 'z'; keySuffix++ {
			key := string([]rune{keyPrefix, keySuffix})
			realReplicas := real.GetAllReplicas(key)
			referenceReplicas := reference.GetAllReplicas(key)
			assert.Equal(t, referenceReplicas, realReplicas, "Key %q produced different replicas", key)
		}
	}
}

func TestGoldenSet(t *testing.T) {
	ch := consistent_hash.NewConsistentHash(consistent_hash.SHA256, 10000)
	require.NoError(t, ch.Set("a", "b", "c", "d"))
	// NOTE: These values should never change -- if you have to change them it
	// means the consistent hash is broken and returning results in a different
	// order than before.
	assert.Equal(t, []string{"d", "b", "c", "a"}, ch.GetAllReplicas(""))
	assert.Equal(t, []string{"b", "c", "d", "a"}, ch.GetAllReplicas("1"))
	assert.Equal(t, []string{"b", "a", "c", "d"}, ch.GetAllReplicas("2"))
	assert.Equal(t, []string{"a", "c", "d", "b"}, ch.GetAllReplicas("3"))
}

func TestReplicaOverlapOnMembershipChange(t *testing.T) {
	// When the set of items changes, we expect that at least one of the first
	// N replicas overlaps with probability:
	//   P(overlap) = 1 - C(d, N) / C(m, N)
	// where d = |new_size - old_size| and m = max(new_size, old_size).
	// See https://en.wikipedia.org/wiki/Hypergeometric_distribution
	// With N = 1, this reduces to P(overlap) = 1 - d/m, which is just the
	// probability that the new item is not chosen as the replica for a key.
	//
	// Examples:
	//   3->4, N=1: P = 1 - C(1,1)/C(4,1) = 1 - 1/4       = 75%
	//   3->6, N=1: P = 1 - C(3,1)/C(6,1) = 1 - 3/6       = 50%
	//   3->4, N=2: P = 1 - C(1,2)/C(4,2) = 1 - 0/6       = 100%
	//   3->6, N=2: P = 1 - C(3,2)/C(6,2) = 1 - 3/15      = 80%
	//   3->6, N=3: P = 1 - C(3,3)/C(6,3) = 1 - 1/20      = 95%
	//  6->10, N=3: P = 1 - C(4,3)/C(10,3) = 1 - 4/120    = 96.7%

	numKeys := 10_000
	keys := make([]string, numKeys)
	for i := range keys {
		s, err := random.RandomString(16)
		require.NoError(t, err)
		keys[i] = s
	}

	maxItems := 10
	items := make([]string, maxItems)
	for i := range items {
		s, err := random.RandomString(8)
		require.NoError(t, err)
		items[i] = s
	}

	type transition struct {
		from, to int
	}
	transitions := []transition{
		{3, 4}, {3, 5}, {3, 6}, {6, 10},
	}
	// Add inverses.
	for _, tr := range slices.Clone(transitions) {
		transitions = append(transitions, transition{tr.to, tr.from})
	}

	ch := consistent_hash.NewConsistentHash(consistent_hash.SHA256, 10_000)
	for _, tr := range transitions {
		t.Run(fmt.Sprintf("%d_to_%d", tr.from, tr.to), func(t *testing.T) {
			ch.Set(slices.Clone(items[:tr.from])...)

			// Save all replicas for each key (not just first N — we
			// filter to first N in the inner loop over N).
			type replicaList = []string
			baselines := make([]replicaList, numKeys)
			for i, k := range keys {
				baselines[i] = ch.GetAllReplicas(k)
			}

			ch.Set(slices.Clone(items[:tr.to])...)

			newReplicas := make([]replicaList, numKeys)
			for i, k := range keys {
				newReplicas[i] = ch.GetAllReplicas(k)
			}
			m := max(tr.from, tr.to)
			d := m - min(tr.from, tr.to)

			for _, n := range []int{1, 2, 3} {
				t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
					if n > tr.from || n > tr.to {
						t.Skip("N exceeds item count")
					}
					overlap := 0
					for i := range keys {
						if hasOverlap(baselines[i][:n], newReplicas[i][:n]) {
							overlap++
						}
					}
					expected := 1.0
					if d >= n {
						expected = 1.0 - float64(choose(d, n))/float64(choose(m, n))
					}
					actual := float64(overlap) / float64(numKeys)
					assert.InDelta(t, expected, actual, 0.03,
						"expected overlap %.3f, got %.3f (%d/%d keys)",
						expected, actual, overlap, numKeys)
				})
			}
		})
	}
}

func hasOverlap(a, b []string) bool {
	return slices.ContainsFunc(a, func(v string) bool {
		return slices.Contains(b, v)
	})
}

// choose returns the binomial coefficient C(n, k).
func choose(n, k int) int {
	if k > n || k < 0 {
		return 0
	}
	if k == 0 || k == n {
		return 1
	}
	result := 1
	for i := 0; i < k; i++ {
		result = result * (n - i) / (i + 1)
	}
	return result
}

func BenchmarkGetAllReplicas(b *testing.B) {
	for _, test := range []struct {
		Name         string
		HashFunction consistent_hash.HashFunction
		NumVnodes    int
	}{
		{Name: "CRC32/100_vnodes", HashFunction: consistent_hash.CRC32, NumVnodes: 100},
		{Name: "SHA256/10000_vnodes", HashFunction: consistent_hash.SHA256, NumVnodes: 10000},
	} {
		b.Run(test.Name, func(b *testing.B) {
			assert := assert.New(b)
			ch := consistent_hash.NewConsistentHash(test.HashFunction, test.NumVnodes)

			hosts := make([]string, 0)
			for i := 0; i < 50; i++ {
				r, err := random.RandomString(5)
				assert.Nil(err)
				hosts = append(hosts, fmt.Sprintf("%s:%d", r, 1000+i))
			}

			if err := ch.Set(hosts...); err != nil {
				b.Fatal(err)
			}

			keys := make([]string, 0, b.N)
			for i := 0; i < b.N; i++ {
				k, err := random.RandomString(64)
				require.NoError(b, err)
				keys = append(keys, k)
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = ch.GetAllReplicas(keys[i])
			}
		})
	}
}

// referenceImpl is a reference implementation of
// consistent_hash.ConsistentHash, so we can make changes against the real
// implementation and make sure that they still match the reference.
type referenceImpl struct {
	ring    map[int]uint8
	keys    []int
	items   []string
	hashKey consistent_hash.HashFunction
}

func newReferereferenceImpl(hashFunction consistent_hash.HashFunction, vnodes int, items ...string) *referenceImpl {
	sort.Strings(items)
	c := &referenceImpl{
		hashKey: hashFunction,
		keys:    make([]int, len(items)*vnodes),
		ring:    make(map[int]uint8, len(items)*vnodes),
		items:   items,
	}
	for itemIndex, key := range c.items {
		for i := 0; i < vnodes; i++ {
			h := c.hashKey(strconv.Itoa(i) + key)
			c.keys = append(c.keys, h)
			c.ring[h] = uint8(itemIndex)
		}
	}
	sort.Ints(c.keys)
	return c
}

func (c *referenceImpl) lookupVnodes(idx int, fn func(vnodeIndex uint8) bool) {
	done := false
	for offset := 1; offset < len(c.keys) && !done; offset += 1 {
		newIdx := (idx + offset) % len(c.keys)
		done = fn(c.ring[c.keys[newIdx]])
	}
}

func (c *referenceImpl) GetAllReplicas(key string) []string {
	idx := sort.SearchInts(c.keys, c.hashKey(key)) % len(c.keys)
	firstItemIndex := c.ring[c.keys[idx]]
	replicas := []string{c.items[firstItemIndex]}
	c.lookupVnodes(idx, func(vnodeIndex uint8) bool {
		replica := c.items[vnodeIndex]
		if slices.Contains(replicas, replica) {
			return false
		}
		replicas = append(replicas, replica)
		return len(replicas) == len(c.items)
	})
	return replicas
}
