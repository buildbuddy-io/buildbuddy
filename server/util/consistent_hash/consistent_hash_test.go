package consistent_hash_test

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/consistent_hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
)

// Number of replicas serving as a reasonable default for testing purposes.
const numReplicas = 100

func TestNodesetOrderIndependence(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	assert := assert.New(t)
	ch := consistent_hash.NewConsistentHash(consistent_hash.CRC32, numReplicas)

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
	rand.Seed(time.Now().UnixNano())
	assert := assert.New(t)
	ch := consistent_hash.NewConsistentHash(consistent_hash.CRC32, numReplicas)

	hosts := make([]string, 0)
	for i := 0; i < 10; i++ {
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
		assert.Equal(10, len(replicas))
	}
}

func TestEvenLoadDistribution(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

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
	minFreq := slices.Min(maps.Values(freq))
	maxFreq := slices.Max(maps.Values(freq))
	skew := float64(maxFreq-minFreq) / float64(minFreq)
	assert.Less(t, skew, 0.1)
}

func BenchmarkGetAllReplicas(b *testing.B) {
	for _, test := range []struct {
		Name         string
		HashFunction consistent_hash.HashFunction
		NumReplicas  int
	}{
		{Name: "CRC32/100_Replicas", HashFunction: consistent_hash.CRC32, NumReplicas: 100},
		{Name: "SHA256/10000_Replicas", HashFunction: consistent_hash.SHA256, NumReplicas: 10000},
	} {
		b.Run(test.Name, func(b *testing.B) {
			rand.Seed(time.Now().UnixNano())
			assert := assert.New(b)
			ch := consistent_hash.NewConsistentHash(test.HashFunction, test.NumReplicas)

			hosts := make([]string, 0)
			for i := 0; i < 10; i++ {
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
