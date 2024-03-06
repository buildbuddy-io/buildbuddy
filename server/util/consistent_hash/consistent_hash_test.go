package consistent_hash_test

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/consistent_hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestNodesetOrderIndependence(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	assert := assert.New(t)
	ch := consistent_hash.NewConsistentHash()

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
	ch := consistent_hash.NewConsistentHash()

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

func TestEvenDistribution(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	for _, useSHA := range []bool{true, false} {
		consistent_hash.UseSHA = useSHA
		fname := "/tmp/results_sha.txt"
		if !useSHA {
			fname = "/tmp/results_crc32.txt"
		}
		f, err := os.Create(fname)
		require.NoError(t, err)
		defer f.Close()

		for trial := 0; trial < 20; trial++ {
			hosts := make([]string, 0)
			for i := 0; i < 4; i++ {
				r, err := random.RandomString(5)
				require.NoError(t, err)
				hosts = append(hosts, fmt.Sprintf("%s:%d", r, 1000+i))
				// hosts = append(hosts, fmt.Sprintf(":%d", i))
			}
			ch := consistent_hash.NewConsistentHash()
			err := ch.Set(hosts...)
			require.NoError(t, err)

			itemCounts := map[string]int{}
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			bufRng := bufio.NewReader(rng)
			buf := make([]byte, 16)
			for i := 0; i < 100_000; i++ {
				_, err := bufRng.Read(buf)
				require.NoError(t, err)
				d, err := digest.Compute(bytes.NewReader(buf), repb.DigestFunction_BLAKE3)
				require.NoError(t, err)

				item := ch.Get(d.String())
				itemCounts[item]++
			}

			minCount := -1
			maxCount := -1
			// log.Infof("Debug JSON:\n%s", ch.DebugJSON())
			// f, err := os.Create("/tmp/consistent-hash.json")
			// require.NoError(t, err)
			// t.Cleanup(func() { f.Close() })
			// _, err = io.WriteString(f, ch.DebugJSON())
			// require.NoError(t, err)

			for item, count := range itemCounts {
				log.Infof("%s: %d", item, count)
				if minCount == -1 {
					minCount = count
					maxCount = count
				} else {
					minCount = min(minCount, count)
					maxCount = max(maxCount, count)
				}
			}
			log.Infof("Range (max-min): %d", maxCount-minCount)
			log.Infof("Skew (max-min)/min: %.3f", float64(maxCount-minCount)/float64(minCount))
			io.WriteString(f, fmt.Sprintf("%f\n", float64(maxCount-minCount)/float64(minCount)))
		}
	}
}

func BenchmarkGetAllReplicas(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	assert := assert.New(b)
	ch := consistent_hash.NewConsistentHash()

	hosts := make([]string, 0)
	for i := 0; i < 10; i++ {
		r, err := random.RandomString(5)
		assert.Nil(err)
		hosts = append(hosts, fmt.Sprintf("%s:%d", r, 1000+i))
	}

	if err := ch.Set(hosts...); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		k, err := random.RandomString(64)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		_ = ch.GetAllReplicas(k)
	}
}
