package consistent_hash_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/consistent_hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/stretchr/testify/assert"
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
