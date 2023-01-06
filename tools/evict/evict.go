package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var (
	listen         = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	monitoringPort = flag.Int("monitoring_port", 0, "The port to listen for monitoring traffic on")
)

type tester struct {
	mu      sync.Mutex
	c       *pebble_cache.PebbleCache
	counter int
	rng     *digest.Generator
	p       *message.Printer
}

func (t *tester) write(ctx context.Context, cacheType rspb.CacheType, age time.Time) {
	d, buf, err := t.rng.RandomDigestBuf(1)
	if err != nil {
		log.Fatalf("could not generate: %s", err)
	}
	rn := digest.NewCacheResourceName(d, "", cacheType)
	err = t.c.SetWithAtime(ctx, rn.ToProto(), buf, age)
	if err != nil {
		log.Fatalf("could not set: %s", err)
	}
	t.mu.Lock()
	t.counter++
	if t.counter != 0 && t.counter%100_000 == 0 {
		log.Infof("set %s entries", t.p.Sprint(t.counter))
	}
	t.mu.Unlock()
}

func (t *tester) work() {
	ctx := context.Background()
	for {
		var ageMillis int64
		if f := rand.Float64(); f < 0.05 {
			ageMillis = rand.Int63n(1000*60*60*24*7) + 1000*60*60*12
		} else {
			ageMillis = rand.Int63n(1000 * 60 * 60 * 12)
		}

		age := time.UnixMilli(ageMillis)
		t.write(ctx, rspb.CacheType_CAS, age)
		for i := 0; i < 4; i++ {
			t.write(ctx, rspb.CacheType_AC, age)
		}
	}
}

func main() {
	flag.Parse()

	hc := healthcheck.NewHealthChecker("evict")
	env := real_environment.NewRealEnv(hc)

	if *monitoringPort > 0 {
		monitoring.StartMonitoringHandler(fmt.Sprintf("%s:%d", *listen, *monitoringPort))
	}

	c, err := pebble_cache.NewPebbleCache(env, &pebble_cache.Options{
		Name:              "pebble",
		RootDirectory:     "/home/vadim/pbcache",
		IsolateByGroupIDs: true,
		MaxSizeBytes:      5_000_000,
	})
	if err != nil {
		log.Fatalf("could not create cache: %s", err)
	}
	if err := c.Start(); err != nil {
		log.Fatalf("could not start cache: %s", err)
	}

	hc.RegisterShutdownFunction(func(ctx context.Context) error {
		return c.Stop()
	})

	rand.Seed(time.Now().UnixMicro())

	log.Infof("Starting test...")

	t := &tester{
		c:   c,
		rng: digest.RandomGenerator(time.Now().UnixMicro()),
		p:   message.NewPrinter(language.English),
	}

	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		go func() {
			t.work()
		}()
	}

	select {}
}
