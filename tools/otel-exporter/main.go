package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"
)

const (
	honeycombMaxRetention = time.Hour * 24 * 30 * 2
)

var (
	honeycombServer = flag.String("honeycomb_server", "api.honeycomb.io:443", "honeycomb api to send  events to")
	honeycombAPIKey = flag.String("honeycomb_api_key", "", "credentials to honeycomb service")
	bbHttpURL       = flag.String("bb_http_url", "https://app.buildbuddy.io/", "BuildBuddy HTTP URL")
	bbGrpcURL       = flag.String("bb_grpc_url", "grpcs://remote.buildbuddy.io", "BuildBuddy GRPC URL")
	bbOrgID         = flag.String("bb_org_id", "", "BuildBuddy Organization ID")
	bbAPIKey        = flag.String("bb_api_key", "", "BuildBuddy API Key, best to use a read-only key")

	serviceSleepDuration = flag.Int("sleep_duration_minutes", 15, "how long to sleep between runs")
	cachePath            = flag.String("cache_path", "/tmp/bb-exporter-cache.json", "JSON file that hold all the cache files")
	serviceName          = "bb-exporter"
	serviceVer           = "v0.0.1.dev"
)

func main() {
	log.SetPrefix(fmt.Sprintf("%s: ", serviceName))

	flag.Parse()
	if *bbAPIKey == "" {
		log.Fatal("missing bb_api_key")
	}

	ctx := context.Background()
	tracer, shutdown := initOtel(ctx, serviceName)
	defer shutdown()

	lastFinishedAt := time.Now().Add(-1 * time.Hour * 24 * 7)

	c := NewCache(*cachePath)
	defer c.fileStore.Close()

	NewDaemon(
		*bbGrpcURL,
		c,
		lastFinishedAt,
		tracer,
		*serviceSleepDuration,
	).Exec(ctx)
}
