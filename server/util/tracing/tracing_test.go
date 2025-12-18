package tracing_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/stretchr/testify/require"
)

func init() {
	*log.LogLevel = "error"
	*log.IncludeShortFileName = true
	log.Configure()
}

func setupBench(b *testing.B) {
	flags.Set(b, "app.trace_fraction", 0.01)
	require.NoError(b, tracing.ConfigureWithNoopExporter(testenv.GetTestEnv(b)))
}

func BenchmarkStartSpan(b *testing.B) {
	setupBench(b)
	for b.Loop() {
		tracing.StartSpan(context.Background())
	}
}

func BenchmarkStartSpanParallel(b *testing.B) {
	setupBench(b)
	b.SetParallelism(10)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tracing.StartSpan(context.Background())
		}
	})
}

func BenchmarkStartNamedSpan(b *testing.B) {
	setupBench(b)
	for b.Loop() {
		tracing.StartNamedSpan(context.Background(), "BenchmarkStartNamedSpan")
	}
}

func BenchmarkStartNamedSpanParallel(b *testing.B) {
	setupBench(b)
	b.SetParallelism(10)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tracing.StartNamedSpan(context.Background(), "BenchmarkStartNamedSpanParallel")
		}
	})
}
