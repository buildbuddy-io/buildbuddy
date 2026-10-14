// Package pprofutil provides helpers for attaching pprof labels to goroutines
// so that CPU profiles can be sliced by request, customer, and subsystem.
//
// All helpers do nothing unless --monitoring.enable_pprof_labels is set.
package pprofutil

import (
	"context"
	"runtime/pprof"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
)

var (
	enablePprofLabels = flag.Bool("monitoring.enable_pprof_labels", false, "If true, attach pprof labels (gRPC method, group ID, subsystem, etc.) to goroutines so CPU profiles can be sliced by them.")
	// enableTraceTasks gates runtime/trace tasks and regions.
	enableTraceTasks = flag.Bool("monitoring.enable_trace_tasks", false, "If true, annotate requests with runtime/trace tasks so execution traces can be sliced by request.")
)

// Label keys.
const (
	GRPCMethod     = "grpc.method"
	Client         = "client"
	Origin         = "origin"
	OriginMethod   = "origin_method"
	InvocationID   = "invocation_id"
	ActionMnemonic = "action_mnemonic"
	GroupID        = "group_id"
	Subsystem      = "subsystem"
	TaskID         = "task_id"
)

// Enabled returns whether pprof labels are enabled.
func Enabled() bool {
	return *enablePprofLabels
}

// TraceTasksEnabled returns whether runtime/trace tasks are enabled.
func TraceTasksEnabled() bool {
	return *enableTraceTasks
}

// Do runs f with the given key/value label pairs added to the labels carried by
// ctx. Pairs with an empty value are skipped. f is called directly if labels are
// disabled or all values are empty.
func Do(ctx context.Context, f func(ctx context.Context), kv ...string) {
	if !Enabled() {
		f(ctx)
		return
	}
	labels := nonEmptyPairs(kv)
	if len(labels) == 0 {
		f(ctx)
		return
	}
	pprof.Do(ctx, pprof.Labels(labels...), f)
}

// SetGoroutineLabels adds the given key/value label pairs to ctx's labels and
// applies them to the current goroutine. Pairs with an empty value are skipped.
// It returns ctx unchanged if labels are disabled or all values are empty;
// otherwise it returns the labeled ctx.
func SetGoroutineLabels(ctx context.Context, kv ...string) context.Context {
	if !Enabled() {
		return ctx
	}
	labels := nonEmptyPairs(kv)
	if len(labels) == 0 {
		return ctx
	}
	ctx = pprof.WithLabels(ctx, pprof.Labels(labels...))
	pprof.SetGoroutineLabels(ctx)
	return ctx
}

// RestoreGoroutineLabels sets the current goroutine's labels to those carried
// by ctx. Use it to undo SetGoroutineLabels on pooled goroutines.
func RestoreGoroutineLabels(ctx context.Context) {
	if Enabled() {
		pprof.SetGoroutineLabels(ctx)
	}
}

func nonEmptyPairs(kv []string) []string {
	labels := make([]string, 0, len(kv))
	for i := 0; i+1 < len(kv); i += 2 {
		if kv[i+1] != "" {
			labels = append(labels, kv[i], kv[i+1])
		}
	}
	return labels
}

// SetSubsystem replaces the current goroutine's labels (including any
// inherited ones) with subsystem=name. Call it at the top of long-lived
// background goroutines.
func SetSubsystem(name string) {
	if !Enabled() {
		return
	}
	pprof.SetGoroutineLabels(pprof.WithLabels(context.Background(), pprof.Labels(Subsystem, name)))
}

// DoSubsystem runs f with the goroutine's labels replaced by subsystem=name,
// then restores the labels carried by ctx. Go can't read a goroutine's current
// labels, so the restored labels are always those of ctx, which should be the
// ctx the calling goroutine's labels came from.
func DoSubsystem(ctx context.Context, name string, f func()) {
	if !Enabled() {
		f()
		return
	}
	SetSubsystem(name)
	defer pprof.SetGoroutineLabels(ctx)
	f()
}
