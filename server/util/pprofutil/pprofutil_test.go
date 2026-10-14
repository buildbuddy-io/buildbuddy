package pprofutil_test

import (
	"context"
	"runtime/pprof"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testpprof"
	"github.com/buildbuddy-io/buildbuddy/server/util/pprofutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

//go:noinline
func waitHere(reached, release chan struct{}) {
	reached <- struct{}{}
	<-release
}

func TestSetSubsystemClearsInheritedLabels(t *testing.T) {
	flags.Set(t, "monitoring.enable_pprof_labels", true)
	reached, release := make(chan struct{}), make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		pprof.Do(context.Background(), pprof.Labels(pprofutil.TaskID, "X"), func(ctx context.Context) {
			pprofutil.SetSubsystem("s")
			waitHere(reached, release)
		})
	}()

	<-reached
	sets := testpprof.GoroutineLabels(t, "_test.waitHere")
	release <- struct{}{}
	<-done

	require.Equal(t, []map[string]string{{pprofutil.Subsystem: "s"}}, sets)
}

func TestSetSubsystemDisabled(t *testing.T) {
	reached, release := make(chan struct{}), make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		pprofutil.SetSubsystem("s")
		waitHere(reached, release)
	}()

	<-reached
	sets := testpprof.GoroutineLabels(t, "_test.waitHere")
	release <- struct{}{}
	<-done

	require.Equal(t, []map[string]string{{}}, sets)
}

func TestDoSubsystemRestoresCallerLabels(t *testing.T) {
	flags.Set(t, "monitoring.enable_pprof_labels", true)
	reached, release := make(chan struct{}), make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		pprof.Do(context.Background(), pprof.Labels(pprofutil.TaskID, "X"), func(ctx context.Context) {
			pprofutil.DoSubsystem(ctx, "s", func() {
				waitHere(reached, release)
			})
			waitHere(reached, release)
		})
	}()

	<-reached
	inside := testpprof.GoroutineLabels(t, "_test.waitHere")
	release <- struct{}{}
	<-reached
	after := testpprof.GoroutineLabels(t, "_test.waitHere")
	release <- struct{}{}
	<-done

	require.Equal(t, []map[string]string{{pprofutil.Subsystem: "s"}}, inside)
	require.Equal(t, []map[string]string{{pprofutil.TaskID: "X"}}, after)
}

func TestDoSkipsEmptyValues(t *testing.T) {
	flags.Set(t, "monitoring.enable_pprof_labels", true)
	pprofutil.Do(context.Background(), func(ctx context.Context) {
		v, ok := pprof.Label(ctx, pprofutil.GroupID)
		require.True(t, ok)
		require.Equal(t, "G", v)
		_, ok = pprof.Label(ctx, pprofutil.TaskID)
		require.False(t, ok)
	}, pprofutil.GroupID, "G", pprofutil.TaskID, "")
}

func TestDoDisabled(t *testing.T) {
	pprofutil.Do(context.Background(), func(ctx context.Context) {
		_, ok := pprof.Label(ctx, pprofutil.GroupID)
		require.False(t, ok)
	}, pprofutil.GroupID, "G")
}

func TestSetGoroutineLabelsAndRestore(t *testing.T) {
	flags.Set(t, "monitoring.enable_pprof_labels", true)
	reached, release := make(chan struct{}), make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		base := pprof.WithLabels(context.Background(), pprof.Labels(pprofutil.GRPCMethod, "m"))
		pprof.SetGoroutineLabels(base)
		ctx := pprofutil.SetGoroutineLabels(base, pprofutil.GroupID, "G", pprofutil.TaskID, "")
		g, ok := pprof.Label(ctx, pprofutil.GroupID)
		require.True(t, ok)
		require.Equal(t, "G", g)
		_, ok = pprof.Label(ctx, pprofutil.TaskID)
		require.False(t, ok)
		waitHere(reached, release)
		pprofutil.RestoreGoroutineLabels(base)
		waitHere(reached, release)
	}()

	<-reached
	inside := testpprof.GoroutineLabels(t, "_test.waitHere")
	release <- struct{}{}
	<-reached
	after := testpprof.GoroutineLabels(t, "_test.waitHere")
	release <- struct{}{}
	<-done

	require.Equal(t, []map[string]string{{pprofutil.GRPCMethod: "m", pprofutil.GroupID: "G"}}, inside)
	require.Equal(t, []map[string]string{{pprofutil.GRPCMethod: "m"}}, after)
}

func TestSetGoroutineLabelsEmptyValuesAndDisabled(t *testing.T) {
	ctx := context.Background()
	_, ok := pprof.Label(pprofutil.SetGoroutineLabels(ctx, pprofutil.GroupID, "G"), pprofutil.GroupID)
	require.False(t, ok)

	flags.Set(t, "monitoring.enable_pprof_labels", true)
	require.Equal(t, ctx, pprofutil.SetGoroutineLabels(ctx, pprofutil.GroupID, ""))
}
