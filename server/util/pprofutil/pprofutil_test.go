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

func TestSetSharedSubsystemClearsInheritedLabels(t *testing.T) {
	flags.Set(t, "monitoring.enable_pprof_labels", true)
	reached, release := make(chan struct{}), make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		pprof.Do(context.Background(), pprof.Labels(pprofutil.InvocationID, "X"), func(ctx context.Context) {
			pprofutil.SetSharedSubsystem("s")
			waitHere(reached, release)
		})
	}()

	<-reached
	sets := testpprof.GoroutineLabels(t, "_test.waitHere")
	release <- struct{}{}
	<-done

	require.Equal(t, []map[string]string{{pprofutil.SharedSubsystem: "s"}}, sets)
}

func TestSetSharedSubsystemDisabled(t *testing.T) {
	reached, release := make(chan struct{}), make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		pprofutil.SetSharedSubsystem("s")
		waitHere(reached, release)
	}()

	<-reached
	sets := testpprof.GoroutineLabels(t, "_test.waitHere")
	release <- struct{}{}
	<-done

	require.Equal(t, []map[string]string{{}}, sets)
}

func TestDoSkipsEmptyValues(t *testing.T) {
	flags.Set(t, "monitoring.enable_pprof_labels", true)
	pprofutil.Do(context.Background(), func(ctx context.Context) {
		v, ok := pprof.Label(ctx, pprofutil.GroupID)
		require.True(t, ok)
		require.Equal(t, "G", v)
		_, ok = pprof.Label(ctx, pprofutil.InvocationID)
		require.False(t, ok)
	}, pprofutil.GroupID, "G", pprofutil.InvocationID, "")
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
	var group, inv string
	var groupOK, invOK bool
	go func() {
		defer close(done)
		base := pprof.WithLabels(context.Background(), pprof.Labels(pprofutil.GRPCMethod, "m"))
		pprof.SetGoroutineLabels(base)
		ctx := pprofutil.SetGoroutineLabels(base, pprofutil.GroupID, "G", pprofutil.InvocationID, "")
		group, groupOK = pprof.Label(ctx, pprofutil.GroupID)
		inv, invOK = pprof.Label(ctx, pprofutil.InvocationID)
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

	require.True(t, groupOK)
	require.Equal(t, "G", group)
	require.False(t, invOK, "empty invocation_id label %q should be skipped", inv)
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
