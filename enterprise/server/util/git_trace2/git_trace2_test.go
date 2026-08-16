package git_trace2_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/git_trace2"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/stretchr/testify/require"
)

func TestProcessorHandlesEventsFromMultipleWriters(t *testing.T) {
	// Attached processes share the trace2 pipe, each writing whole event
	// lines to the inherited descriptor. Verify that events from separate
	// processes are all delivered, that a malformed line does not prevent
	// subsequent events from being processed, and that Stop drains buffered
	// events.
	var events []*git_trace2.Event
	processor, err := git_trace2.StartProcessor(func(event *git_trace2.Event) {
		events = append(events, event)
	})
	require.NoError(t, err)
	t.Cleanup(processor.Stop)

	for _, script := range []string{
		`printf '%s\n' 'not json' '{"event":"start","sid":"parent"}' >&3`,
		`printf '%s\n' '{"event":"data","sid":"parent/child","category":"progress","key":"total_bytes","value":"123"}' >&3`,
		`printf '%s\n' '{"event":"data_json","sid":"parent/child2","key":"argv","value":["git","fetch"]}' >&3`,
	} {
		cmd := exec.CommandContext(t.Context(), "sh", "-c", script)
		require.NoError(t, processor.Attach(cmd))
		require.NoError(t, cmd.Run())
	}

	// Attach fails when the trace2 target descriptor would exceed git's
	// single-digit limit.
	overLimit := exec.CommandContext(t.Context(), "sh", "-c", "true")
	overLimit.ExtraFiles = make([]*os.File, 7)
	require.Error(t, processor.Attach(overLimit))

	// Stop is documented to be safe to call more than once; the cleanup hook
	// registered above exercises that by calling Stop again after the test
	// body.
	processor.Stop()

	// Attach fails once the processor is stopped.
	require.Error(t, processor.Attach(exec.CommandContext(t.Context(), "sh", "-c", "true")))

	// Stop waits for the processing goroutine, so events is safe to read
	// afterwards. The data_json event's non-string value should not prevent
	// the rest of the event from being delivered.
	var sessionIDs, stringValues []string
	for _, event := range events {
		sessionIDs = append(sessionIDs, event.SessionID)
		stringValues = append(stringValues, event.StringValue())
	}
	require.Equal(t, []string{"parent", "parent/child", "parent/child2"}, sessionIDs)
	require.Equal(t, []string{"", "123", ""}, stringValues)
}

func TestProcessorReceivesSubmoduleFetchEvents(t *testing.T) {
	// Create a submodule repo and a superproject that references it.
	// protocol.file.allow=always is needed because Git disallows file-based
	// submodule URLs by default.
	subRepoPath, _ := testgit.MakeTempRepo(t, map[string]string{"sub.txt": "A"})
	superRepoPath, _ := testgit.MakeTempRepo(t, map[string]string{"super.txt": "A"})
	testshell.Run(t, superRepoPath, `
		git -c protocol.file.allow=always submodule add `+subRepoPath+` sub
		git commit -m "Add submodule"
	`)

	// Clone the superproject with the submodule populated, so that a fetch
	// with --recurse-submodules=yes spawns a child Git process for the
	// submodule.
	workDir := testfs.MakeTempDir(t)
	testshell.Run(t, workDir, `git -c protocol.file.allow=always clone --recurse-submodules `+superRepoPath+` work`)

	// Add a new commit to the submodule remote so the submodule fetch has
	// objects to transfer.
	testgit.CommitFiles(t, subRepoPath, map[string]string{"sub.txt": "B"})

	var sessionIDs []string
	processor, err := git_trace2.StartProcessor(func(event *git_trace2.Event) {
		sessionIDs = append(sessionIDs, event.SessionID)
	})
	require.NoError(t, err)
	t.Cleanup(processor.Stop)

	// Run the fetch attached to the processor, so the whole Git process tree
	// streams Trace2 events to the shared pipe.
	cmd := exec.CommandContext(t.Context(), "git", "-c", "protocol.file.allow=always", "fetch", "--recurse-submodules=yes")
	cmd.Dir = filepath.Join(workDir, "work")
	require.NoError(t, processor.Attach(cmd))
	b, err := cmd.CombinedOutput()
	require.NoError(t, err, "git fetch: %s", string(b))
	processor.Stop()

	// Session IDs are slash-separated ancestry chains. Expect an event from
	// the top-level fetch, and an event from a grandchild process: the
	// submodule fetch is a child of the top-level fetch, and the processes it
	// spawns to transfer submodule objects are grandchildren. Without
	// submodule recursion the process tree is only two levels deep.
	var sawTopLevel, sawGrandchild bool
	for _, sid := range sessionIDs {
		depth := strings.Count(sid, "/")
		sawTopLevel = sawTopLevel || depth == 0
		sawGrandchild = sawGrandchild || depth >= 2
	}
	require.True(t, sawTopLevel, "expected events from the top-level fetch; got session IDs: %v", sessionIDs)
	require.True(t, sawGrandchild, "expected events from submodule fetch descendants; got session IDs: %v", sessionIDs)
}
