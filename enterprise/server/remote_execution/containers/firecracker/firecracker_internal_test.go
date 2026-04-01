package firecracker

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/copy_on_write"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaploader"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type fakeSnapshotLoader struct {
	localLastSaved  map[string]time.Time
	remoteLastSaved map[string]time.Time
}

func (f *fakeSnapshotLoader) CacheSnapshot(ctx context.Context, key *fcpb.SnapshotKey, opts *snaploader.CacheSnapshotOptions) error {
	return nil
}

func (f *fakeSnapshotLoader) LocalSnapshotLastSavedTime(ctx context.Context, key *fcpb.SnapshotKey, supportsRemoteChunks bool) (time.Time, error) {
	if t, ok := f.localLastSaved[snapshotKeyString(key)]; ok {
		return t, nil
	}
	return time.Time{}, status.NotFoundError("local snapshot not found")
}

func (f *fakeSnapshotLoader) RemoteSnapshotLastSavedTime(ctx context.Context, key *fcpb.SnapshotKey) (time.Time, error) {
	if t, ok := f.remoteLastSaved[snapshotKeyString(key)]; ok {
		return t, nil
	}
	return time.Time{}, status.NotFoundError("remote snapshot not found")
}

func (f *fakeSnapshotLoader) GetSnapshot(ctx context.Context, keys *fcpb.SnapshotKeySet, opts *snaploader.GetSnapshotOptions) (*snaploader.Snapshot, error) {
	snapshots := f.localLastSaved
	if opts.SupportsRemoteManifest {
		snapshots = f.remoteLastSaved
	}
	if _, ok := snapshots[snapshotKeyString(keys.GetBranchKey())]; ok {
		return nil, nil
	}
	return nil, status.NotFoundError("snapshot not found")
}

func (f *fakeSnapshotLoader) UnpackSnapshot(ctx context.Context, snapshot *snaploader.Snapshot, outputDirectory string) (*snaploader.UnpackedSnapshot, error) {
	return &snaploader.UnpackedSnapshot{ChunkedFiles: map[string]*copy_on_write.COWStore{}}, nil
}

func TestShouldSaveRemoteSnapshot_DefaultBranchRateLimited(t *testing.T) {
	flags.Set(t, "executor.enable_remote_snapshot_sharing", true)
	flags.Set(t, "executor.shared_snapshot_refresh_interval", time.Hour)

	ctx := context.Background()
	mainKey := testSnapshotKey("main")

	tests := []struct {
		name      string
		policy    string
		lastSaved time.Time
		expected  bool
	}{
		{
			name:      "fresh default snapshot",
			policy:    platform.OnlySaveNonDefaultSnapshotIfNoneAvailable,
			lastSaved: time.Now().Add(-30 * time.Minute),
			expected:  false,
		},
		{
			name:      "stale default snapshot",
			policy:    platform.OnlySaveNonDefaultSnapshotIfNoneAvailable,
			lastSaved: time.Now().Add(-2 * time.Hour),
			expected:  true,
		},
		{
			name:      "always policy bypasses rate limit",
			policy:    platform.AlwaysSaveSnapshot,
			lastSaved: time.Now().Add(-30 * time.Minute),
			expected:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := testContainer(
				testTask("main", "main", tc.policy),
				mainKey,
				mainKey,
				nil,
				&fakeSnapshotLoader{
					remoteLastSaved: map[string]time.Time{snapshotKeyString(mainKey): tc.lastSaved},
				},
			)
			assert.Equal(t, tc.expected, c.shouldSaveRemoteSnapshot(ctx))
		})
	}
}

func TestShouldSaveLocalSnapshot_DefaultBranchRateLimited(t *testing.T) {
	flags.Set(t, "executor.enable_local_snapshot_sharing", true)
	flags.Set(t, "executor.enable_remote_snapshot_sharing", true)
	flags.Set(t, "executor.shared_snapshot_refresh_interval", time.Hour)

	ctx := context.Background()
	mainKey := testSnapshotKey("main")

	tests := []struct {
		name      string
		policy    string
		lastSaved time.Time
		expected  bool
	}{
		{
			name:      "fresh default snapshot",
			policy:    platform.OnlySaveFirstNonDefaultSnapshot,
			lastSaved: time.Now().Add(-30 * time.Minute),
			expected:  false,
		},
		{
			name:      "stale default snapshot",
			policy:    platform.OnlySaveFirstNonDefaultSnapshot,
			lastSaved: time.Now().Add(-2 * time.Hour),
			expected:  true,
		},
		{
			name:      "always policy bypasses rate limit",
			policy:    platform.AlwaysSaveSnapshot,
			lastSaved: time.Now().Add(-30 * time.Minute),
			expected:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := testContainer(
				testTask("main", "main", tc.policy),
				mainKey,
				mainKey,
				nil,
				&fakeSnapshotLoader{
					localLastSaved: map[string]time.Time{snapshotKeyString(mainKey): tc.lastSaved},
				},
			)
			assert.Equal(t, tc.expected, c.shouldSaveLocalSnapshot(ctx))
		})
	}
}

func TestShouldSaveSnapshots_MergeQueueRefreshesWriteKey(t *testing.T) {
	flags.Set(t, "executor.enable_local_snapshot_sharing", true)
	flags.Set(t, "executor.enable_remote_snapshot_sharing", true)
	flags.Set(t, "executor.shared_snapshot_refresh_interval", time.Hour)

	ctx := context.Background()
	mergeQueueKey := testSnapshotKey("gh-readonly-queue/main/pr-1")
	mainKey := testSnapshotKey("main")
	fresh := time.Now().Add(-30 * time.Minute)

	c := testContainer(
		testTask("gh-readonly-queue/main/pr-1", "main", platform.OnlySaveFirstNonDefaultSnapshot),
		mergeQueueKey,
		mainKey,
		[]*fcpb.SnapshotKey{mainKey},
		&fakeSnapshotLoader{
			localLastSaved:  map[string]time.Time{snapshotKeyString(mainKey): fresh},
			remoteLastSaved: map[string]time.Time{snapshotKeyString(mainKey): fresh},
		},
	)

	assert.False(t, c.shouldSaveLocalSnapshot(ctx))
	assert.False(t, c.shouldSaveRemoteSnapshot(ctx))
}

func TestShouldSaveSnapshots_NonDefaultBranchBehaviorUnchanged(t *testing.T) {
	flags.Set(t, "executor.enable_local_snapshot_sharing", true)
	flags.Set(t, "executor.enable_remote_snapshot_sharing", true)
	flags.Set(t, "executor.shared_snapshot_refresh_interval", time.Hour)

	ctx := context.Background()
	prKey := testSnapshotKey("feature")
	mainKey := testSnapshotKey("main")
	fresh := time.Now().Add(-30 * time.Minute)

	c := testContainer(
		testTask("feature", "main", platform.OnlySaveFirstNonDefaultSnapshot),
		prKey,
		prKey,
		[]*fcpb.SnapshotKey{mainKey},
		&fakeSnapshotLoader{
			localLastSaved:  map[string]time.Time{snapshotKeyString(prKey): fresh},
			remoteLastSaved: map[string]time.Time{snapshotKeyString(prKey): fresh},
		},
	)

	assert.False(t, c.shouldSaveLocalSnapshot(ctx))
	assert.False(t, c.shouldSaveRemoteSnapshot(ctx))
}

func testContainer(task *repb.ExecutionTask, branchKey, writeKey *fcpb.SnapshotKey, fallbackKeys []*fcpb.SnapshotKey, loader snaploader.Loader) *FirecrackerContainer {
	return &FirecrackerContainer{
		task:                    task,
		loader:                  loader,
		recyclingEnabled:        true,
		supportsRemoteSnapshots: true,
		snapshotKeySet: &fcpb.SnapshotKeySet{
			BranchKey:    branchKey,
			WriteKey:     writeKey,
			FallbackKeys: fallbackKeys,
		},
	}
}

func testTask(branch, defaultBranch, savePolicy string) *repb.ExecutionTask {
	envVars := []*repb.Command_EnvironmentVariable{
		{Name: "GIT_BRANCH", Value: branch},
	}
	if defaultBranch != "" {
		envVars = append(envVars, &repb.Command_EnvironmentVariable{Name: "GIT_REPO_DEFAULT_BRANCH", Value: defaultBranch})
	}
	return &repb.ExecutionTask{
		Command: &repb.Command{
			Arguments: []string{"./buildbuddy_ci_runner"},
			Platform: &repb.Platform{Properties: []*repb.Platform_Property{
				{Name: "recycle-runner", Value: "true"},
				{Name: platform.SnapshotSavePolicyPropertyName, Value: savePolicy},
			}},
			EnvironmentVariables: envVars,
		},
	}
}

func testSnapshotKey(ref string) *fcpb.SnapshotKey {
	return &fcpb.SnapshotKey{
		InstanceName:      "instance",
		PlatformHash:      "platform",
		ConfigurationHash: "config",
		Ref:               ref,
		VersionId:         "version",
	}
}

func snapshotKeyString(key *fcpb.SnapshotKey) string {
	return key.GetInstanceName() + "|" + key.GetPlatformHash() + "|" + key.GetConfigurationHash() + "|" + key.GetRef() + "|" + key.GetVersionId()
}
