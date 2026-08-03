package gitstorage_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/gitmirror/gitremote"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/gitmirror/gitstorage"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestRepoIdentityDerivedFromNormalizedURL(t *testing.T) {
	repo, err := gitremote.RestoreRepo("https://github.com:443/buildbuddy-io/buildbuddy")
	require.NoError(t, err)

	// Persistent identity uses the normalized resolved URL and retains the
	// existing on-disk hash and readable label.
	require.Equal(t,
		gitstorage.ID("be69d98478e7756fc41d26544069c5f3c01ace9af3bfd3e5703d138eefb1f53c"),
		gitstorage.IDForRepo(repo))
	require.Equal(t, "https_github.com_443_buildbuddy-io_buildbuddy", gitstorage.LabelForRepo(repo))
}

func TestLabelForRepoPreservesPathStructure(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		repoURL   string
		wantLabel string
	}{
		{
			name:      "dot segment remains visible",
			repoURL:   "https://git.example.com/org/./repo",
			wantLabel: "https_git.example.com_443_org_._repo",
		},
		{
			name:      "empty segment remains visible",
			repoURL:   "https://git.example.com/org//repo",
			wantLabel: "https_git.example.com_443_org__repo",
		},
		{
			name:      "backslash is made filesystem-safe",
			repoURL:   `https://git.example.com/org\repo`,
			wantLabel: "https_git.example.com_443_org_repo",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			repo, err := gitremote.RestoreRepo(testCase.repoURL)
			require.NoError(t, err)
			require.Equal(t, testCase.wantLabel, gitstorage.LabelForRepo(repo))
		})
	}
}

func TestInitializeIsAtomic(t *testing.T) {
	// Create a storage entry without initializing its on-disk bare repository.
	rootDir := testfs.MakeTempDir(t)
	storage, err := gitstorage.New(rootDir, clockwork.NewRealClock(), 0)
	require.NoError(t, err)
	repo, err := gitremote.RestoreRepo("https://github.com:443/example/repo")
	require.NoError(t, err)
	storedRepo, release := storage.Acquire(repo)
	defer release()

	// Cancel initialization before Git can finish configuring the temporary
	// repository. No partially initialized final path should become visible.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.Error(t, storedRepo.Initialize(ctx))
	require.NoDirExists(t, storedRepo.Path())
	entries, err := os.ReadDir(filepath.Dir(storedRepo.Path()))
	require.NoError(t, err)
	require.Empty(t, entries)

	// A later request can initialize the repository successfully because the
	// canceled attempt did not poison its final path.
	require.NoError(t, storedRepo.Initialize(t.Context()))
	require.DirExists(t, storedRepo.Path())
	require.Equal(t, repo.String(), strings.TrimSpace(
		testshell.Run(t, storedRepo.Path(), "git remote get-url origin"),
	))
	require.Equal(t, "true", strings.TrimSpace(testshell.Run(
		t, storedRepo.Path(), "git config --bool --get uploadpack.allowAnySHA1InWant",
	)))
	require.Equal(t, "true", strings.TrimSpace(testshell.Run(
		t, storedRepo.Path(), "git config --bool --get uploadpack.allowFilter",
	)))
}

func TestStorageDiscardsRepositoryWithoutOrigin(t *testing.T) {
	// Reproduce an interrupted initialization that published a bare repository
	// after git init, but before configuring its origin.
	rootDir := testfs.MakeTempDir(t)
	storage, err := gitstorage.New(rootDir, clockwork.NewRealClock(), 0)
	require.NoError(t, err)
	repo, err := gitremote.RestoreRepo("https://github.com:443/example/repo")
	require.NoError(t, err)
	storedRepo, release := storage.Acquire(repo)
	require.NoError(t, os.MkdirAll(storedRepo.Path(), 0755))
	testshell.Run(t, storedRepo.Path(), "git init --bare --quiet")
	release()

	// Loading storage should discard the incomplete repository instead of
	// accepting HEAD as proof that initialization finished.
	reloadedStorage, err := gitstorage.New(rootDir, clockwork.NewRealClock(), 0)
	require.NoError(t, err)
	require.NoDirExists(t, storedRepo.Path())

	// A later request can recreate the repository with every required setting.
	reloadedRepo, releaseReloadedRepo := reloadedStorage.Acquire(repo)
	defer releaseReloadedRepo()
	require.NoError(t, reloadedRepo.Initialize(t.Context()))
	require.DirExists(t, reloadedRepo.Path())
}

func TestStorageDiscardsRepositoryWithoutUploadPackConfig(t *testing.T) {
	// Reproduce an interrupted initialization that configured the origin but
	// stopped before enabling requests for unadvertised object IDs.
	rootDir := testfs.MakeTempDir(t)
	storage, err := gitstorage.New(rootDir, clockwork.NewRealClock(), 0)
	require.NoError(t, err)
	repo, err := gitremote.RestoreRepo("https://github.com:443/example/repo")
	require.NoError(t, err)
	storedRepo, release := storage.Acquire(repo)
	require.NoError(t, os.MkdirAll(storedRepo.Path(), 0755))
	testshell.Run(t, storedRepo.Path(), "git init --bare --quiet")
	testshell.Run(t, storedRepo.Path(), "git remote add origin "+repo.String())
	release()

	// Loading storage should discard the incomplete repository so the next
	// request can initialize it from scratch.
	reloadedStorage, err := gitstorage.New(rootDir, clockwork.NewRealClock(), 0)
	require.NoError(t, err)
	require.NoDirExists(t, storedRepo.Path())
	reloadedRepo, releaseReloadedRepo := reloadedStorage.Acquire(repo)
	defer releaseReloadedRepo()
	require.NoError(t, reloadedRepo.Initialize(t.Context()))
}

func TestStorageEvictsExpiredRepository(t *testing.T) {
	// Initialize and release a repository so its last-used time is persisted.
	clock := clockwork.NewFakeClockAt(time.Date(2026, time.August, 4, 12, 0, 0, 0, time.UTC))
	rootDir := testfs.MakeTempDir(t)
	storage, err := gitstorage.New(rootDir, clock, 30*time.Minute)
	require.NoError(t, err)
	repo, err := gitremote.RestoreRepo("https://github.com:443/example/repo")
	require.NoError(t, err)
	storedRepo, release := storage.Acquire(repo)
	require.NoError(t, storedRepo.Initialize(t.Context()))
	release()
	require.NoError(t, storage.Close())

	// Once the retention period passes, eviction removes the repository and
	// clears the temporary trash directory contents.
	clock.Advance(time.Hour)
	require.NoError(t, storage.EvictOnce())
	require.NoDirExists(t, storedRepo.Path())
	trashEntries, err := os.ReadDir(filepath.Join(rootDir, ".trash"))
	require.NoError(t, err)
	require.Empty(t, trashEntries)

	// A later acquisition can reuse the deterministic path after the old
	// repository was moved to trash.
	recreatedRepo, releaseRecreatedRepo := storage.Acquire(repo)
	defer releaseRecreatedRepo()
	require.NotSame(t, storedRepo, recreatedRepo)
	require.NoError(t, recreatedRepo.Initialize(t.Context()))
	require.DirExists(t, recreatedRepo.Path())
}

func TestStorageRetainsActiveRepository(t *testing.T) {
	// Keep a lease active beyond the retention period while the request uses
	// the repository.
	clock := clockwork.NewFakeClockAt(time.Date(2026, time.August, 4, 12, 0, 0, 0, time.UTC))
	rootDir := testfs.MakeTempDir(t)
	storage, err := gitstorage.New(rootDir, clock, time.Hour)
	require.NoError(t, err)
	repo, err := gitremote.RestoreRepo("https://github.com:443/example/repo")
	require.NoError(t, err)
	storedRepo, release := storage.Acquire(repo)
	require.NoError(t, storedRepo.Initialize(t.Context()))
	require.NoError(t, storage.Close())
	clock.Advance(2 * time.Hour)
	require.NoError(t, storage.EvictOnce())
	require.DirExists(t, storedRepo.Path())

	// Releasing the lease records the end of the request as a recent use. The
	// repository becomes eligible only after another full retention period.
	release()
	require.NoError(t, storage.EvictOnce())
	require.DirExists(t, storedRepo.Path())
	clock.Advance(time.Hour)
	require.NoError(t, storage.EvictOnce())
	require.NoDirExists(t, storedRepo.Path())
}

func TestStorageLoadsPersistedLastUsedTime(t *testing.T) {
	// Record a use, then construct new storage from the root directory to
	// simulate a server restart.
	clock := clockwork.NewFakeClockAt(time.Date(2026, time.August, 4, 12, 0, 0, 0, time.UTC))
	rootDir := testfs.MakeTempDir(t)
	storage, err := gitstorage.New(rootDir, clock, time.Hour)
	require.NoError(t, err)
	repo, err := gitremote.RestoreRepo("https://github.com:443/example/repo")
	require.NoError(t, err)
	storedRepo, release := storage.Acquire(repo)
	require.NoError(t, storedRepo.Initialize(t.Context()))
	release()
	require.FileExists(t, filepath.Join(storedRepo.Path(), "info", "buildbuddy", "last-used"))
	require.NoError(t, storage.Close())

	// Reloaded storage retains the repository until the persisted last-used time
	// crosses the retention cutoff, then evicts it without another acquisition.
	clock.Advance(30 * time.Minute)
	reloadedStorage, err := gitstorage.New(rootDir, clock, time.Hour)
	require.NoError(t, err)
	defer reloadedStorage.Close()
	require.NoError(t, reloadedStorage.EvictOnce())
	require.DirExists(t, storedRepo.Path())
	clock.Advance(31 * time.Minute)
	require.NoError(t, reloadedStorage.EvictOnce())
	require.NoDirExists(t, storedRepo.Path())
}
