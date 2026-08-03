// Package gitstorage manages local bare repository mirrors.
package gitstorage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/gitmirror/gitremote"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/jonboulle/clockwork"
)

var (
	// fetchRefSpecs includes provider-specific refs so exact commits reachable
	// only from refs such as GitHub's refs/pull/* are available locally.
	fetchRefSpecs = []string{
		"+refs/*:refs/*",
	}
)

const (
	// Tracks when each repo was last used.
	lastUsedFileRelpath    = "info/buildbuddy/last-used"
	trashDirName           = ".trash"
	retentionSweepInterval = time.Minute
)

// ID identifies a stored repository by the SHA-256 hash of its normalized URL.
type ID string

// IDForRepo computes the persistent storage ID for repo.
func IDForRepo(repo *gitremote.Repo) ID {
	digest := sha256.Sum256([]byte(repo.String()))
	return ID(fmt.Sprintf("%x", digest))
}

// LabelForRepo returns the readable filesystem label for repo.
func LabelForRepo(repo *gitremote.Repo) string {
	u := repo.URL()
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._-", r) {
			return r
		}
		return '_'
	}, u.Scheme+"_"+u.Host+u.Path)
}

// Storage manages bare repository mirrors under a root directory.
type Storage struct {
	rootDir         string
	clock           clockwork.Clock
	retentionPeriod time.Duration
	stop            chan struct{}
	stopOnce        sync.Once
	backgroundTasks sync.WaitGroup

	mu sync.Mutex
	// TODO: LRU eviction by size, with pruning before we evict entirely. When
	// evicting, move repositories to a trash directory atomically so active
	// requests can finish before deletion.
	repositories map[ID]*repositoryEntry
}

type repositoryEntry struct {
	repository     *Repository
	activeRequests int
	lastUsed       time.Time
}

// New initializes storage at rootDir, synchronously loads existing repository
// mirrors, and periodically deletes mirrors older than retentionPeriod. A zero
// retention period disables deletion.
func New(rootDir string, clock clockwork.Clock, retentionPeriod time.Duration) (*Storage, error) {
	if retentionPeriod < 0 {
		return nil, errors.New("git mirror retention period cannot be negative")
	}
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, fmt.Errorf("create git storage root directory %q: %w", rootDir, err)
	}
	storage := &Storage{
		rootDir:         rootDir,
		clock:           clock,
		retentionPeriod: retentionPeriod,
		stop:            make(chan struct{}),
		repositories:    make(map[ID]*repositoryEntry),
	}
	// Repositories are renamed here before deletion so a crash cannot leave a
	// partially deleted repository at its deterministic path. Anything already
	// in trash is unreachable and safe to finish deleting during startup.
	if err := os.RemoveAll(storage.trashDir()); err != nil {
		return nil, fmt.Errorf("empty git storage trash directory: %w", err)
	}
	if err := os.MkdirAll(storage.trashDir(), 0755); err != nil {
		return nil, fmt.Errorf("create git storage trash directory: %w", err)
	}
	if err := storage.load(); err != nil {
		return nil, err
	}
	if storage.retentionPeriod > 0 {
		if err := storage.EvictOnce(); err != nil {
			log.Errorf("Could not delete expired git mirrors: %s", err)
		}
		storage.backgroundTasks.Go(storage.runRetention)
	}
	return storage, nil
}

func (s *Storage) load() error {
	err := filepath.WalkDir(s.rootDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == s.trashDir() {
			return fs.SkipDir
		}
		if !entry.IsDir() || !strings.HasSuffix(entry.Name(), ".git") {
			return nil
		}
		if err := s.loadRepository(path); err != nil {
			return err
		}
		return fs.SkipDir
	})
	if err != nil {
		return fmt.Errorf("scan mirrored repositories in %q: %w", s.rootDir, err)
	}
	if len(s.repositories) > 0 {
		log.Infof("Scanned %d existing git mirror(s) in %q", len(s.repositories), s.rootDir)
	}
	return nil
}

func removeInvalidRepository(path, reason string) error {
	log.Warningf("Removing invalid repo dir %q: %s", path, reason)
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("remove invalid repo dir %q: %w", path, err)
	}
	return nil
}

func (s *Storage) loadRepository(path string) error {
	if _, err := os.Stat(filepath.Join(path, "HEAD")); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return removeInvalidRepository(path, "missing HEAD")
		}
		return fmt.Errorf("stat HEAD for %q: %w", path, err)
	}
	bareOutput, err := exec.Command("git", "-C", path, "rev-parse", "--is-bare-repository").Output()
	if err != nil || strings.TrimSpace(string(bareOutput)) != "true" {
		return removeInvalidRepository(path, "not a bare Git repository")
	}
	// The origin remote in Git config is the source of truth for the upstream
	// repository URL.
	originOutput, err := exec.Command("git", "-C", path, "remote", "get-url", "origin").Output()
	if err != nil {
		return removeInvalidRepository(path, "missing origin remote")
	}
	upstreamRepo, err := gitremote.RestoreRepo(strings.TrimSpace(string(originOutput)))
	if err != nil {
		return removeInvalidRepository(path, "invalid origin URL: "+err.Error())
	}
	id := IDForRepo(upstreamRepo)
	storedRepo := &Repository{
		rootDir: s.rootDir,
		repo:    upstreamRepo,
	}
	// The origin URL must reproduce the ID and label encoded in the directory
	// path. Discard the mirror if this invariant does not hold.
	if expectedPath := storedRepo.Path(); expectedPath != path {
		return removeInvalidRepository(path, fmt.Sprintf("expected path %q", expectedPath))
	}
	allowAnySHAOutput, err := exec.Command(
		"git", "-C", path, "config", "--bool", "--get", "uploadpack.allowAnySHA1InWant",
	).Output()
	if err != nil || strings.TrimSpace(string(allowAnySHAOutput)) != "true" {
		return removeInvalidRepository(path, "missing upload-pack configuration")
	}
	var lastUsed time.Time
	lastUsedInfo, err := os.Stat(storedRepo.lastUsedPath())
	if err == nil {
		lastUsed = lastUsedInfo.ModTime()
	} else if errors.Is(err, fs.ErrNotExist) {
		// Repositories created before last-use tracking was added fall back to
		// their directory modification time for their first retention decision.
		repositoryInfo, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("stat repository directory %q: %w", path, err)
		}
		lastUsed = repositoryInfo.ModTime()
	} else {
		return fmt.Errorf("stat repository last-used marker %q: %w", storedRepo.lastUsedPath(), err)
	}
	s.repositories[id] = &repositoryEntry{
		repository: storedRepo,
		lastUsed:   lastUsed,
	}
	return nil
}

// RootDir returns the directory containing all repository mirrors.
func (s *Storage) RootDir() string {
	return s.rootDir
}

// Close stops the storage's background work.
func (s *Storage) Close() error {
	s.stopOnce.Do(func() {
		close(s.stop)
		s.backgroundTasks.Wait()
	})
	return nil
}

func (s *Storage) runRetention() {
	ticker := s.clock.NewTicker(retentionSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.Chan():
			if err := s.EvictOnce(); err != nil {
				log.Errorf("Could not delete expired git mirrors: %s", err)
			}
		case <-s.stop:
			return
		}
	}
}

// Acquire returns the mirror for repo and prevents it from being evicted until
// the returned release function is called. The release function is idempotent.
func (s *Storage) Acquire(repo *gitremote.Repo) (*Repository, func()) {
	s.mu.Lock()
	id := IDForRepo(repo)
	entry, ok := s.repositories[id]
	if !ok {
		entry = &repositoryEntry{
			repository: &Repository{
				rootDir: s.rootDir,
				repo:    repo,
			},
			lastUsed: s.clock.Now(),
		}
		s.repositories[id] = entry
	}
	entry.activeRequests++
	s.mu.Unlock()

	release := sync.OnceFunc(func() {
		lastUsed := s.clock.Now()
		if err := entry.repository.recordLastUsed(lastUsed); err != nil {
			log.Warningf("Could not record last use of git mirror %q: %s", entry.repository.Path(), err)
		}
		s.mu.Lock()
		entry.lastUsed = lastUsed
		entry.activeRequests--
		s.mu.Unlock()
	})
	return entry.repository, release
}

// EvictOnce deletes repositories whose last use exceeds the configured
// retention period. Repositories used by active requests are retained.
func (s *Storage) EvictOnce() error {
	cutoff := s.clock.Now().Add(-s.retentionPeriod)
	trashDir := filepath.Join(s.rootDir, trashDirName)
	var evictionErrors []error

	s.mu.Lock()
	for id, entry := range s.repositories {
		if entry.activeRequests > 0 || entry.lastUsed.After(cutoff) {
			continue
		}
		randSuffix := fmt.Sprintf("-%d", rand.IntN(1e18))
		trashedRepositoryPath := filepath.Join(trashDir, filepath.Base(entry.repository.Path())+randSuffix)
		if err := os.Rename(entry.repository.Path(), trashedRepositoryPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			evictionErrors = append(evictionErrors, fmt.Errorf("move git mirror %q to trash: %w", entry.repository.Path(), err))
			continue
		}
		delete(s.repositories, id)
	}
	s.mu.Unlock()

	trashEntries, err := os.ReadDir(trashDir)
	if err != nil {
		evictionErrors = append(evictionErrors, fmt.Errorf("read git storage trash directory: %w", err))
	} else {
		for _, entry := range trashEntries {
			path := filepath.Join(trashDir, entry.Name())
			if err := os.RemoveAll(path); err != nil {
				evictionErrors = append(evictionErrors, fmt.Errorf("remove trashed git mirror %q: %w", path, err))
			}
		}
	}
	return errors.Join(evictionErrors...)
}

func (s *Storage) trashDir() string {
	return filepath.Join(s.rootDir, trashDirName)
}

// Repository is a local bare mirror of a resolved upstream repository.
type Repository struct {
	rootDir string
	repo    *gitremote.Repo

	// Serializes fetches so a client can observe a commit that was pushed
	// immediately before its mirrored fetch.
	mu                               sync.Mutex
	nextFetchSequence                atomic.Uint64
	lastSuccessfulFetchStartSequence uint64
}

// Path returns the repository mirror's deterministic filesystem path.
func (r *Repository) Path() string {
	// Keep a path-safe upstream label alongside the hash so operators can
	// identify mirrors using ordinary filesystem tools.
	id := IDForRepo(r.repo)
	return filepath.Join(r.rootDir, string(id[:2]), string(id)+"_"+LabelForRepo(r.repo)+".git")
}

func (r *Repository) lastUsedPath() string {
	return filepath.Join(r.Path(), lastUsedFileRelpath)
}

func (r *Repository) recordLastUsed(lastUsed time.Time) error {
	if _, err := os.Stat(r.Path()); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("stat repository: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(r.lastUsedPath()), 0755); err != nil {
		return fmt.Errorf("create last-used marker directory: %w", err)
	}
	marker, err := os.OpenFile(r.lastUsedPath(), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open last-used marker: %w", err)
	}
	if err := marker.Close(); err != nil {
		return fmt.Errorf("close last-used marker: %w", err)
	}
	if err := os.Chtimes(r.lastUsedPath(), lastUsed, lastUsed); err != nil {
		return fmt.Errorf("update last-used marker: %w", err)
	}
	return nil
}

// Initialize creates the local bare repository without fetching upstream refs.
func (r *Repository) Initialize(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.initialize(ctx)
}

func (r *Repository) initialize(ctx context.Context) error {
	repoPath := r.Path()
	_, err := os.Stat(filepath.Join(repoPath, "HEAD"))
	if err == nil {
		// Repo is already initialized.
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		// Treat errors other than fs.ErrNotExist as fatal.
		return fmt.Errorf("stat repository HEAD: %w", err)
	}

	if err := os.RemoveAll(repoPath); err != nil {
		return fmt.Errorf("remove incomplete mirror dir: %w", err)
	}
	parentDir := filepath.Dir(repoPath)
	if err := os.MkdirAll(parentDir, 0755); err != nil {
		return fmt.Errorf("create mirror parent dir: %w", err)
	}
	// Initialize in a sibling directory so publishing the completed repository
	// is an atomic rename on the same filesystem.
	tempDir, err := os.MkdirTemp(parentDir, ".gitmirror-init-*")
	if err != nil {
		return fmt.Errorf("create temporary mirror dir: %w", err)
	}
	defer os.RemoveAll(tempDir)
	if err := r.runGitAt(ctx, tempDir, "", "init", "--bare", "--quiet"); err != nil {
		return err
	}
	if err := r.runGitAt(ctx, tempDir, "", "remote", "add", "origin", r.repo.String()); err != nil {
		return err
	}
	// Allow clients to request unadvertised object IDs already present in the
	// mirror. This does not affect which objects the upstream will provide.
	if err := r.runGitAt(ctx, tempDir, "", "config", "uploadpack.allowAnySHA1InWant", "true"); err != nil {
		return err
	}
	// Advertise partial clone filters so clients can request objects such as
	// blobs lazily. The mirror may still cache a complete upstream copy.
	if err := r.runGitAt(ctx, tempDir, "", "config", "uploadpack.allowFilter", "true"); err != nil {
		return err
	}
	if err := os.Rename(tempDir, repoPath); err != nil {
		return fmt.Errorf("publish initialized mirror: %w", err)
	}
	return nil
}

// Fetch refreshes the local mirror from its resolved upstream. Overlapping
// calls are coalesced once a fetch that started after the call completes.
func (r *Repository) Fetch(ctx context.Context, authorization string, gitFlags ...string) error {
	fetchSequence := r.nextFetchSequence.Add(1)

	// Serialize fetches. Concurrent fetches can race while updating the same
	// ref, causing one to fail with "cannot lock ref" or "is at ... but
	// expected ...". They can also overwrite each other's FETCH_HEAD.
	r.mu.Lock()
	defer r.mu.Unlock()

	// Skip fetching if the last successful upstream fetch started after this
	// call to Fetch started. This can improve thundering herd situations, e.g.:
	// - C1: fetch starts at 0ms, initiates request to upstream
	// - C2: fetch starts at 100ms, blocks since C1 is in progress
	// - C3: fetch starts at 200ms, blocks since C1 is in progress
	// - C1: fetch completes at 500ms
	// - C2: fetch unblocks at 500.1ms, still has to fetch, since C1's fetch
	//   may have missed some commits that C2 pushed after C1 started fetching
	//   C1 already started fetching
	// - C2: fetch completes at 1000ms
	// - C3: unblocks at 1000.1ms. Since fetchStart was 200ms, and C2 started
	//   fetching at 500.1ms, C3 can use the result of C2's fetch, and skip
	//   fetching.
	//
	// Note that in this example, C2 spends an extra 400ms blocked behind C1,
	// which is latency entirely introduced by the serialization here.
	// A future optimization could be to parse C2's requested wants and skip its
	// fetch if C1 made the requested objects available locally.
	if r.lastSuccessfulFetchStartSequence >= fetchSequence {
		return nil
	}

	if err := r.initialize(ctx); err != nil {
		return err
	}
	args := append([]string{}, gitFlags...)
	// Record the upstream default branch from the fetch advertisement rather
	// than querying it in a separate request. Note: this requires git >= 2.48
	args = append(args, "-c", "remote.origin.followRemoteHEAD=always")
	for _, refSpec := range fetchRefSpecs {
		args = append(args, "-c", "remote.origin.fetch="+refSpec)
	}

	// Git normally retains local copies of refs that have been deleted
	// upstream. --prune removes those refs during the fetch so the mirror does
	// not keep advertising deleted branches, tags, or provider-specific refs.
	//
	// Note that pruning removes refs, but not the underlying objects. Objects
	// made unreachable by pruning still exist in the object database until
	// later maintenance, so pruning is safe with concurrent upload-pack
	// requests.
	upstreamFetchStartSequence := r.nextFetchSequence.Load()
	args = append(args, "fetch", "--prune", "--quiet", "origin")
	if err := r.runGit(ctx, authorization, args...); err != nil {
		return err
	}
	if err := r.syncDefaultBranch(ctx); err != nil {
		return err
	}

	r.lastSuccessfulFetchStartSequence = upstreamFetchStartSequence
	return nil
}

func (r *Repository) syncDefaultBranch(ctx context.Context) error {
	out, err := r.runGitOutput(ctx, "", "for-each-ref", "--format=%(symref)", "refs/remotes/origin/HEAD")
	if err != nil {
		return err
	}
	remoteHead := strings.TrimSpace(string(out))
	branch, ok := strings.CutPrefix(remoteHead, "refs/remotes/origin/")
	if !ok {
		return nil
	}
	return r.runGit(ctx, "", "symbolic-ref", "HEAD", "refs/heads/"+branch)
}

// runGit passes authorization through environment-backed Git config so the
// credential is not exposed in the process command line.
func (r *Repository) runGit(ctx context.Context, authorization string, args ...string) error {
	_, err := r.runGitOutputAt(ctx, r.Path(), authorization, args...)
	return err
}

func (r *Repository) runGitAt(ctx context.Context, dir, authorization string, args ...string) error {
	_, err := r.runGitOutputAt(ctx, dir, authorization, args...)
	return err
}

func (r *Repository) runGitOutput(ctx context.Context, authorization string, args ...string) ([]byte, error) {
	return r.runGitOutputAt(ctx, r.Path(), authorization, args...)
}

func (r *Repository) runGitOutputAt(ctx context.Context, dir, authorization string, args ...string) ([]byte, error) {
	args = append([]string{
		// Disable async auto-maintenance; we manage maintenance via a separate
		// bounded worker pool with explicit resource accounting.
		"-c", "maintenance.auto=false",
		// Ignore inherited credential helpers. The mirror authenticates only
		// with credentials supplied by the current request.
		// TODO: maybe add a config flag to enable credential helpers? (They
		// may be useful in certain environments.)
		"-c", "credential.helper=",
	}, args...)
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if authorization != "" {
		// To avoid exposing auth credentials in the git command line, set it
		// via env instead. Git supports passing configs as 0-indexed
		// "GIT_CONFIG_{KEY,VALUE}_{index}" env vars, with "GIT_CONFIG_COUNT"
		// specifying the total number of pairs.
		cmd.Env = append(cmd.Env,
			"GIT_CONFIG_COUNT=1",
			"GIT_CONFIG_KEY_0=http.extraHeader",
			"GIT_CONFIG_VALUE_0=Authorization: "+authorization,
		)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w: %q", strings.Join(args, " "), err, out)
	}
	return out, nil
}
