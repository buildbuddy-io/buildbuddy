package disk

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
)

const (
	// default timeout for WaitUntilExists
	defaultWaitTimeout = 1 * time.Second

	// default poll interval for WaitUntilExists.
	defaultWaitPollInterval = 1 * time.Millisecond
)

var (
	tmpWriteFileRe = regexp.MustCompile(`\.[0-9a-zA-Z]{10}\.tmp$`)
)

type Partition struct {
	ID                  string `yaml:"id" json:"id" usage:"The ID of the partition."`
	MaxSizeBytes        int64  `yaml:"max_size_bytes" json:"max_size_bytes" usage:"Maximum size of the partition."`
	EncryptionSupported bool   `yaml:"encryption_supported" json:"encryption_supported" usage:"Whether encrypted data can be stored on this partition."`
}

type PartitionMapping struct {
	GroupID     string `yaml:"group_id" json:"group_id" usage:"The Group ID to which this mapping applies."`
	Prefix      string `yaml:"prefix" json:"prefix" usage:"The remote instance name prefix used to select this partition."`
	PartitionID string `yaml:"partition_id" json:"partition_id" usage:"The partition to use if the Group ID and prefix match."`
}

// EnsureDirectoryExists is a synonym for os.MkdirAll(dir, 0755). It returns an
// error if dir exists but isn't a directory.
func EnsureDirectoryExists(dir string) error {
	// This could be inlined, but there many callers in many files.
	return os.MkdirAll(dir, 0755)
}

// RemoveIfExists attempts to remove the given named file or (empty) directory,
// ignoring IsNotExist errors.
func RemoveIfExists(filename string) error {
	err := os.Remove(filename)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func WriteFile(ctx context.Context, fullPath string, data []byte) (int, error) {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer spn.End()
	if err := EnsureDirectoryExists(filepath.Dir(fullPath)); err != nil {
		return 0, err
	}

	randStr, err := random.RandomString(10)
	if err != nil {
		return 0, err
	}

	tmpFileName := fullPath + fmt.Sprintf(".%s.tmp", randStr)
	// We defer a cleanup function that would delete our tempfile here --
	// that way if the write is truncated (say, because it's too big) we
	// still remove the tmp file.
	defer func() {
		if err := RemoveIfExists(tmpFileName); err != nil {
			log.Warningf("Failed to delete %s: %s", tmpFileName, err)
		}
	}()

	if err := os.WriteFile(tmpFileName, data, 0644); err != nil {
		return 0, err
	}
	return len(data), os.Rename(tmpFileName, fullPath)
}

func ReadFile(ctx context.Context, fullPath string) ([]byte, error) {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer spn.End()
	data, err := os.ReadFile(fullPath)
	if os.IsNotExist(err) {
		return nil, status.NotFoundError(err.Error())
	}
	return data, err
}

func DeleteFile(ctx context.Context, fullPath string) error {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer spn.End()
	return os.Remove(fullPath)
}

// ForceRemove attempts to delete a directory using os.RemoveAll. If that fails,
// it will attempt to traverse the directory and update permissions so that the
// directory can be removed, then retry os.RemoveAll. This fallback approach is
// used for performance reasons, since recursive chmod can be slow for very
// large directories.
func ForceRemove(ctx context.Context, path string) error {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer spn.End()

	err := os.RemoveAll(path)
	if err == nil {
		return nil
	}
	log.Debugf("Failed to remove %q: %s. Attempting to force-remove by changing permissions.", path, err)
	err = filepath.WalkDir(path, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			// In order to remove dir entries and make sure we can further
			// recurse into the dir, we need all bits set (RWX).
			return os.Chmod(path, 0777)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return os.RemoveAll(path)
}

func FileExists(ctx context.Context, fullPath string) (bool, error) {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer spn.End()
	_, err := os.Stat(fullPath)
	if err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return false, err
	}
}

type WaitOpts struct {
	// Timeout specifies how long to wait for a file to exist before returning
	// context.DeadlineExceeded. A negative value indicates that the parent
	// context's timeout should be used.
	Timeout time.Duration

	// PollInterval specifies how often to poll the filesystem to check whether
	// the file exists.
	PollInterval time.Duration
}

// WaitUntilExists polls the filesystem for a given path to be created. It
// returns an error if the provided timeout is exceeded or if the context is
// cancelled.
func WaitUntilExists(ctx context.Context, path string, opts WaitOpts) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	if opts.Timeout == 0 {
		opts.Timeout = defaultWaitTimeout
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = defaultWaitPollInterval
	}
	if opts.Timeout >= 0 {
		c, cancel := context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
		ctx = c
	}

	ticker := time.NewTicker(opts.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_, err := os.Stat(path)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return err
			}
			return nil
		}
	}
}

type readCloser struct {
	*io.SectionReader
	io.Closer
}

func FileReader(ctx context.Context, fullPath string, offset, length int64) (io.ReadCloser, error) {
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	if offset == 0 && length == 0 {
		return f, nil
	}
	if length == 0 {
		length = math.MaxInt64
	}
	return &readCloser{io.NewSectionReader(f, offset, length), f}, nil
}

type writeMover struct {
	*os.File
	tmpFileIsClosed bool
	ctx             context.Context
	finalPath       string
}

func (w *writeMover) Write(p []byte) (int, error) {
	_, spn := tracing.StartSpan(w.ctx)
	defer spn.End()
	return w.File.Write(p)
}

func (w *writeMover) Commit() error {
	tmpName := w.File.Name()
	if err := w.File.Close(); err != nil {
		return err
	}
	w.tmpFileIsClosed = true
	return os.Rename(tmpName, w.finalPath)
}

func (w *writeMover) Close() error {
	if !w.tmpFileIsClosed {
		w.File.Close()
	}
	if err := RemoveIfExists(w.File.Name()); err != nil {
		log.Warningf("Failed to delete %s: %s", w.File.Name(), err)
	}
	return nil
}

func FileWriter(ctx context.Context, fullPath string) (interfaces.CommittedWriteCloser, error) {
	if err := EnsureDirectoryExists(filepath.Dir(fullPath)); err != nil {
		return nil, err
	}
	randStr, err := random.RandomString(10)
	if err != nil {
		return nil, err
	}

	tmpFileName := fullPath + fmt.Sprintf(".%s.tmp", randStr)
	f, err := os.OpenFile(tmpFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	wm := &writeMover{
		File:      f,
		ctx:       ctx,
		finalPath: fullPath,
	}
	return wm, nil
}

func IsWriteTempFile(fullPath string) bool {
	return tmpWriteFileRe.MatchString(fullPath)
}

// DirSize returns the size of a directory specified by path, in bytes.
func DirSize(path string) (int64, error) {
	var size int64
	err := filepath.WalkDir(path, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		size += info.Size()
		return nil
	})
	return size, err
}

type DirUsage struct {
	TotalBytes uint64
	UsedBytes  uint64
	FreeBytes  uint64
	AvailBytes uint64
}

// MoveFile attempts to rename the src file to the dest file. If the src and
// dest file are on different filesystems, a copy is performed instead of a
// rename. In the copy case, the file is copied to an intermediate ".tmp" file
// as a sibling of "dest" to ensure atomicity. In both cases, the original
// source file is unlinked.
func MoveFile(src, dest string) error {
	if err := os.Rename(src, dest); err != nil {
		if os.IsNotExist(err) {
			return err
		}
		if err := CopyViaTmpSibling(src, dest); err != nil {
			return err
		}
		// Remove the original file.
		if err := os.Remove(src); err != nil {
			return err
		}
		return nil
	}
	return nil
}

func CopyViaTmpSibling(src, dest string) error {
	randStr, err := random.RandomString(10)
	if err != nil {
		return err
	}
	s, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !s.Mode().IsRegular() {
		return fmt.Errorf("%s: non-regular file", src)
	}
	sf, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sf.Close()
	tmpPath := fmt.Sprintf("%s.%s.tmp", dest, randStr)
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, s.Mode().Perm())
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(f, sf); err != nil {
		return err
	}
	// Move the temp file to its final destination.
	if err := os.Rename(tmpPath, dest); err != nil {
		return err
	}
	return nil
}

type UsageMonitor struct {
	rootDir     string  // The path to watch
	maxFullness float64 // .90 means fail health check with 90 % full
}

func NewUsageMonitor(path string, maxFullness float64) *UsageMonitor {
	return &UsageMonitor{
		rootDir:     path,
		maxFullness: maxFullness,
	}
}

func (s *UsageMonitor) Check(ctx context.Context) error {
	usage, err := GetDirUsage(s.rootDir)
	if err != nil {
		log.Warningf("Error getting usage for path %q: %v", s.rootDir, err)
		return nil
	}
	percentUsed := float64(usage.UsedBytes) / float64(usage.TotalBytes)
	if percentUsed > s.maxFullness {
		return status.FailedPreconditionErrorf("Insufficent free space on %q, %2.2f%% full", s.rootDir, percentUsed*100)
	}
	return nil
}

func (s *UsageMonitor) Statusz(ctx context.Context) string {
	usage, err := GetDirUsage(s.rootDir)
	if err != nil {
		return fmt.Sprintf("Error getting usage for path %q: %v", s.rootDir, err)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "<div>TotalBytes: %d</div>", usage.TotalBytes)
	fmt.Fprintf(&b, "<div>UsedBytes: %d</div>", usage.UsedBytes)
	fmt.Fprintf(&b, "<div>FreeBytes: %d</div>", usage.FreeBytes)
	fmt.Fprintf(&b, "<div>AvailBytes: %d</div>", usage.AvailBytes)
	fmt.Fprintf(&b, "<div>Percent Full: %2.2f%%</div>", float64(usage.UsedBytes)*100/float64(usage.TotalBytes))
	return b.String()
}
