package disk

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"golang.org/x/sys/unix"
)

const (
	// default timeout for WaitUntilExists
	defaultWaitTimeout = 1 * time.Second

	// default poll interval for WaitUntilExists.
	defaultWaitPollInterval = 1 * time.Millisecond
)

type Partition struct {
	ID           string `yaml:"id" json:"id" usage:"The ID of the partition."`
	MaxSizeBytes int64  `yaml:"max_size_bytes" json:"max_size_bytes" usage:"Maximum size of the partition."`
}

type PartitionMapping struct {
	GroupID     string `yaml:"group_id" json:"group_id" usage:"The Group ID to which this mapping applies."`
	Prefix      string `yaml:"prefix" json:"prefix" usage:"The remote instance name prefix used to select this partition."`
	PartitionID string `yaml:"partition_id" json:"partition_id" usage:"The partition to use if the Group ID and prefix match."`
}

func EnsureDirectoryExists(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return os.MkdirAll(dir, 0755)
	}
	return nil
}

func DeleteLocalFileIfExists(filename string) {
	_, err := os.Stat(filename)
	if err == nil {
		if err := os.Remove(filename); err != nil {
			log.Warningf("Error deleting file %q: %s", filename, err)
		}
	}
}

func WriteFile(ctx context.Context, fullPath string, data []byte) (int, error) {
	ctx, spn := tracing.StartSpan(ctx)
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
	defer DeleteLocalFileIfExists(tmpFileName)

	if err := os.WriteFile(tmpFileName, data, 0644); err != nil {
		return 0, err
	}
	return len(data), os.Rename(tmpFileName, fullPath)
}

func ReadFile(ctx context.Context, fullPath string) ([]byte, error) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	data, err := os.ReadFile(fullPath)
	if os.IsNotExist(err) {
		return nil, status.NotFoundError(err.Error())
	}
	return data, err
}

func DeleteFile(ctx context.Context, fullPath string) error {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	return os.Remove(fullPath)
}

func FileExists(ctx context.Context, fullPath string) (bool, error) {
	ctx, spn := tracing.StartSpan(ctx)
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
	// context.DeadlineExceeded.
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

	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

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
	ctx context.Context
}

func (r *readCloser) Read(p []byte) (int, error) {
	_, spn := tracing.StartSpan(r.ctx)
	defer spn.End()
	return r.SectionReader.Read(p)
}

func FileReader(ctx context.Context, fullPath string, offset, length int64) (io.ReadCloser, error) {
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if length > 0 {
		return &readCloser{io.NewSectionReader(f, offset, length), f, ctx}, nil
	}
	return &readCloser{io.NewSectionReader(f, offset, info.Size()-offset), f, ctx}, nil
}

type writeMover struct {
	*os.File
	ctx       context.Context
	finalPath string
}

func (w *writeMover) Write(p []byte) (int, error) {
	_, spn := tracing.StartSpan(w.ctx)
	defer spn.End()
	return w.File.Write(p)
}

func (w *writeMover) Close() error {
	tmpName := w.File.Name()
	if err := w.File.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, w.finalPath)
}

func FileWriter(ctx context.Context, fullPath string) (io.WriteCloser, error) {
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
	// Ensure that the temp file is cleaned up here too!
	runtime.SetFinalizer(wm, func(m *writeMover) {
		DeleteLocalFileIfExists(tmpFileName)
	})
	return wm, nil
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

func GetDirUsage(path string) (*DirUsage, error) {
	fs := unix.Statfs_t{}
	if err := unix.Statfs(path, &fs); err != nil {
		return nil, err
	}
	return &DirUsage{
		TotalBytes: fs.Blocks * uint64(fs.Bsize),
		UsedBytes:  (fs.Blocks - fs.Bfree) * uint64(fs.Bsize),
		FreeBytes:  fs.Bfree * uint64(fs.Bsize),
		AvailBytes: fs.Bavail * uint64(fs.Bsize),
	}, nil
}
