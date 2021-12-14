package disk

import (
	"context"
	"fmt"
	"io/ioutil"
	"runtime"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing/ctxio"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing/filepath"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing/fs"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing/os"
)

func EnsureDirectoryExists(ctx context.Context, dir string) error {
	if _, err := os.Stat(ctx, dir); os.IsNotExist(err) {
		return os.MkdirAll(ctx, dir, 0755)
	}
	return nil
}

func DeleteLocalFileIfExists(ctx context.Context, filename string) {
	_, err := os.Stat(ctx, filename)
	if err == nil {
		if err := os.Remove(ctx, filename); err != nil {
			log.Warningf("Error deleting file %q: %s", filename, err)
		}
	}
}

func WriteFile(ctx context.Context, fullPath string, data []byte) (int, error) {
	if err := EnsureDirectoryExists(ctx, filepath.Dir(fullPath)); err != nil {
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
	defer DeleteLocalFileIfExists(ctx, tmpFileName)

	if err := ioutil.WriteFile(tmpFileName, data, 0644); err != nil {
		return 0, err
	}
	return len(data), os.Rename(ctx, tmpFileName, fullPath)
}

func ReadFile(ctx context.Context, fullPath string) ([]byte, error) {
	data, err := ioutil.ReadFile(fullPath)
	if os.IsNotExist(err) {
		return nil, status.NotFoundError(err.Error())
	}
	return data, err
}

func DeleteFile(ctx context.Context, fullPath string) error {
	return os.Remove(ctx, fullPath)
}

func FileExists(ctx context.Context, fullPath string) (bool, error) {
	_, err := os.Stat(ctx, fullPath)
	if err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return false, err
	}
}

type SectionReaderCloser struct {
	*ctxio.SectionReader
	ctxio.Closer
}

func FileReader(ctx context.Context, fullPath string, offset, length int64) (*SectionReaderCloser, error) {
	f, err := os.Open(ctx, fullPath)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat(ctx)
	if err != nil {
		return nil, err
	}
	if length > 0 {
		return &SectionReaderCloser{ctxio.NewSectionReader(f, offset, length), f}, nil
	}
	return &SectionReaderCloser{ctxio.NewSectionReader(f, offset, info.Size()-offset), f}, nil
}

type writeMover struct {
	*os.File
	finalPath string
}

func (w *writeMover) Close(ctx context.Context) error {
	tmpName := w.File.Name()
	if err := w.File.Close(ctx); err != nil {
		return err
	}
	return os.Rename(ctx, tmpName, w.finalPath)
}

func FileWriter(ctx context.Context, fullPath string) (ctxio.WriteCloser, error) {
	if err := EnsureDirectoryExists(ctx, filepath.Dir(fullPath)); err != nil {
		return nil, err
	}
	randStr, err := random.RandomString(10)
	if err != nil {
		return nil, err
	}

	tmpFileName := fullPath + fmt.Sprintf(".%s.tmp", randStr)
	f, err := os.OpenFile(ctx, tmpFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	wm := &writeMover{
		File:      f,
		finalPath: fullPath,
	}
	// Ensure that the temp file is cleaned up here too!
	runtime.SetFinalizer(wm, func(m *writeMover) {
		DeleteLocalFileIfExists(ctx, tmpFileName)
	})
	return wm, nil
}

// DirSize returns the size of a directory specified by path, in bytes.
func DirSize(ctx context.Context, path string) (int64, error) {
	var size int64
	err := filepath.WalkDir(ctx, path, func(ctx context.Context, _ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info(ctx)
		if err != nil {
			return err
		}
		size += info.Size()
		return nil
	})
	return size, err
}
