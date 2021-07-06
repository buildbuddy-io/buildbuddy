package disk

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

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

	if err := ioutil.WriteFile(tmpFileName, data, 0644); err != nil {
		return 0, err
	}
	return len(data), os.Rename(tmpFileName, fullPath)
}

func ReadFile(ctx context.Context, fullPath string) ([]byte, error) {
	data, err := ioutil.ReadFile(fullPath)
	if os.IsNotExist(err) {
		return nil, status.NotFoundError(err.Error())
	}
	return data, err
}

func DeleteFile(ctx context.Context, fullPath string) error {
	return os.Remove(fullPath)
}

func FileExists(fullPath string) (bool, error) {
	_, err := os.Stat(fullPath)
	if err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return false, err
	}
}

type SectionReaderCloser struct {
	*io.SectionReader
	io.Closer
}

func FileReader(ctx context.Context, fullPath string, offset, length int64) (*SectionReaderCloser, error) {
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if length > 0 {
		return &SectionReaderCloser{io.NewSectionReader(f, offset, length), f}, nil
	}
	return &SectionReaderCloser{io.NewSectionReader(f, offset, info.Size()-offset), f}, nil
}

type writeMover struct {
	*os.File
	finalPath string
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
