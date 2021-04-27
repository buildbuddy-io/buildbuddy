package disk

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"

	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

func EnsureDirectoryExists(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return os.MkdirAll(dir, 0750)
	}
	return nil
}

func DeleteLocalFileIfExists(filename string) {
	_, err := os.Stat(filename)
	if os.IsExist(err) {
		os.Remove(filename)
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

	if err := ioutil.WriteFile(tmpFileName, data, 0600); err != nil {
		return 0, err
	}
	return len(data), os.Rename(tmpFileName, fullPath)
}

func ReadFile(ctx context.Context, fullPath string) ([]byte, error) {
	data, err := ioutil.ReadFile(filepath.Clean(fullPath))
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
	// Verbose for clarity.
	if os.IsNotExist(err) {
		return false, nil
	} else if os.IsExist(err) {
		return true, nil
	} else if err == nil {
		return true, nil
	} else {
		return false, err
	}
}

type SectionReaderCloser struct {
	*io.SectionReader
	io.Closer
}

func FileReader(ctx context.Context, fullPath string, offset, length int64) (*SectionReaderCloser, error) {
	f, err := os.Open(filepath.Clean(fullPath))
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

	tmpFile, err := ioutil.TempFile(filepath.Dir(fullPath), fmt.Sprintf("%s.*.tmp", filepath.Base(fullPath)))
	if err != nil {
		return nil, err
	}
	wm := &writeMover{
		File:      tmpFile,
		finalPath: fullPath,
	}
	// Ensure that the temp file is cleaned up here too!
	runtime.SetFinalizer(wm, func(m *writeMover) {
		DeleteLocalFileIfExists(tmpFile.Name())
	})
	return wm, nil
}
