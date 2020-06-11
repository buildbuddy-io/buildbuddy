package disk

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
)

func EnsureDirectoryExists(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return os.MkdirAll(dir, 0755)
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

	tmpFileName := fullPath + ".tmp"
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
	return ioutil.ReadFile(fullPath)
}

func DeleteFile(ctx context.Context, fullPath string) error {
	return os.Remove(fullPath)
}

func FileExists(ctx context.Context, fullPath string) (bool, error) {
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

func FileReader(ctx context.Context, fullPath string, offset, length int64) (io.Reader, error) {
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	f.Seek(offset, 0)
	if length > 0 {
		return io.LimitReader(f, length), nil
	}
	return f, nil
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
	tmpFileName := fullPath + ".tmp"
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
