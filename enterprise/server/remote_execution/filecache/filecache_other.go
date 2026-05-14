//go:build !linux && !darwin && !windows

package filecache

func syncFilesystem(path string) error {
	return nil
}
