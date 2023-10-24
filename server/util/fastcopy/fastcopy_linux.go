//go:build linux && !android

package fastcopy

import (
	"os"

	"golang.org/x/sys/unix"
)

func reflink(source, destination string) error {
	sourceFile, err := os.Open(source)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	info, err := sourceFile.Stat()
	if err != nil {
		return err
	}
	destFile, err := os.OpenFile(destination, os.O_RDWR|os.O_CREATE|os.O_EXCL, info.Mode())
	if err != nil {
		return err
	}
	defer destFile.Close()

	reflinkWasSuccessful := false
	defer func() {
		if !reflinkWasSuccessful {
			os.Remove(destination)
		}
	}()

	sourceConn, err := sourceFile.SyscallConn()
	if err != nil {
		return err
	}
	destConn, err := destFile.SyscallConn()
	if err != nil {
		return err
	}

	var destErr, sourceErr, ioctlErr error
	destErr = destConn.Control(func(dfd uintptr) {
		sourceErr = sourceConn.Control(func(sfd uintptr) {
			ioctlErr = unix.IoctlFileClone(int(dfd), int(sfd))
		})
	})
	if ioctlErr != nil {
		return ioctlErr
	}
	if sourceErr != nil {
		return sourceErr
	}
	if destErr != nil {
		return destErr
	}
	reflinkWasSuccessful = true
	return nil
}
