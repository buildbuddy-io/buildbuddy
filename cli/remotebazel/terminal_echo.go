package remotebazel

import (
	"os"

	"golang.org/x/sys/unix"
)

// disableTerminalEcho disables writing input to the terminal, but preserves
// normal terminal signal handling.
// Returns a function to restore the original settings and flush queued input.
func disableTerminalEchoWithRequests(f *os.File, getReq, setReq, restoreReq uint) (func() error, error) {
	fd := int(f.Fd())
	oldState, err := unix.IoctlGetTermios(fd, getReq)
	if err != nil {
		return nil, err
	}

	newState := *oldState
	newState.Lflag &^= unix.ECHO | unix.ECHONL
	if err := unix.IoctlSetTermios(fd, setReq, &newState); err != nil {
		return nil, err
	}

	return func() error {
		return unix.IoctlSetTermios(fd, restoreReq, oldState)
	}, nil
}
