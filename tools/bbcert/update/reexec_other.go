//go:build windows

package update

import "errors"

func Reexec() error { return errors.New("restart is not supported on this platform") }
