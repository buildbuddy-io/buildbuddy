package signalutil

import (
	"os"
	"os/signal"
	"syscall"
)

func OnSignal(s syscall.Signal, fn func() error, errFn func(error)) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, s)
	go func() {
		for range c {
			if err := fn(); err != nil {
				errFn(err)
			}
		}
	}()
}
