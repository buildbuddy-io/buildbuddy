//go:build linux && !android

package workspace

import (
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

// lstatDir returns the fingerprint of the directory at path, and false if path
// isn't a directory.
func lstatDir(path string) (dirFingerprint, bool, error) {
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		return dirFingerprint{}, false, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR {
		return dirFingerprint{}, false, nil
	}
	return dirFingerprint{
		ino:        st.Ino,
		ctimeNanos: st.Ctim.Nano(),
		mode:       st.Mode,
		uid:        st.Uid,
		gid:        st.Gid,
	}, true, nil
}

// waitForTimestampTick waits until the kernel's coarse clock is past t so we
// can detect directory changes made by the action after we record fingerprints.
// The kernel takes file timestamps from this clock, which only advances once
// per timer tick (every 1-10 ms, depending on CONFIG_HZ). Linux 6.13 added
// finer "multigrain" timestamps for some filesystems, but older kernels don't
// have them. If the action changes a directory in the same tick as setup's last
// change, the kernel assigns the same ctime we recorded, so we miss the
// action's change. We pass the newest recorded ctime as t, and normally wait
// at most one tick before starting the action.
//
// If the wall clock steps backwards, as is the case when a clock sync corrects
// it, the coarse clock may not pass t for a long time. So we give up after a
// few seconds, measured on the monotonic clock, and return an error, which
// makes the caller skip pooling the workspace.
func waitForTimestampTick(t int64) error {
	deadline := time.Now().Add(3 * time.Second)
	for {
		var now unix.Timespec
		if err := unix.ClockGettime(unix.CLOCK_REALTIME_COARSE, &now); err != nil {
			return fmt.Errorf("read coarse clock: %w", err)
		}
		if now.Nano() > t {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("coarse clock (%d) didn't pass the newest directory ctime (%d)", now.Nano(), t)
		}
		time.Sleep(500 * time.Microsecond)
	}
}
