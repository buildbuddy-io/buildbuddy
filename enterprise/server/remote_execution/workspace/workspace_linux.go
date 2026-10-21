//go:build linux && !android

package workspace

import (
	"errors"
	"fmt"
	"strings"
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

// hasXattrs returns whether the file at path has xattrs other than its SELinux
// label, security.selinux, which hosts that run SELinux give every new file.
//
// TODO: On hosts that give new directories other xattrs, such as Smack labels
// (security.SMACK64) or ACLs inherited from a default ACL (system.posix_acl_*),
// we discard every input directory that a task changes. If self-hosted
// executors need it, record each input directory's xattrs after input setup
// and compare against them instead.
func hasXattrs(path string) (bool, error) {
	// Most directories have no xattrs, or only an SELinux label, so we first
	// ask for the size of the list, which doesn't need a buffer.
	size, err := unix.Llistxattr(path, nil)
	if errors.Is(err, errors.ErrUnsupported) {
		// The filesystem doesn't support xattrs.
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("list xattrs: %w", err)
	}
	if size == 0 {
		return false, nil
	}
	list := make([]byte, size)
	n, err := unix.Llistxattr(path, list)
	if errors.Is(err, unix.ERANGE) {
		// The kernel returns ERANGE if someone added xattrs after we asked for
		// the size of the list, so the directory has at least one xattr besides
		// an SELinux label.
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("list xattrs: %w", err)
	}
	for name := range strings.SplitSeq(strings.TrimSuffix(string(list[:n]), "\x00"), "\x00") {
		if name != "" && name != "security.selinux" {
			return true, nil
		}
	}
	return false, nil
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
