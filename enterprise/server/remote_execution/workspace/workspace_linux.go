//go:build linux && !android

package workspace

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
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

// readXattrs returns the names and values of the extended attributes of the
// file at path, sorted by name and encoded as a string, so that comparing the
// strings tells whether the attributes changed. It leaves out security.*
// attributes, such as SELinux labels, since container runtimes change them
// for their own reasons, as is the case when Docker relabels a workspace that
// it mounts with --executor.docker_mount_mode=z.
func readXattrs(path string) (string, error) {
	// Most directories have no extended attributes, so we first ask for the
	// size of the list, which doesn't need a buffer.
	size, err := unix.Llistxattr(path, nil)
	if errors.Is(err, errors.ErrUnsupported) {
		// The filesystem doesn't support extended attributes.
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("list xattrs: %w", err)
	}
	if size == 0 {
		return "", nil
	}
	list, err := xattrCall(func(buf []byte) (int, error) { return unix.Llistxattr(path, buf) })
	if err != nil {
		return "", fmt.Errorf("list xattrs: %w", err)
	}
	var names []string
	for name := range strings.SplitSeq(strings.TrimSuffix(string(list), "\x00"), "\x00") {
		if name != "" && !strings.HasPrefix(name, "security.") {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	var encoded []byte
	for _, name := range names {
		value, err := xattrCall(func(buf []byte) (int, error) { return unix.Lgetxattr(path, name, buf) })
		if errors.Is(err, errors.ErrUnsupported) {
			// The filesystem lists the attribute but can't read it.
			continue
		}
		if err != nil {
			return "", fmt.Errorf("get xattr %q: %w", name, err)
		}
		// Values can hold any byte, so we prefix them with their length to
		// keep the encoding unambiguous. Names can't hold NUL bytes.
		encoded = append(encoded, name...)
		encoded = append(encoded, 0)
		encoded = binary.AppendUvarint(encoded, uint64(len(value)))
		encoded = append(encoded, value...)
	}
	return string(encoded), nil
}

// xattrCall calls an xattr syscall that fills buf, such as listxattr, and
// returns what it filled in. Directories usually have no extended attributes,
// or a few short ones such as an SELinux label, so we start with a small buffer
// and double it while the result doesn't fit. The kernel limits results to 64
// KiB, so this ends.
func xattrCall(call func(buf []byte) (int, error)) ([]byte, error) {
	for size := 256; ; size *= 2 {
		buf := make([]byte, size)
		n, err := call(buf)
		if errors.Is(err, unix.ERANGE) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return buf[:n], nil
	}
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
