// Package fileaccess records which files are opened by processes in a cgroup,
// using an eBPF program attached to the BPF LSM hook file_open.
package fileaccess

import (
	"bytes"
	_ "embed"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/ringbuf"
	"golang.org/x/sys/unix"
)

// fileaccess.bpf.o is compiled from fileaccess.bpf.c by the fileaccess_bpf
// genrule.
//
//go:embed fileaccess.bpf.o
var bpfObject []byte

// key mirrors struct key in fileaccess.bpf.c.
type key struct {
	CgroupID uint64
	Dentry   uint64
	Ino      uint64
}

// pathOffset is offsetof(struct event, path) in fileaccess.bpf.c.
const pathOffset = 28

// Tracker records file opens for registered cgroups. A single Tracker should
// be shared by all containers on a host.
type Tracker struct {
	objs struct {
		FileOpen *ebpf.Program `ebpf:"file_open"`
		Tracked  *ebpf.Map     `ebpf:"tracked"`
		Seen     *ebpf.Map     `ebpf:"seen"`
		Scratch  *ebpf.Map     `ebpf:"scratch"`
		Events   *ebpf.Map     `ebpf:"events"`
	}
	link   link.Link
	reader *ringbuf.Reader

	mu         sync.Mutex
	recordings map[uint64]*Recording

	flushMu sync.Mutex
	flushed chan struct{}
}

// ErrBPFLSMNotActive is returned by NewTracker when the kernel's BPF LSM is not
// active, so the program would never run.
var ErrBPFLSMNotActive = errors.New("BPF LSM is not active")

// NewTracker loads and attaches the eBPF program. It requires CAP_SYS_ADMIN,
// Linux 6.10 or later with BTF, and the BPF LSM to be active.
func NewTracker() (*Tracker, error) {
	if err := checkBPFLSM(); err != nil {
		return nil, err
	}
	spec, err := ebpf.LoadCollectionSpecFromReader(bytes.NewReader(bpfObject))
	if err != nil {
		return nil, fmt.Errorf("load eBPF spec: %w", err)
	}
	t := &Tracker{
		recordings: make(map[uint64]*Recording),
		flushed:    make(chan struct{}),
	}
	if err := spec.LoadAndAssign(&t.objs, nil); err != nil {
		return nil, fmt.Errorf("load eBPF objects: %w", err)
	}
	t.link, err = link.AttachLSM(link.LSMOptions{Program: t.objs.FileOpen})
	if err != nil {
		t.closeObjs()
		return nil, fmt.Errorf("attach eBPF program: %w", err)
	}
	t.reader, err = ringbuf.NewReader(t.objs.Events)
	if err != nil {
		t.link.Close()
		t.closeObjs()
		return nil, fmt.Errorf("open eBPF ring buffer: %w", err)
	}
	go t.readEvents()
	return t, nil
}

// checkBPFLSM returns ErrBPFLSMNotActive unless "bpf" is in the kernel's list
// of active LSMs. securityfs, which exposes that list, is mounted at a
// temporary directory because it is usually not mounted in containers.
func checkBPFLSM() error {
	dir, err := os.MkdirTemp("", "securityfs-")
	if err != nil {
		return err
	}
	defer os.Remove(dir)
	flags := uintptr(unix.MS_RDONLY | unix.MS_NOSUID | unix.MS_NODEV | unix.MS_NOEXEC)
	if err := unix.Mount("securityfs", dir, "securityfs", flags, ""); err != nil {
		return fmt.Errorf("mount securityfs: %w", err)
	}
	defer unix.Unmount(dir, 0)
	b, err := os.ReadFile(filepath.Join(dir, "lsm"))
	if err != nil {
		return fmt.Errorf("read active LSMs: %w", err)
	}
	lsms := strings.TrimSpace(string(b))
	if !slices.Contains(strings.Split(lsms, ","), "bpf") {
		return fmt.Errorf("%w (active LSMs: %s); add bpf to the kernel's lsm= boot parameter", ErrBPFLSMNotActive, lsms)
	}
	return nil
}

func (t *Tracker) closeObjs() {
	t.objs.FileOpen.Close()
	t.objs.Tracked.Close()
	t.objs.Seen.Close()
	t.objs.Scratch.Close()
	t.objs.Events.Close()
}

// Close detaches the eBPF program. Recordings in progress return no further
// paths.
func (t *Tracker) Close() error {
	err := errors.Join(t.link.Close(), t.reader.Close())
	t.closeObjs()
	return err
}

func (t *Tracker) readEvents() {
	var rec ringbuf.Record
	for {
		err := t.reader.ReadInto(&rec)
		if errors.Is(err, ringbuf.ErrFlushed) {
			t.flushed <- struct{}{}
			continue
		}
		if errors.Is(err, ringbuf.ErrClosed) {
			return
		}
		if err != nil {
			log.Warningf("Failed to read file access event: %s", err)
			continue
		}
		t.handleEvent(rec.RawSample)
	}
}

func (t *Tracker) handleEvent(b []byte) {
	if len(b) < pathOffset {
		return
	}
	k := key{
		CgroupID: binary.NativeEndian.Uint64(b[0:8]),
		Dentry:   binary.NativeEndian.Uint64(b[8:16]),
		Ino:      binary.NativeEndian.Uint64(b[16:24]),
	}
	n := int(binary.NativeEndian.Uint32(b[24:28]))
	if n < 1 || pathOffset+n > len(b) {
		return
	}
	path := string(b[pathOffset : pathOffset+n-1])

	t.mu.Lock()
	r := t.recordings[k.CgroupID]
	if r != nil {
		r.keys = append(r.keys, k)
		r.paths[path] = struct{}{}
	}
	t.mu.Unlock()
	if r == nil {
		t.objs.Seen.Delete(k)
	}
}

// flush waits until all events submitted so far have been handled.
func (t *Tracker) flush() error {
	t.flushMu.Lock()
	defer t.flushMu.Unlock()
	if err := t.reader.Flush(); err != nil {
		return err
	}
	<-t.flushed
	return nil
}

// Recording collects the files opened by processes in one cgroup.
type Recording struct {
	tracker  *Tracker
	cgroupID uint64

	// Guarded by tracker.mu.
	keys  []key
	paths map[string]struct{}
}

// Start begins recording files opened for reading by processes in the given
// cgroup (an absolute path under /sys/fs/cgroup) or its descendants.
// The cgroup must not already be recorded.
func (t *Tracker) Start(cgroupPath string) (*Recording, error) {
	var st unix.Stat_t
	if err := unix.Stat(cgroupPath, &st); err != nil {
		return nil, fmt.Errorf("stat cgroup: %w", err)
	}
	r := &Recording{
		tracker:  t,
		cgroupID: st.Ino,
		paths:    make(map[string]struct{}),
	}
	t.mu.Lock()
	t.recordings[r.cgroupID] = r
	t.mu.Unlock()
	if err := t.objs.Tracked.Put(r.cgroupID, uint64(0)); err != nil {
		t.mu.Lock()
		delete(t.recordings, r.cgroupID)
		t.mu.Unlock()
		return nil, fmt.Errorf("track cgroup: %w", err)
	}
	return r, nil
}

// Stop ends the recording and returns the paths of the regular files that were
// opened for reading, as seen from the root directory of the opening process.
// If dropped is non-zero, that many opens could not be recorded (for example
// because a buffer was full) and paths is incomplete.
func (r *Recording) Stop() (paths []string, dropped uint64, err error) {
	t := r.tracker
	var errs []error
	if err := t.objs.Tracked.Lookup(r.cgroupID, &dropped); err != nil {
		errs = append(errs, fmt.Errorf("read dropped count: %w", err))
	}
	if err := t.objs.Tracked.Delete(r.cgroupID); err != nil {
		errs = append(errs, fmt.Errorf("untrack cgroup: %w", err))
	}
	if err := t.flush(); err != nil {
		errs = append(errs, fmt.Errorf("flush events: %w", err))
	}

	t.mu.Lock()
	delete(t.recordings, r.cgroupID)
	keys, pathSet := r.keys, r.paths
	r.keys, r.paths = nil, nil
	t.mu.Unlock()

	if len(keys) > 0 {
		if _, err := t.objs.Seen.BatchDelete(keys, nil); err != nil {
			errs = append(errs, fmt.Errorf("delete seen keys: %w", err))
		}
	}
	paths = make([]string, 0, len(pathSet))
	for p := range pathSet {
		paths = append(paths, p)
	}
	return paths, dropped, errors.Join(errs...)
}
