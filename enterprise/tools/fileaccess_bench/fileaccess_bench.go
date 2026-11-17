// fileaccess_bench measures what the fileaccess eBPF program adds to the cost
// of opening a file, for processes in a recorded cgroup and for every other
// process on the host, and checks that a recording returns every file that
// was opened. It must run as root, and no other process on the host should
// have the program loaded (stop executors that run with
// --executor.oci.record_file_accesses).
//
// It creates a tree of empty files and a cgroup, and times child processes
// in that cgroup that open and close files, in these phases:
//
//	baseline   the eBPF program is not loaded
//	untracked  the program is loaded, and the children's cgroup is not recorded
//	first      the cgroup is recorded, and each file is opened for the first time
//	seen       the cgroup is recorded, and each file was opened before
//
// Each phase opens every file of the tree once ("unique"), and opens a single
// file many times ("repeat"). With several processes, the processes split the
// files (or the repeated opens) between them and start at the same moment.
// Each process has a single thread, as most build tools do, because threads
// of a process share a file descriptor table and contend for its lock.
//
// After each "first" phase it compares the paths that the recording returned
// with the files that were opened, and prints how many are missing and how
// often the kernel skipped the program meanwhile (recursion_misses in the
// program's fdinfo). On a kernel that preempts tasks in kernel mode, the
// kernel skips an fentry program on a CPU where a task was preempted inside
// it, so files go missing when processes compete for CPUs.
//
// Timings on a 32-CPU machine:
//
//	bb build //enterprise/tools/fileaccess_bench
//	sudo bazel-bin/enterprise/tools/fileaccess_bench/fileaccess_bench_/fileaccess_bench --procs=1,8,32
//
// Only the completeness check, with twice as many processes as CPUs, and
// with processes that compute for 200 microseconds after each open:
//
//	sudo .../fileaccess_bench --only_first --reps=20 --procs=32,64
//	sudo .../fileaccess_bench --only_first --reps=20 --procs=32,64 --files=64000 --work_us=200
//
// --hook=lsm loads the prototype that attaches to the BPF LSM hook file_open
// (fileaccess.NewLSMTracker) in place of the fentry program.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/ociruntime/fileaccess"
	"golang.org/x/sys/unix"
)

var (
	dir         = flag.String("dir", "", "Directory to create the test files in, which must not exist. It is deleted afterwards. Defaults to a new directory in the system temp directory.")
	files       = flag.Int("files", 100000, "Number of distinct files to open.")
	repeatOpens = flag.Int("repeat_opens", 2000000, "Number of times to open a single file.")
	procs       = flag.String("procs", "1,8", "Comma-separated numbers of processes to measure with.")
	reps        = flag.Int("reps", 5, "Number of repetitions of each measurement. The median is reported.")
	noTracker   = flag.Bool("no_tracker", false, "Only measure the baseline, without loading the eBPF program.")
	overlay     = flag.Bool("overlay", false, "Also record a read of a file through an overlayfs mount, and print the recorded paths.")
	hook        = flag.String("hook", "fentry", "How the eBPF program attaches: fentry (security_file_open, as in the PR) or lsm (the BPF LSM hook file_open).")
	workUs      = flag.Int("work_us", 0, "Microseconds of CPU work that each process does after each open, to imitate a tool that does more than open files. Leave it at 0 for the timings, which include it.")
	onlyFirst   = flag.Bool("only_first", false, "Skip the timings and only check, in each repetition, that a recording returns every file that was opened.")

	childMode    = flag.String("child_mode", "", "Internal: unique, repeat or cat.")
	childIndex   = flag.Int("child_index", 0, "Internal: which of the processes this child is.")
	childProcs   = flag.Int("child_procs", 1, "Internal: number of processes that share the work.")
	childStartNs = flag.Int64("child_start_ns", 0, "Internal: Unix time in nanoseconds at which the child starts opening files.")
	childPath    = flag.String("child_path", "", "Internal: the file that the cat child reads.")
)

// childResult is what a child prints when it is done.
type childResult struct {
	Ops int `json:"ops"`
	// Ns is the time that the child spent in its loop.
	Ns int64 `json:"ns"`
}

func filePath(root string, i int) string {
	return filepath.Join(root, "bazel-out/k8-fastbuild/bin/external", fmt.Sprintf("repo_%04d/pkg/include", i/1000), fmt.Sprintf("header_%06d.h", i))
}

func runChild() error {
	var paths []*byte
	switch *childMode {
	case "cat":
		_, err := os.ReadFile(*childPath)
		return err
	case "unique":
		for i := *childIndex; i < *files; i += *childProcs {
			p, err := unix.BytePtrFromString(filePath(*dir, i))
			if err != nil {
				return err
			}
			paths = append(paths, p)
		}
	case "repeat":
		p, err := unix.BytePtrFromString(filePath(*dir, 0))
		if err != nil {
			return err
		}
		paths = slices.Repeat([]*byte{p}, *repeatOpens / *childProcs)
	default:
		return fmt.Errorf("unknown child mode %q", *childMode)
	}
	work := time.Duration(*workUs) * time.Microsecond
	runtime.LockOSThread()
	// Spin until the start time, so that every child is on a CPU when the
	// work starts.
	for time.Now().UnixNano() < *childStartNs {
	}
	begin := time.Now()
	for _, p := range paths {
		fd, _, errno := unix.RawSyscall6(unix.SYS_OPENAT, uintptr(unix.AT_FDCWD&0xffffffff), uintptr(unsafe.Pointer(p)), unix.O_RDONLY|unix.O_CLOEXEC, 0, 0, 0)
		if errno != 0 {
			return fmt.Errorf("open: %w", errno)
		}
		unix.RawSyscall(unix.SYS_CLOSE, fd, 0, 0)
		if work > 0 {
			for t := time.Now(); time.Since(t) < work; {
			}
		}
	}
	return json.NewEncoder(os.Stdout).Encode(childResult{Ops: len(paths), Ns: time.Since(begin).Nanoseconds()})
}

// bench runs children in a cgroup and collects their timings.
type bench struct {
	root       string
	cgroupPath string
	cgroup     *os.File
	// samples holds the ns per open of each repetition, by row label.
	samples map[string][]float64
	labels  []string
}

// children runs n children at once and returns the sum of their results.
func (b *bench) children(mode string, n int, extra ...string) (childResult, error) {
	start := time.Now().Add(200*time.Millisecond + time.Duration(n)*5*time.Millisecond)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var total childResult
	var firstErr error
	for i := range n {
		args := []string{
			"--child_mode=" + mode, "--child_index=" + strconv.Itoa(i), "--child_procs=" + strconv.Itoa(n), "--child_start_ns=" + strconv.FormatInt(start.UnixNano(), 10),
			"--dir=" + b.root, "--files=" + strconv.Itoa(*files), "--repeat_opens=" + strconv.Itoa(*repeatOpens), "--work_us=" + strconv.Itoa(*workUs),
		}
		cmd := exec.Command("/proc/self/exe", append(args, extra...)...)
		cmd.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: int(b.cgroup.Fd())}
		cmd.Stderr = os.Stderr
		wg.Go(func() {
			out, err := cmd.Output()
			var res childResult
			if err == nil && mode != "cat" {
				err = json.Unmarshal(out, &res)
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("run %s child: %w", mode, err)
			}
			total.Ops += res.Ops
			total.Ns += res.Ns
		})
	}
	wg.Wait()
	return total, firstErr
}

// measure runs children and records their time per open under the given
// label.
func (b *bench) measure(label, mode string, n int) error {
	res, err := b.children(mode, n)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s\t%s\t%d", label, mode, n)
	if _, ok := b.samples[key]; !ok {
		b.labels = append(b.labels, key)
	}
	b.samples[key] = append(b.samples[key], float64(res.Ns)/float64(res.Ops))
	return nil
}

func median(v []float64) float64 {
	s := slices.Clone(v)
	sort.Float64s(s)
	return s[len(s)/2]
}

// recursionMisses returns how many runs of the eBPF programs that this
// process holds the kernel skipped, because another run of the program was
// in progress on the same CPU.
func recursionMisses() int64 {
	entries, err := os.ReadDir("/proc/self/fdinfo")
	if err != nil {
		return -1
	}
	var total int64
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join("/proc/self/fdinfo", e.Name()))
		if err != nil {
			continue
		}
		for line := range strings.SplitSeq(string(b), "\n") {
			if v, ok := strings.CutPrefix(line, "recursion_misses:"); ok {
				n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
				if err != nil {
					return -1
				}
				total += n
			}
		}
	}
	return total
}

// cpuPressureUs returns for how long, in microseconds since boot, at least one
// task on the host has been waiting for a CPU (the "some" line of
// /proc/pressure/cpu), or -1 if the kernel does not report it.
func cpuPressureUs() int64 {
	b, err := os.ReadFile("/proc/pressure/cpu")
	if err != nil {
		return -1
	}
	for line := range strings.SplitSeq(string(b), "\n") {
		if !strings.HasPrefix(line, "some ") {
			continue
		}
		if _, v, ok := strings.Cut(line, "total="); ok {
			n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			if err == nil {
				return n
			}
		}
	}
	return -1
}

func run() error {
	root := *dir
	if root == "" {
		root = filepath.Join(os.TempDir(), fmt.Sprintf("fileaccess-bench-%d", os.Getpid()))
	}
	if err := os.Mkdir(root, 0755); err != nil {
		return fmt.Errorf("create test directory: %w", err)
	}
	defer os.RemoveAll(root)
	for i := range *files {
		p := filePath(root, i)
		if i%1000 == 0 {
			if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
				return err
			}
		}
		if err := os.WriteFile(p, nil, 0644); err != nil {
			return err
		}
	}
	cgroupPath := fmt.Sprintf("/sys/fs/cgroup/fileaccess-bench-%d", os.Getpid())
	if err := os.Mkdir(cgroupPath, 0755); err != nil {
		return fmt.Errorf("create cgroup: %w", err)
	}
	defer os.Remove(cgroupPath)
	cg, err := os.Open(cgroupPath)
	if err != nil {
		return err
	}
	defer cg.Close()
	b := &bench{root: root, cgroupPath: cgroupPath, cgroup: cg, samples: map[string][]float64{}}

	var procCounts []int
	for s := range strings.SplitSeq(*procs, ",") {
		n, err := strconv.Atoi(s)
		if err != nil {
			return fmt.Errorf("parse --procs: %w", err)
		}
		procCounts = append(procCounts, n)
	}
	fmt.Printf("%d files under %s (path length %d), %d repeat opens, %d repetitions, kernel %s, %d CPUs, hook %s, %d us of work after each open\n", *files, root, len(filePath(root, 0)), *repeatOpens, *reps, kernelRelease(), runtime.NumCPU(), *hook, *workUs)

	// Fill the dentry cache, so that no phase pays for the first lookups.
	if _, err := b.children("unique", 1); err != nil {
		return err
	}
	if !*onlyFirst {
		for _, n := range procCounts {
			for range *reps {
				if err := b.measure("baseline", "unique", n); err != nil {
					return err
				}
				if err := b.measure("baseline", "repeat", n); err != nil {
					return err
				}
			}
		}
	}
	if !*noTracker {
		newTracker := fileaccess.NewTracker
		if *hook == "lsm" {
			newTracker = fileaccess.NewLSMTracker
		}
		tracker, err := newTracker()
		if err != nil {
			return fmt.Errorf("create tracker: %w", err)
		}
		defer tracker.Close()
		prefix := filepath.Join(root, "bazel-out") + "/"
		for _, n := range procCounts {
			var stops, missing []float64
			incomplete := 0
			var pressureUs, openUs int64
			for rep := range *reps {
				if !*onlyFirst {
					if err := b.measure("untracked", "unique", n); err != nil {
						return err
					}
					if err := b.measure("untracked", "repeat", n); err != nil {
						return err
					}
				}
				missesBefore := recursionMisses()
				rec, err := tracker.Start(cgroupPath)
				if err != nil {
					return fmt.Errorf("start recording: %w", err)
				}
				pressureBefore, openStart := cpuPressureUs(), time.Now()
				if err := b.measure("first", "unique", n); err != nil {
					return err
				}
				missesFirst := recursionMisses()
				pressureUs += cpuPressureUs() - pressureBefore
				openUs += time.Since(openStart).Microseconds()
				if !*onlyFirst {
					if err := b.measure("seen", "unique", n); err != nil {
						return err
					}
					if err := b.measure("seen", "repeat", n); err != nil {
						return err
					}
				}
				stopStart := time.Now()
				paths, dropped, err := rec.Stop()
				if err != nil {
					return fmt.Errorf("stop recording: %w", err)
				}
				stops = append(stops, float64(time.Since(stopStart).Microseconds())/1e3)
				recorded := 0
				for _, p := range paths {
					if strings.HasPrefix(p, prefix) {
						recorded++
					}
				}
				missing = append(missing, 100*float64(*files-recorded)/float64(*files))
				if recorded != *files {
					incomplete++
					fmt.Printf("procs=%d rep %d: the recording returned %d of the %d files that were opened (%d missing), dropped=%d; the kernel skipped the program %d times while they were opened\n", n, rep, recorded, *files, *files-recorded, dropped, missesFirst-missesBefore)
				}
			}
			var missingSum float64
			for _, m := range missing {
				missingSum += m
			}
			fmt.Printf("procs=%d: %d of %d recordings were incomplete, missing %.2f%% of the files on average and %.2f%% at most; Stop took %.1f ms (median; min %.1f, max %.1f); some task on the host waited for a CPU %.0f%% of the time\n",
				n, incomplete, *reps, missingSum/float64(len(missing)), slices.Max(missing), median(stops), slices.Min(stops), slices.Max(stops), 100*float64(pressureUs)/float64(openUs))
		}
		if *overlay {
			if err := overlayCheck(b, tracker); err != nil {
				return fmt.Errorf("overlay check: %w", err)
			}
		}
		fmt.Printf("recursion_misses=%d in total\n", recursionMisses())
	}

	fmt.Printf("\n%-10s %-7s %7s %10s %8s %8s %10s\n", "phase", "opens", "procs", "ns/open", "min", "max", "vs base")
	base := map[string]float64{}
	for _, key := range b.labels {
		f := strings.Split(key, "\t")
		m := median(b.samples[key])
		if f[0] == "baseline" {
			base[f[1]+"\t"+f[2]] = m
		}
		vs := ""
		if v, ok := base[f[1]+"\t"+f[2]]; ok {
			vs = fmt.Sprintf("%+10.0f", m-v)
		}
		fmt.Printf("%-10s %-7s %7s %10.0f %8.0f %8.0f %10s\n", f[0], f[1], f[2], m, slices.Min(b.samples[key]), slices.Max(b.samples[key]), vs)
	}
	return nil
}

func kernelRelease() string {
	var u unix.Utsname
	if err := unix.Uname(&u); err != nil {
		return "unknown"
	}
	return unix.ByteSliceToString(u.Release[:])
}

// overlayCheck records a child that reads a file of an overlayfs mount's
// lower directory through the mount, and prints every recorded path.
func overlayCheck(b *bench, tracker *fileaccess.Tracker) error {
	base := filepath.Join(b.root, "overlay")
	lower, upper, work, merged := filepath.Join(base, "lower"), filepath.Join(base, "upper"), filepath.Join(base, "work"), filepath.Join(base, "merged")
	for _, d := range []string{filepath.Join(lower, "usr/lib"), upper, work, merged} {
		if err := os.MkdirAll(d, 0755); err != nil {
			return err
		}
	}
	if err := os.WriteFile(filepath.Join(lower, "usr/lib/libexample.so"), []byte("x"), 0644); err != nil {
		return err
	}
	if err := unix.Mount("overlay", merged, "overlay", 0, fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", lower, upper, work)); err != nil {
		return fmt.Errorf("mount overlayfs: %w", err)
	}
	defer unix.Unmount(merged, 0)
	rec, err := tracker.Start(b.cgroupPath)
	if err != nil {
		return err
	}
	_, childErr := b.children("cat", 1, "--child_path="+filepath.Join(merged, "usr/lib/libexample.so"))
	paths, dropped, err := rec.Stop()
	if childErr != nil {
		return childErr
	}
	if err != nil {
		return err
	}
	sort.Strings(paths)
	fmt.Printf("overlay check: reading %s recorded %d paths (%d dropped):\n", filepath.Join(merged, "usr/lib/libexample.so"), len(paths), dropped)
	for _, p := range paths {
		fmt.Printf("  %s\n", p)
	}
	return nil
}

func main() {
	flag.Parse()
	var err error
	if *childMode != "" {
		err = runChild()
	} else {
		err = run()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
