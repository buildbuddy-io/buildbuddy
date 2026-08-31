//go:build openbsd

package processlist

import (
	"os"
	"os/exec"
	"testing"
)

func TestKinfoProcPrefixLayout(t *testing.T) {
	if got, want := kinfoProcPrefixSize, 116; got != want {
		t.Fatalf("kinfo_proc prefix size: got %d, want %d", got, want)
	}
}

func TestListIncludesCurrentProcess(t *testing.T) {
	processes, err := List()
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range processes {
		if p.PID == os.Getpid() {
			if got, want := p.PPID, os.Getppid(); got != want {
				t.Fatalf("parent PID: got %d, want %d", got, want)
			}
			return
		}
	}
	t.Fatalf("current PID %d was not present in process table", os.Getpid())
}

func TestListIncludesChildProcess(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()

	processes, err := List()
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range processes {
		if p.PID == cmd.Process.Pid {
			if got, want := p.PPID, os.Getpid(); got != want {
				t.Fatalf("parent PID: got %d, want %d", got, want)
			}
			return
		}
	}
	t.Fatalf("child PID %d was not present in process table", cmd.Process.Pid)
}
