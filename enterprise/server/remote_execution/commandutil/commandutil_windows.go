//go:build windows

package commandutil

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"runtime"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/procstats"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sys/windows"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	putil "github.com/shirou/gopsutil/v3/process"
)

const (
	// For a process to be assigned to a job object, it must be created with
	// PROCESS_SET_QUOTA and PROCESS_TERMINATE access rights. Also add
	// PROCESS_QUERY_LIMITED_INFORMATION to allow the process to be queried
	// for its exit code.
	//
	// References:
	// - https://learn.microsoft.com/en-us/windows/win32/api/jobapi2/nf-jobapi2-assignprocesstojobobject#parameters
	// - https://learn.microsoft.com/en-us/windows/win32/procthread/process-security-and-access-rights
	processWithJobObjPerm = windows.PROCESS_SET_QUOTA | windows.PROCESS_TERMINATE | windows.PROCESS_QUERY_LIMITED_INFORMATION

	// Exit code given to TerminateJobObject when killing the process tree.
	// Go converts the process's DWORD exit code to int without sign
	// extension, so this value reads back from ProcessState.ExitCode as
	// 4294967295 on 64-bit builds and -1 only on windows_386. Compare
	// uint32(exitCode) against this constant instead of checking for
	// KilledExitCode.
	// https://learn.microsoft.com/en-us/windows/win32/api/jobapi2/nf-jobapi2-terminatejobobject#parameters
	windowsKilledExitCode = ^uint32(0)
)

// process struct is a wrapper around exec.Cmd that adds support for killing the
// process tree.

// On Windows, this is done by creating a job object and assigning the
// process to the job object. When the job object is closed, the process
// tree is killed.
type process struct {
	cmd        *exec.Cmd
	terminated chan struct{}

	jobMu     sync.Mutex
	jobHandle windows.Handle
	killed    bool
}

type processKilledError struct {
	err error
}

func (e *processKilledError) Error() string {
	return e.err.Error()
}

func (e *processKilledError) Unwrap() error {
	return e.err
}

// jobObjectBasicAccountingInformation mirrors the Windows
// JOBOBJECT_BASIC_ACCOUNTING_INFORMATION structure. CPU times are expressed in
// 100-nanosecond ticks.
// https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-jobobject_basic_accounting_information
type jobObjectBasicAccountingInformation struct {
	TotalUserTime             int64
	TotalKernelTime           int64
	ThisPeriodTotalUserTime   int64
	ThisPeriodTotalKernelTime int64
	TotalPageFaultCount       uint32
	TotalProcesses            uint32
	ActiveProcesses           uint32
	TotalTerminatedProcesses  uint32
}

// preStart creates a job object and sets the job object info.
func (p *process) preStart() error {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return fmt.Errorf("failed to create job object: %w", err)
	}

	jobObjInfo := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{
		BasicLimitInformation: windows.JOBOBJECT_BASIC_LIMIT_INFORMATION{
			LimitFlags: windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
		},
	}
	// SetInformationJobObject takes uintptr, so pin the buffer across the
	// wrapper call to keep it alive and prevent stack movement.
	var pin runtime.Pinner
	pin.Pin(&jobObjInfo)
	defer pin.Unpin()
	if _, err := windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&jobObjInfo)),
		uint32(unsafe.Sizeof(jobObjInfo))); err != nil {
		_ = windows.CloseHandle(job)
		return fmt.Errorf("failed to set job object info: %w", err)
	}
	p.jobMu.Lock()
	p.jobHandle = job
	p.jobMu.Unlock()

	return nil
}

// postStart assigns the process to the job object.
func (p *process) postStart() error {
	pid := uint32(p.cmd.Process.Pid)

	// Assign process to job object.
	processHandle, err := windows.OpenProcess(processWithJobObjPerm, false, pid)
	if err != nil {
		return fmt.Errorf("failed to open process: %w", err)
	}
	defer windows.CloseHandle(processHandle)
	if err := windows.AssignProcessToJobObject(p.jobHandle, processHandle); err != nil {
		return fmt.Errorf("failed to assign process to job object: %w", err)
	}

	// Resume process.
	proc, err := putil.NewProcess(int32(p.cmd.Process.Pid))
	if err != nil {
		return fmt.Errorf("failed to get process: %w", err)
	}
	return proc.ResumeWithContext(context.TODO())
}

func (p *process) monitorUsage(listener procstats.Listener) *repb.UsageStats {
	return procstats.Monitor(p.cmd.Process.Pid, listener, p.terminated)
}

func (p *process) jobAccounting() (*jobObjectBasicAccountingInformation, error) {
	p.jobMu.Lock()
	defer p.jobMu.Unlock()
	if p.jobHandle == 0 {
		return nil, fmt.Errorf("job object is closed")
	}
	accounting := &jobObjectBasicAccountingInformation{}
	if err := windows.QueryInformationJobObject(
		p.jobHandle,
		windows.JobObjectBasicAccountingInformation,
		uintptr(unsafe.Pointer(accounting)),
		uint32(unsafe.Sizeof(*accounting)),
		nil,
	); err != nil {
		return nil, fmt.Errorf("query job object accounting information: %w", err)
	}
	return accounting, nil
}

// Wait until the job reports no active processes before taking the final
// cumulative accounting sample. Windows may finish process teardown and
// signal individual process handles after the active count reaches zero.
func (p *process) waitForJobInactive() error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		accounting, err := p.jobAccounting()
		if err != nil {
			return err
		}
		if accounting.ActiveProcesses == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %d job processes to become inactive", accounting.ActiveProcesses)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (p *process) cleanup() error {
	p.jobMu.Lock()
	defer p.jobMu.Unlock()
	if p.jobHandle == 0 {
		return nil
	}
	if err := windows.CloseHandle(p.jobHandle); err != nil {
		return err
	}
	p.jobHandle = 0
	return nil
}

func (p *process) withProcessHandle(f func(windows.Handle) error) error {
	var err error
	if handleErr := p.cmd.Process.WithHandle(func(handle uintptr) {
		err = f(windows.Handle(handle))
	}); handleErr != nil {
		return handleErr
	}
	return err
}

func (p *process) killRootProcess() error {
	// Use the existing handle so startup recovery does not need to allocate
	// another handle when setup failed due to resource pressure.
	return p.withProcessHandle(func(handle windows.Handle) error {
		return windows.TerminateProcess(handle, 1)
	})
}

func isKilledExitCode(exitCode int, err error) bool {
	var killedErr *processKilledError
	return uint32(exitCode) == windowsKilledExitCode && errors.As(err, &killedErr)
}

func (p *process) wait() (*espb.Rusage, error) {
	defer close(p.terminated)
	if err := p.withProcessHandle(func(handle windows.Handle) error {
		_, err := windows.WaitForSingleObject(handle, windows.INFINITE)
		return err
	}); err != nil {
		_ = p.cleanup()
		_ = p.killRootProcess()
		_ = p.cmd.Wait()
		return nil, fmt.Errorf("wait for root process: %w", err)
	}
	// Cmd.Wait also waits for stdout/stderr copying. Terminate descendants
	// holding inherited pipes before waiting for EOF.
	p.jobMu.Lock()
	killed := p.killed
	var cleanupErr error
	if p.jobHandle != 0 {
		cleanupErr = windows.TerminateJobObject(p.jobHandle, windowsKilledExitCode)
	}
	p.jobMu.Unlock()
	if cleanupErr == nil {
		cleanupErr = p.waitForJobInactive()
	}
	if cleanupErr != nil {
		// Closing the job is the fallback if explicit termination or the
		// accounting query failed. Do not report successful cleanup.
		_ = p.cleanup()
	}
	err := p.cmd.Wait()
	if cleanupErr != nil {
		return nil, fmt.Errorf("wait for job processes: %w", cleanupErr)
	}
	if killed && err != nil {
		err = &processKilledError{err: err}
	}
	return nil, err
}

func (p *process) signal(sig syscall.Signal) error {
	return status.UnimplementedError("not implemented")
}

// killProcessTree kills the given pid as well as any descendant processes.
//
// For Windows, see
// https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects
// and https://learn.microsoft.com/en-us/windows/win32/procthread/nested-jobs
// for more details.
func (p *process) killProcessTree() error {
	p.jobMu.Lock()
	defer p.jobMu.Unlock()
	if p.jobHandle == 0 {
		return nil
	}
	// Keep the job handle open so wait() can check for job inactivity.
	if err := windows.TerminateJobObject(p.jobHandle, windowsKilledExitCode); err != nil {
		return err
	}
	p.killed = true
	return nil
}

// SetCredential adds credentials to the cmd by resolving a "USER[:GROUP]" string
// to a credential with both uid and gid populated. Both numeric IDs and non-numeric
// names can be  specified for either USER or GROUP. If no group is specified, then
// the user's primary group is used. This is a no-op on Windows.
//
// NOTE: This function does not authenticate that the user is part of the
// specified group.
func SetCredential(cmd *exec.Cmd, spec string) error {
	return nil
}

func getDefaultSysProcAttr() *syscall.SysProcAttr {
	// Ensure that we start the process in suspended state
	// so that we can assign it to the job object before it
	// is resumed in postStart().
	return &syscall.SysProcAttr{
		CreationFlags: windows.CREATE_NEW_PROCESS_GROUP | windows.CREATE_SUSPENDED,
	}
}
