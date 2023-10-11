//go:build windows

package commandutil

import (
	"context"
	"fmt"
	"os/exec"
	"syscall"
	"unsafe"

	putil "github.com/shirou/gopsutil/v3/process"
	"golang.org/x/sys/windows"
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
)

// process struct is a wrapper around exec.Cmd that adds support for killing the
// process tree.

// On Windows, this is done by creating a job object and assigning the
// process to the job object. When the job object is closed, the process
// tree is killed.
type process struct {
	cmd        *exec.Cmd
	terminated chan struct{}

	jobHandle windows.Handle
}

func createJobObjInfo() (uintptr, uint32) {
	extLimitInfo := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{
		BasicLimitInformation: windows.JOBOBJECT_BASIC_LIMIT_INFORMATION{
			LimitFlags: windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
		},
	}
	return uintptr(unsafe.Pointer(&extLimitInfo)), uint32(unsafe.Sizeof(extLimitInfo))
}

// preStart creates a job object and sets the job object info.
func (p *process) preStart() error {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return fmt.Errorf("failed to create job object: %w", err)
	}

	jobObjInfo, jobObjInfoLength := createJobObjInfo()
	if _, err := windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		jobObjInfo,
		jobObjInfoLength); err != nil {
		return fmt.Errorf("failed to set job object info: %w", err)
	}
	p.jobHandle = job

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

// killProcessTree kills the given pid as well as any descendant processes.
//
// For Windows, see
// https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects
// and https://learn.microsoft.com/en-us/windows/win32/procthread/nested-jobs
// for more details.
func (p *process) killProcessTree() error {
	return windows.CloseHandle(p.jobHandle)
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
