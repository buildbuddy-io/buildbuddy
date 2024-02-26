//go:build (darwin || linux) && !android && !ios

package commandutil

import (
	"os/exec"
	"os/user"
	"strconv"
	"strings"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
)

type process struct {
	cmd        *exec.Cmd
	terminated chan struct{}
}

func (p *process) preStart() error {
	return nil
}

func (p *process) postStart() error {
	return nil
}

func (p *process) wait() (*espb.Rusage, error) {
	defer close(p.terminated)
	err := p.cmd.Wait()
	if p.cmd.ProcessState == nil {
		return nil, err
	}
	rusage, ok := p.cmd.ProcessState.SysUsage().(*syscall.Rusage)
	if !ok {
		return nil, err
	}
	return &espb.Rusage{
		UserCpuTimeUsec:            rusage.Utime.Nano() / 1e3,
		SysCpuTimeUsec:             rusage.Stime.Nano() / 1e3,
		MaxResidentSetSizeBytes:    rusage.Maxrss,
		PageReclaims:               rusage.Minflt,
		PageFaults:                 rusage.Majflt,
		Swaps:                      rusage.Nswap,
		BlockInputOperations:       rusage.Inblock,
		BlockOutputOperations:      rusage.Oublock,
		MessagesSent:               rusage.Msgsnd,
		MessagesReceived:           rusage.Msgrcv,
		SignalsReceived:            rusage.Nsignals,
		VoluntaryContextSwitches:   rusage.Nvcsw,
		InvoluntaryContextSwitches: rusage.Nivcsw,
	}, err
}

// killProcessTree kills the given pid as well as any descendant processes.
//
// It tries to kill as many processes in the tree as possible. If it encounters
// an error along the way, it proceeds to kill subsequent pids in the tree. It
// returns the last error encountered, if any.
func (p *process) killProcessTree() error {
	var lastErr error

	// Run a BFS on the process tree to build up a list of processes to kill.
	// Before listing child processes for each pid, send SIGSTOP to prevent it
	// from spawning new child processes. Otherwise the child process list has a
	// chance to become stale if the pid forks a new child just after we list
	// processes but before we send SIGKILL.

	pidsToExplore := []int{p.cmd.Process.Pid}
	pidsToKill := []int{}
	for len(pidsToExplore) > 0 {
		pid := pidsToExplore[0]
		pidsToExplore = pidsToExplore[1:]
		if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
			lastErr = err
			// If we fail to SIGSTOP, proceed anyway; the more we can clean up,
			// the better.
		}
		pidsToKill = append(pidsToKill, pid)

		childPids, err := ChildPids(pid)
		if err != nil {
			lastErr = err
			continue
		}
		pidsToExplore = append(pidsToExplore, childPids...)
	}
	for _, pid := range pidsToKill {
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// SetCredential adds credentials to the cmd by resolving a "USER[:GROUP]" string
// to a credential with both uid and gid populated. Both numeric IDs and non-numeric
// names can be  specified for either USER or GROUP. If no group is specified, then
// the user's primary group is used.
//
// NOTE: This function does not authenticate that the user is part of the
// specified group.
func SetCredential(cmd *exec.Cmd, spec string) error {
	parts := strings.Split(spec, ":")
	if len(parts) == 0 {
		return status.InvalidArgumentError("credential spec is empty: expected USER[:GROUP]")
	}
	if len(parts) > 2 {
		return status.InvalidArgumentError("credential spec had too many parts: expected USER[:GROUP]")
	}
	userSpec := parts[0]
	var u *user.User
	var g *user.Group
	var err error
	if allDigits.MatchString(userSpec) {
		u, err = user.LookupId(userSpec)
		if err != nil {
			return status.InvalidArgumentErrorf("uid lookup failed: %s", err)
		}
	} else {
		u, err = user.Lookup(userSpec)
		if err != nil {
			return status.InvalidArgumentErrorf("user lookup failed: %s", err)
		}
	}

	groupSpec := ""
	if len(parts) > 1 {
		groupSpec = parts[1]
	}
	var groups []uint32
	if groupSpec == "" {
		// If a group isn't explicitly set, use the user's gid as the primary
		// group ID and use all of their group memberships as supplemental
		// group IDs.
		g, err = user.LookupGroupId(u.Gid)
		if err != nil {
			return status.InvalidArgumentErrorf("gid lookup failed: %s", err)
		}
		gidStrings, err := u.GroupIds()
		if err != nil {
			return status.InternalErrorf("groups lookup failed: %s", err)
		}
		for _, s := range gidStrings {
			gid, err := strconv.Atoi(s)
			if err != nil {
				return status.InternalErrorf("failed to parse groups: %s", err)
			}
			groups = append(groups, uint32(gid))
		}
	} else if allDigits.MatchString(groupSpec) {
		g, err = user.LookupGroupId(groupSpec)
		if err != nil {
			return status.InvalidArgumentErrorf("gid lookup failed: %s", err)
		}
	} else {
		g, err = user.LookupGroup(groupSpec)
		if err != nil {
			return status.InvalidArgumentErrorf("group lookup failed: %s", err)
		}
	}

	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		return status.InternalErrorf("failed to parse uid: %s", err)
	}
	gid, err := strconv.Atoi(g.Gid)
	if err != nil {
		return status.InternalErrorf("failed to parse gid: %s", err)
	}
	cmd.SysProcAttr.Credential = &syscall.Credential{
		Uid:    uint32(uid),
		Gid:    uint32(gid),
		Groups: groups,
	}
	return nil
}

func getDefaultSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}
