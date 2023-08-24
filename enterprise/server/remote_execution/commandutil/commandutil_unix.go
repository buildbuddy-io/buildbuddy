//go:build (darwin || linux) && !android && !ios

package commandutil

import (
	"os/exec"
	"os/user"
	"strconv"
	"strings"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
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

	groupSpec := u.Gid
	if len(parts) > 1 {
		groupSpec = parts[1]
	}
	if allDigits.MatchString(groupSpec) {
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

	cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}
	return nil
}

func getDefaultSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}
