//go:build openbsd

package commandutil

import "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/processlist"

// ChildPids returns all direct child PIDs of the process identified by pid.
func ChildPids(pid int) ([]int, error) {
	processes, err := processlist.List()
	if err != nil {
		return nil, err
	}

	var out []int
	for _, p := range processes {
		if p.PPID == pid {
			out = append(out, p.PID)
		}
	}
	return out, nil
}
