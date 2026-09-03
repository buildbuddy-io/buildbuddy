//go:build !openbsd

package commandutil

import ps "github.com/mitchellh/go-ps"

// ChildPids returns all direct child PIDs of the process identified by pid.
func ChildPids(pid int) ([]int, error) {
	procs, err := ps.Processes()
	if err != nil {
		return nil, err
	}
	var out []int
	for _, proc := range procs {
		if proc.PPid() != pid {
			continue
		}
		out = append(out, proc.Pid())
	}
	return out, nil
}
