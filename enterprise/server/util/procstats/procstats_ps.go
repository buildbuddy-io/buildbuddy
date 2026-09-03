//go:build !openbsd

package procstats

import ps "github.com/mitchellh/go-ps"

func listProcesses() ([]process, error) {
	procs, err := ps.Processes()
	if err != nil {
		return nil, err
	}
	result := make([]process, 0, len(procs))
	for _, p := range procs {
		result = append(result, process{pid: p.Pid(), ppid: p.PPid()})
	}
	return result, nil
}
