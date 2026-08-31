//go:build openbsd

package procstats

import "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/processlist"

func listProcesses() ([]process, error) {
	processes, err := processlist.List()
	if err != nil {
		return nil, err
	}

	result := make([]process, 0, len(processes))
	for _, p := range processes {
		result = append(result, process{pid: p.PID, ppid: p.PPID})
	}
	return result, nil
}
