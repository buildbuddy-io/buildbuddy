#!/usr/bin/env python3

import re
import sys
import json

def main():
    entries = json.load(sys.stdin)
    worst_score = 0
    worst_entry = None
    for entry in entries:
        message = entry.get('jsonPayload', {}).get('message', '')
        m = re.match(r'Computed CPU usage: (.*?) average milli-CPU from (.*?) CPU-millis over (.*?) exec duration minus full-stall durations cpu=(.*?), mem=(.*?), io=(.*)$', message)
        [mcpu_str, cpu_millis_str, exec_dur_str, cpu_stall_str, mem_stall_str, io_stall_str] = m.groups()
        mcpu = int(mcpu_str)
        cpu_millis = float(cpu_millis_str)
        exec_dur = parse_go_duration(exec_dur_str)
        cpu_stall = parse_go_duration(cpu_stall_str)
        mem_stall = parse_go_duration(mem_stall_str)
        io_stall = parse_go_duration(io_stall_str)

        total_stall_dur = cpu_stall + mem_stall + io_stall

        # Higher stall duration relative to exec duration =
        # more likely to reproduce
        score = total_stall_dur / exec_dur
        # Lower durations get more points - can reproduce more quickly
        if score > 1:
            score += 1 / exec_dur

        if score > worst_score:
            worst_score = score
            worst_entry = entry

    print('#', worst_entry['jsonPayload']['message'], file=sys.stderr)
    print("export EXECUTION_ID=" + worst_entry['jsonPayload']['execution_id'])
    print("export GROUP_ID=" + worst_entry['jsonPayload']['group_id'])


def parse_go_duration(duration: str) -> float:
    pattern = re.compile(r'(?P<value>\d+(\.\d+)?)(?P<unit>[a-z]+)')
    units_in_seconds = {
        'ns': 1e-9,    # nanoseconds
        'us': 1e-6,    # microseconds
        'µs': 1e-6,    # microseconds (alternative symbol)
        'ms': 1e-3,    # milliseconds
        's': 1,        # seconds
        'm': 60,       # minutes
        'h': 3600      # hours
    }
    total_seconds = 0.0
    matches = pattern.finditer(duration)
    for match in matches:
        value = float(match.group('value'))
        unit = match.group('unit')
        if unit not in units_in_seconds:
            raise ValueError(f"Invalid unit: {unit}")
        total_seconds += value * units_in_seconds[unit]
    return total_seconds

if __name__ == "__main__":
    main()

