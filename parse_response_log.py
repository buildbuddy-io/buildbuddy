#!/usr/bin/env python3

import datetime
import json
import sys
from dateutil import parser

def main():
    for line in sys.stdin:
        if not line.strip():
            continue
        response = json.loads(line)
        result = response['result']
        md = result['executionMetadata']
        stats = md['usageStats']

        exec_start = md['executionStartTimestamp']
        exec_end = md['executionCompletedTimestamp']

        exec_start_s = parser.isoparse(exec_start).timestamp()
        exec_end_s = parser.isoparse(exec_end).timestamp()
        exec_duration_s = exec_end_s - exec_start_s

        stall = lambda res, typ: int(stats.get(res+'Pressure', {}).get(typ, {}).get('total', 0)) / 1e6
        resources = ['cpu', 'memory', 'io']

        cpu_stall_s, mem_stall_s, io_stall_s = [stall(res, 'full') for res in resources]
        cpu_some_stall_s, mem_some_stall_s, io_some_stall_s = [stall(res, 'some') for res in resources]

        total_stall_s = cpu_stall_s + mem_stall_s + io_stall_s
        total_some_stall_s = cpu_some_stall_s + mem_some_stall_s + io_some_stall_s
        active_duration_s = exec_duration_s - total_stall_s

        cpu_millis = int(stats.get('cpuNanos', 0)) / 1e6
        mcpu = cpu_millis / active_duration_s

        stall_frac = total_stall_s / exec_duration_s
        full_stall_color = stall_color(stall_frac)


        exec_start_ts = datetime.datetime.fromtimestamp(exec_start_s).strftime('%m-%d %H:%M:%S.%f')
        print(f'{exec_start_ts} {cpu_millis:.2f} ms\t{exec_duration_s:.2f}s exec\t{full_stall_color}{total_stall_s:.2f}s stall_f ({stall_frac*100:.0f}%)\x1b[m\t{mcpu:.0f}m avg\tstall_s {total_some_stall_s:.2f}s\tc:{cpu_stall_s:.2f} m:{mem_stall_s:.2f} i:{io_stall_s:.2f}')


def stall_color(value):
    if value < 0.05:
        return '\x1b[32m'
    if value < 0.15:
        return '\x1b[33m'
    if value > 1:
        return '\x1b[37;101m'
    return '\x1b[31m'

if __name__ == "__main__":
    main()