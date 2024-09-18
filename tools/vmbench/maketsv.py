#!/usr/bin/env python3
import argparse
import json
import sys

def main():
    print('\t'.join([
        'ISOLATION',
        'OVERPROVISION',
        'TASK_CPU',
        'PARALLEL_TASKS',
        'TRIAL',
        'TASK_INDEX',
        'TASK_EXEC_DURATION',
    ]))
    for line in sys.stdin:
        result = json.loads(line)
        for (i, exec_duration) in enumerate(result["exec_durations"]):
            print('\t'.join([str(v) for v in [
                result['isolation'],
                result['overprovision'],
                result['task_cpu'],
                result['parallel_tasks'],
                result['trial'],
                i,
                exec_duration,
            ]]))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    # parser.add_argument("-v", "--verbose", help="Run in verbose mode", action="store_true")
    args = parser.parse_args()
    main(**vars(args))

