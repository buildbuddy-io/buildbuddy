#!/usr/bin/env python3

import collections
import re
import subprocess
import json
import sys
import numpy as np

def main():
  extra_filters = '\n'.join(sys.argv[1:])
  p = subprocess.run(['gcloud', 'logging', 'read', f"""
"High computed CPU"
timestamp>="2024-08-13T13:36:49.895400+00:00"
{extra_filters}
""", '--limit=10000', '--format=json', '--order=asc'], encoding='utf-8', capture_output=True, check=False)
  if p.returncode != 0:
    print(p.stderr)
    exit(p.returncode)

  psi_values = [0, 0.25, 0.5, 1]
  millicpus = collections.defaultdict(list)

  entries = json.loads(p.stdout)
  for entry in entries:
    message = entry.get('jsonPayload', {}).get('message', '')
    m = re.search(r'(\d+) average milli-CPU from (.*?) CPU-millis over (.*?) exec duration minus full-stall durations cpu=(.*?), mem=(.*?), io=(.*?)$', message)
    if not m:
      continue
    [millicpu_raw, usage_raw, exec_duration_raw, cpustall_raw, memstall_raw, iostall_raw] = m.groups()
    computed_millicpu = int(millicpu_raw)
    cpu_millis_used = float(usage_raw)
    exec_duration_s = parse_go_duration(exec_duration_raw)
    cpustall_s = parse_go_duration(cpustall_raw)
    memstall_s = parse_go_duration(memstall_raw)
    iostall_s = parse_go_duration(iostall_raw)

    for psi_amount in [0, 0.25, 0.5, 0.75, 1]:
      millicpu = cpu_millis_used / (exec_duration_s - psi_amount * (cpustall_s + memstall_s + iostall_s))

      # Sanity check: millicpu should match computed_millicpu when psi_amount == 1
      if psi_amount == 1 and abs(1 - millicpu / computed_millicpu) > 0.001:
        print(f'Warning: formula-computed {millicpu} != reported {computed_millicpu}')

      millicpus[psi_amount].append(millicpu)

  for k, v in millicpus.items():
    print(f'millicpu using {k:.2f}*stall_duration:\n{stats(v)}\n')
  print(f'N={len(entries)}')

def stats(numbers):
    if not numbers:
        return {}

    stats = {}
    stats['min'] = np.min(numbers)
    stats['max'] = np.max(numbers)
    stats['average'] = np.mean(numbers)
    stats['p50'] = np.percentile(numbers, 50)
    stats['p90'] = np.percentile(numbers, 90)
    stats['p95'] = np.percentile(numbers, 95)
    stats['p99'] = np.percentile(numbers, 99)

    return '\n'.join([f'- {k}: {v}' for k, v in stats.items()])

def parse_go_duration(duration: str) -> float:
    """
    Parse a Go-style duration string into seconds.

    Supported units:
    - ns: nanoseconds
    - us, µs: microseconds
    - ms: milliseconds
    - s: seconds
    - m: minutes
    - h: hours
    """
    unit_to_seconds = {
        'ns': 1e-9, 'us': 1e-6, 'µs': 1e-6,
        'ms': 1e-3, 's': 1, 'm': 60, 'h': 3600
    }
    pattern = re.compile(r'(\d+\.?\d*)(ns|us|µs|ms|s|m|h)')
    matches = pattern.findall(duration)
    total_seconds = 0
    for value, unit in matches:
        total_seconds += float(value) * unit_to_seconds[unit]
    return total_seconds

if __name__ == "__main__":
  main()