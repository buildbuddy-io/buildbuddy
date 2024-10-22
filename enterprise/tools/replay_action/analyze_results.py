#!/usr/bin/env python3

import collections
import os
import json
import numpy as np

def main():
    results = []

    results_dir = '/tmp/replay/latest'
    for action_hash in os.listdir(results_dir):
        for attempt in os.listdir(results_dir + "/" + action_hash):
            response_json_path = os.path.join(results_dir, action_hash, attempt, "execute_response.json")
            if not os.path.exists(response_json_path):
                continue
            with open(os.path.join(results_dir, action_hash, attempt, 'stdout'), 'r') as f:
                stdout = f.read()
            with open(os.path.join(results_dir, action_hash, attempt, 'stderr'), 'r') as f:
                stderr = f.read()
            with open(response_json_path, 'r') as f:
                response = json.load(f)
            results.append({
                'stdout': stdout,
                'stderr': stderr,
                'response': response,
            })
    
    stats = collections.defaultdict(list)
    for result in results:
        response = result['response']
        action_result = response.get('result', {})
        metadata = action_result.get('executionMetadata', {})
        usage_stats = metadata.get('usageStats', {})
        cpu_pressure = usage_stats.get('cpuPressure', {})
        cpu_some_total_usec = float(cpu_pressure.get('some', {}).get('total', 0))
        cpu_full_total_usec = float(cpu_pressure.get('full', {}).get('total', 0))

        success = int(1 if action_result.get('exitCode', 0) == 0 else 0)

        if success == 0:
            print('stderr of failing action:', result['stderr'])
            print('stdout of failing action:', result['stdout'])

        stats['success'].append(success)
        stats['cpu_pressure.some.total'].append(cpu_some_total_usec / 1e6)
        stats['cpu_pressure.full.total'].append(cpu_full_total_usec / 1e6)
    
    for stat, values in stats.items():
        print('---')
        if stat == 'success':
            print(f'Success rate: {np.mean(values):.3f} ({sum(values)} of {len(values)})')
            continue

        print(stat)
        summary = {
            'avg': np.mean(values),
            'sum': np.sum(values),
            'p50': np.quantile(values, 0.5),
            'p90': np.quantile(values, 0.9),
            'min': np.min(values),
            'max': np.max(values),
        }
        for metric, value in summary.items():
            print(f'{metric:5} {value:10.3f}')
        

if __name__ == "__main__":
    main()