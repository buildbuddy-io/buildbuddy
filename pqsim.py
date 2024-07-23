import json
import re
import sys

avail_mcpu = 29300
avail_mem = 102_000_000_000

tasks = {}
scheduled = set()
q = []

logs = json.load(sys.stdin)

for entry in logs:
    # Schedule
    payload = entry.get('jsonPayload', {})
    message = payload.get('message', '') or entry.get('textPayload', '')
    execution_id = payload.get('execution_id', '')

    if 'Scheduling task of size' in message:
        m = re.search(r'milli_cpu=(\d+), memory_bytes=(\d+)', message)
        task = {
            'execution_id': execution_id,
            'mcpu': int(m.group(1)),
            'mem': int(m.group(2)),
        }
        tasks[execution_id] = task

        i = 0
        while i < len(q):
            if q[i] == execution_id:
                q = q[:i] + q[i+1:]
                break
            i += 1
        scheduled.add(execution_id)
        avail_mcpu -= tasks[execution_id]['mcpu']
        avail_mem -= tasks[execution_id]['mem']
        continue

    if 'Added task' in message:
        m = re.search(r'estimated_memory_bytes:(\d+) estimated_milli_cpu:(\d+)', message)
        task = {
            'execution_id': execution_id,
            'mem': int(m.group(1)),
            'mcpu': int(m.group(2)),
        }
        tasks[execution_id] = task

        if execution_id in scheduled:
            continue
        q.append(execution_id)
        continue

    if 'interrupt' in message:
        # Not interested in any events when getting the shutdown signal
        print('SHUTDOWN')
        break

    if 'Could not claim task' in message or 'Close()' in message:
        avail_mem += tasks[execution_id]['mem']
        avail_mcpu += tasks[execution_id]['mcpu']
        del tasks[execution_id]
        continue

def fmt_commas(num):
    out = ''
    while num > 1000:
        out = f',{num % 1000:03d}' + out
        num = int(num / 1000)
    out = f'{num}' + out
    return out


print(f'avail_mem={fmt_commas(avail_mem)}')
print(f'avail_mcpu={fmt_commas(avail_mcpu)}')

for (i, execution_id) in enumerate(q):
    if tasks[execution_id]['mem'] > avail_mem or tasks[execution_id]['mcpu'] > avail_mcpu:
        print(f'queue[{i}] is unschedulable')
        print(tasks[execution_id])

    # Complete
