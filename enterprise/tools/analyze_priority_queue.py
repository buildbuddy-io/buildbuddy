#!/usr/bin/env python3

import datetime
import sys
import json
import collections
import re
# Accepts GCP logs on stdin, and prints out details about the priority queue's
# state over time. Specifically it tries to detect when the queue is clogged up
# due to executions taking too long.
#
# Use this logs query:
"""
"executor-abc-xyz" -- executor pod name
sourceLocation.file="priority_task_scheduler.go" OR "Close() called with err:"
"""

def fatal(message):
    print(message, file=sys.stderr)
    sys.exit(1)

def parse_timestamp(timestamp):
    # Parse timestamp (like '2025-02-19T18:29:58.730337943Z') to unix seconds
    if '.' in timestamp:
        date_part, frac_and_zone = timestamp.split('.', 1)
        frac = frac_and_zone.rstrip('Z')
        # Truncate or pad the fractional part to 6 digits for microsecond precision.
        frac = (frac + "000000")[:6]
        formatted_timestamp = f"{date_part}.{frac}Z"
        return datetime.datetime.strptime(formatted_timestamp, '%Y-%m-%dT%H:%M:%S.%fZ').timestamp()

    return datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ').timestamp()

EXECUTOR_MILLI_CPU_CAPACITY = 43000
MMAP_MEMORY_BYTES_CAPACITY = 10000000000
EXECUTOR_MEMORY_BYTES_CAPACITY = 161061273600 - MMAP_MEMORY_BYTES_CAPACITY

if __name__ == "__main__":
    logs = json.load(sys.stdin)
    # Sort by timestamp first, as a sanity check.
    logs.sort(key=lambda x: x['timestamp'])

    queue_length = 0
    queued_milli_cpu = 0
    queued_memory_bytes = 0
    scheduled_milli_cpu = 0
    scheduled_memory_bytes = 0
    scheduled_tasks = {}

    last_dequeue_timestamp = None
    last_finish_timestamp = None
    estimated_memory_bytes_by_task_id = {}
    estimated_milli_cpu_by_task_id = {}
    group_id_by_task_id = {}

    group_queues = collections.defaultdict(list)

    for entry in logs:
        timestamp = parse_timestamp(entry['timestamp'])
        message = entry.get('jsonPayload', {}).get('message', '')
        task_id = entry.get('jsonPayload', {}).get('execution_id', '')
        pod_name = entry.get('resource', {}).get('labels', {}).get('pod_name', '')

        # Parse the different log events based on the message.

        # QUEUE event - starts with "Added task"
        # Parsed fields: estimated_memory_bytes, estimated_milli_cpu (parsed from task_size)
        # Example message:
        # Added task task_id:"/uploads/c65d230b-ee06-483b-bc7c-291711488c05/blobs/blake3/81ded2513be9224a2b7c417bb4bd0e1309b6d1a34e088c4719cf36e5ffeac8a6/399"  task_size:{estimated_memory_bytes:392904704  estimated_milli_cpu:1248}  scheduling_metadata:{task_size:{estimated_memory_bytes:392904704  estimated_milli_cpu:1248}  default_task_size:{estimated_memory_bytes:20000000  estimated_milli_cpu:1000  estimated_free_disk_bytes:100000000}  measured_task_size:{estimated_memory_bytes:392904704  estimated_milli_cpu:1248}  requested_task_size:{}  os:"linux"  arch:"amd64"  executor_group_id:"GR2285257076483062166"  task_group_id:"GR8308021038238701728"  cgroup_settings:{cpu_weight:49  cpu_quota_limit_usec:3000000  cpu_quota_period_usec:100000  pids_max:2048  memory_oom_group:true}}  trace_metadata:{entries:{key:"traceparent"  value:"00-6200be6dee97a710f2642f77d335f9fa-1d6aec1142f822f3-00"}} to pq.
        event = None
        estimated_memory_bytes = None
        estimated_milli_cpu = None
        task_group_id = None
        if message.startswith('Added task'):
            event = 'QUEUE'
            m = re.match(r'Added task.*?task_size:\{estimated_memory_bytes:(\d+)\s+estimated_milli_cpu:(\d+)', message)
            if m:
                estimated_memory_bytes = int(m.group(1))
                estimated_milli_cpu = int(m.group(2))
                estimated_memory_bytes_by_task_id[task_id] = estimated_memory_bytes
                estimated_milli_cpu_by_task_id[task_id] = estimated_milli_cpu
            else:
                fatal(f'Failed to parse task size from message: {message}')
            m = re.search(r'task_group_id:"(GR[0-9]+)"', message)
            if m:
                task_group_id = m.group(1)
                group_id_by_task_id[task_id] = task_group_id
            group_queues[task_group_id].append(task_id)

        # ATTEMPT event
        # Parsed fields: estimated_memory_bytes, estimated_milli_cpu (parsed from size)
        # Example message:
        # Scheduling task of size milli_cpu=250, memory_bytes=6000000
        elif message.startswith('Scheduling task of size'):
            event = 'ATTEMPT'
            m = re.match(r'Scheduling task of size milli_cpu=(\d+), memory_bytes=(\d+)', message)
            if m:
                estimated_milli_cpu = int(m.group(1))
                estimated_memory_bytes = int(m.group(2))
            else:
                fatal(f'Failed to parse task size from message: {message}')
            task_group_id = group_id_by_task_id[task_id]
            # group_queue = group_queues[task_group_id]
            # if not group_queue or group_queue[0] != task_id:
            #     index = group_queue.index(task_id)
            #     print(f"Task {task_id} is not at the head of its group queue: task_group_id={task_group_id}, index={index}")
            #     print(group_queues)
            #     exit(1)
            group_queues[task_group_id].remove(task_id)
        # CLAIM_FAILED event
        # Parsed fields: (none)
        # Example message:
        # Could not claim task "/uploads/807e3eb0-1941-40a5-a0d8-c3386dfdd8cf/blobs/d86f28b32a6177806dcfbe570d88f14238f1cab39addfa455c6926bb76791d97/261": rpc error: code = NotFound desc = task already claimed
        elif message.startswith('Could not claim task'):
            event = 'CLAIM_FAILED'
        # FINISH event
        # Parsed fields: (none)
        # Example message:
        # TaskLeaser "/uploads/db6f8390-3300-41d1-807f-99dc9b0a4651/blobs/blake3/2dd8282a6e35b7edda70a9abb09f2cff8d6f8e6fa1358074098d2c1363e993d8/393" Close() called with err: <nil>
        elif message.startswith('TaskLeaser'):
            event = 'FINISH'

        if event is None:
            fatal(f'Unknown event: {message}')

        if event == 'QUEUE':
            queue_length += 1
        elif event == 'ATTEMPT':
            queue_length -= 1
            if last_dequeue_timestamp is not None:
                delta = timestamp - last_dequeue_timestamp
                if delta > 100:
                    print(f'Queue stalled: {delta} seconds since last dequeue')
            last_dequeue_timestamp = timestamp
            scheduled_milli_cpu += estimated_milli_cpu
            scheduled_memory_bytes += estimated_memory_bytes
            scheduled_tasks[task_id] = {
                'task_id': task_id,
                'dequeued_at': timestamp,
                'estimated_milli_cpu': estimated_milli_cpu,
                'estimated_memory_bytes': estimated_memory_bytes,
            }
        elif event == 'CLAIM_FAILED':
            scheduled_milli_cpu -= estimated_milli_cpu_by_task_id[task_id]
            scheduled_memory_bytes -= estimated_memory_bytes_by_task_id[task_id]
            del scheduled_tasks[task_id]
        elif event == 'FINISH':
            scheduled_milli_cpu -= estimated_milli_cpu_by_task_id[task_id]
            scheduled_memory_bytes -= estimated_memory_bytes_by_task_id[task_id]
            last_finish_timestamp = timestamp
            del scheduled_tasks[task_id]

        print(f'{timestamp:.6f}\t{event:<12} Task: mcpu: {estimated_milli_cpu_by_task_id[task_id]:>6}m mem: {estimated_memory_bytes_by_task_id[task_id]/1e6:.2f}MB Executor:\tqueued: {queue_length:>4}\tactive: {len(scheduled_tasks):>4}\tmcpu: {scheduled_milli_cpu:>6}\tmem: {scheduled_memory_bytes/1e6:.2f}MB')

        time_since_last_finish = timestamp - last_finish_timestamp if last_finish_timestamp is not None else None

        if queue_length > 300 and (time_since_last_finish is None or time_since_last_finish > 30):
            print('---')
            print(f"Queue blocked by long-running tasks:")
            print(f"- Pod: {pod_name}")
            print("- Current timestamp: ", timestamp)
            print("- Last task finished at: ", last_finish_timestamp)
            print(f"- Time since last task finished: {time_since_last_finish:.3f}s")
            print("- Queue length: ", queue_length)
            print("- Scheduled tasks: ", len(scheduled_tasks))
            print("- Scheduled memory: ", scheduled_memory_bytes/1e6, "MB")
            print("- Scheduled cpu: ", scheduled_milli_cpu, "m")
            print("- Active tasks:")
            for task in scheduled_tasks.values():
                task_execution_time = timestamp - task['dequeued_at']
                print(f"  - {task['task_id']} (group_id={group_id_by_task_id[task['task_id']]}, mcpu={task['estimated_milli_cpu']}, mem={task['estimated_memory_bytes']/1e6:.2f}MB, running for {task_execution_time:.3f}s)")
            exit()

        # print(timestamp, event, task_id, estimated_memory_bytes, estimated_milli_cpu)

        prev_timestamp = timestamp
