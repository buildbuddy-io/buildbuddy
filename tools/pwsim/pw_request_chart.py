#!/usr/bin/env python3

import json
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from collections import defaultdict

# Configuration
BUCKET_MINUTES = 1

# Read the data
data = []
with open('/tmp/executions.jsonl', 'r') as f:
    for line in f:
        data.append(json.loads(line.strip()))

# Group by persistent_worker_key and minute
worker_minute_counts = defaultdict(lambda: defaultdict(int))
# Track action_mnemonics for each persistent_worker_key
worker_action_mnemonics = defaultdict(set)

for record in data:
    pw_key = record.get('persistent_worker_key', '')
    action_mnemonic = record.get('action_mnemonic', '')

    # Track the action_mnemonic for this worker key
    if pw_key and action_mnemonic:
        worker_action_mnemonics[pw_key].add(action_mnemonic)

    # Use queued_timestamp_usec (microseconds) and convert to minutes
    timestamp_usec = record.get('queued_timestamp_usec')
    if timestamp_usec:
        timestamp_sec = int(timestamp_usec) / 1_000_000
        dt = datetime.fromtimestamp(timestamp_sec)
        # Round down to the bucket interval
        minutes_since_epoch = dt.minute + dt.hour * 60
        bucket_offset = (minutes_since_epoch // BUCKET_MINUTES) * BUCKET_MINUTES
        bucket = dt.replace(hour=bucket_offset // 60, minute=bucket_offset % 60, second=0, microsecond=0)
        worker_minute_counts[pw_key][bucket] += 1
        # Initialize previous and next bucket to 0
        prev_bucket = bucket - timedelta(minutes=BUCKET_MINUTES)
        next_bucket = bucket + timedelta(minutes=BUCKET_MINUTES)
        if prev_bucket not in worker_minute_counts[pw_key]:
            worker_minute_counts[pw_key][prev_bucket] = 0
        if next_bucket not in worker_minute_counts[pw_key]:
            worker_minute_counts[pw_key][next_bucket] = 0

# Sort keys by total frequency (descending)
key_totals = []
for pw_key in worker_minute_counts.keys():
    if not pw_key:  # Skip empty keys
        continue
    total = sum(worker_minute_counts[pw_key].values())
    key_totals.append((pw_key, total))

key_totals.sort(key=lambda x: x[1], reverse=True)
sorted_keys = [k for k, _ in key_totals]

# Define distinct colors for the most frequent keys
distinct_colors = [
    '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
    '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
    '#aec7e8', '#ffbb78', '#98df8a', '#ff9896', '#c5b0d5',
    '#c49c94', '#f7b6d2', '#c7c7c7', '#dbdb8d', '#9edae5'
]

# Plot all persistent_worker_keys on the same chart
plt.figure(figsize=(14, 8))

for idx, pw_key in enumerate(sorted_keys):
    minute_counts = worker_minute_counts[pw_key]

    # Sort by time
    times = sorted(minute_counts.keys())
    counts = [minute_counts[t] for t in times]

    # Build label with action_mnemonics prefix
    mnemonics = sorted(worker_action_mnemonics[pw_key])
    mnemonic_str = ','.join(mnemonics) if mnemonics else ''
    key_str = pw_key[:50] + '...' if len(pw_key) > 50 else pw_key
    label = f"{mnemonic_str} {key_str}" if mnemonic_str else key_str

    # Assign color round-robin from distinct colors
    color = distinct_colors[idx % len(distinct_colors)]
    plt.plot(times, counts, marker='o', markersize=2, linewidth=1, label=label, color=color)

plt.xlabel('Time')
plt.ylabel('Request Frequency')
plt.title('Request Frequency by Persistent Worker Key')
plt.xticks(rotation=45)
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.grid(True, alpha=0.3)

# Save the chart
plt.savefig('/tmp/pw_chart_all_keys.png', dpi=150, bbox_inches='tight')
print(f'Saved chart to /tmp/pw_chart_all_keys.png')
plt.close()

print(f'\nProcessed {len(data)} records')
print(f'Found {len(worker_minute_counts)} unique persistent_worker_keys')
