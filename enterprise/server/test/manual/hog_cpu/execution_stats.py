import collections
import dateutil.parser
import json
import sys
import numpy as np

def parse_timestamp(s):
    return dateutil.parser.isoparse(s).timestamp()

executions = []
for line in sys.stdin:
    if not line.strip():
        continue
    try:
        executions.append(json.loads(line))
    except:
        print(line)
        raise
    

records = []
for execution in executions:
    md = execution.get("result", {}).get("executionMetadata", {})
    exec_start = parse_timestamp(md["workerStartTimestamp"])
    exec_end = parse_timestamp(md["workerCompletedTimestamp"])
    cpu_pressure = md.get("usageStats", {}).get("cpuPressure", {})
    records.append({
        "exec duration, s": exec_end - exec_start,
        # "cpu some-stall, ms": float(cpu_pressure.get("some", {}).get("total", 0)) / 1000,
        "cpu full-stall, ms": float(cpu_pressure.get("full", {}).get("total", 0)) / 1000,
    })

record_stats = collections.defaultdict(list)
for record in records:
    for k, v in record.items():
        record_stats[k].append(v)

for stat, vals in record_stats.items():
    print("---", stat, "---")
    stats = {
      "N": len(vals),

      "Mean": np.mean(vals),
      "Max": np.max(vals),

      "p25": np.quantile(vals, 0.25),
      "p50": np.quantile(vals, 0.5),
      "p75": np.quantile(vals, 0.75),
      "p90": np.quantile(vals, 0.9),
      "p95": np.quantile(vals, 0.95),
      "p99": np.quantile(vals, 0.99),
    }
    for k, v in stats.items():
        print(f"{k}\t{v:12.3f}")