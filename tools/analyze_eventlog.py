#!/usr/bin/env python3

import json
import collections


def readlines(path):
    with open(path, "r") as f:
        return f.readlines()


def main():
    lines = readlines("/tmp/bb_events.log")
    event_count = collections.Counter()
    # path_counts = collections.Counter()
    for line in lines:
        if line.startswith('{"'):
            line = line.strip()
            try:
                entry = json.loads(line)
            except:
                print(f"INVALID JSON: {line}")
                continue
            event_count[entry["event"]] += 1
    for (event, count) in event_count.most_common():
        print(f"{count} {event}")
    # for (path, count) in path_counts.most_common():


if __name__ == "__main__":
    main()