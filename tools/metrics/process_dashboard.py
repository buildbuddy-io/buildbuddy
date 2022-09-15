#!/usr/bin/env python3
"""Prepares the input Grafana dashboard JSON for storage in Git.

Its purpose is to reduce unnecessary diffs, normalizing the dashboard and
reverting changes which are most likely unintended.
"""

import collections
import json
import sys

DASHBOARD_REFRESH_INTERVAL = "1m"


def main():
    dashboard = json.load(sys.stdin)["dashboard"]

    # Remove volatile versioning info since we use Git for versioning.
    if "version" in dashboard:
        del dashboard["version"]
    if "iteration" in dashboard:
        del dashboard["iteration"]

    # Sometimes null values creep into the dashboard JSON and cause unnecessary
    # diffs. Strip these out since the resulting dashboard is equivalent.
    #
    # Related: https://github.com/grafana/grafana/issues/54126
    remove_dict_none_values(dashboard)

    dashboard = with_ordered_dicts(dashboard)

    # Grafana updates the refresh interval in the JSON when changing it in the
    # UI. Hard-code it here to prevent accidental updates.
    dashboard["refresh"] = DASHBOARD_REFRESH_INTERVAL

    # Grafana updates "collapsed" state when expanding panels in the UI. Ensure
    # all panels are collapsed to prevent accidental updates.
    for panel in dashboard["panels"]:
        panel["collapsed"] = True

    # Note: ensure_ascii=False keeps strings like "µs" as-is.
    json.dump(dashboard, sys.stdout, indent=2, ensure_ascii=False)


def with_ordered_dicts(obj):
    if isinstance(obj, list):
        return [with_ordered_dicts(item) for item in obj]
    elif isinstance(obj, dict):
        new_items = [(k, with_ordered_dicts(v)) for (k, v) in obj.items()]
        return collections.OrderedDict(sorted(new_items))
    else:
        return obj


def remove_dict_none_values(obj):
    if isinstance(obj, list):
        for item in obj:
            remove_dict_none_values(item)
    elif isinstance(obj, dict):
        keys_to_remove = []
        for k, v in obj.items():
            if v is None:
                keys_to_remove.append(k)
            else:
                remove_dict_none_values(v)
        for k in keys_to_remove:
            del obj[k]


if __name__ == "__main__":
    main()
