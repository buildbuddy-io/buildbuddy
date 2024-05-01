#!/usr/bin/env python3
"""Prepares the input Grafana dashboard JSON for storage in Git.

Its purpose is to reduce unnecessary diffs, normalizing the dashboard and
reverting changes which are most likely unintended.
"""

import argparse
import collections
import hashlib
import json
import sys


ARGS = argparse.ArgumentParser()
ARGS.add_argument("--name", default="", help="Dashboard file name")
ARGS.add_argument("--uid", default="", help="Force dashboard UID to this value")
ARGS.add_argument(
    "--data_source_uid",
    default="prom",
    help="Set all prometheus data sources to this data source UID (either 'prom' or 'vm')",
)

DASHBOARD_REFRESH_INTERVAL = "1m"


def main(args):
    dashboard = json.load(sys.stdin)

    # Remove volatile versioning info since we use Git for versioning.
    if "version" in dashboard:
        del dashboard["version"]
    if "iteration" in dashboard:
        del dashboard["iteration"]
    # Delete non-deterministic id; it doesn't seem to be needed.
    if "id" in dashboard:
        del dashboard["id"]
    # Populate the file tag so that the file name remains stable.
    if not get_file_tag(dashboard):
        dashboard["tags"] = dashboard.get("tags", [])
        dashboard["tags"].append("file:" + args.name)

    # Sometimes null values creep into the dashboard JSON and cause unnecessary
    # diffs. Strip these out since the resulting dashboard is equivalent.
    #
    # Related: https://github.com/grafana/grafana/issues/54126
    remove_dict_none_values(dashboard)

    # Set title suffix based on data source UID.
    suffix = " (VictoriaMetrics)"
    if args.data_source_uid == "vm":
        if not dashboard["title"].endswith(suffix):
            dashboard["title"] += suffix
    elif dashboard['title'].endswith(suffix):
        dashboard['title'] = dashboard['title'][:-len(suffix)]

    # Unless we are preserving a legacy dashboard UID, set the UID
    # deterministically based on file name + data source. This is done because
    # we need the UID to be different when generating the VictoriaMetrics
    # dashboard from the prom dashboard.
    if args.uid:
        dashboard["uid"] = args.uid
    else:
        dashboard["uid"] = sha256(repr([get_file_tag(dashboard), args.data_source_uid]))[:9]

    dashboard = with_data_source_uids(dashboard, args.data_source_uid)
    dashboard = with_ordered_dicts(dashboard)

    # Grafana updates the refresh interval in the JSON when changing it in the
    # UI. Hard-code it here to prevent accidental updates.
    dashboard["refresh"] = DASHBOARD_REFRESH_INTERVAL

    # Grafana updates "collapsed" state when expanding panels in the UI. Ensure
    # all panels are collapsed to prevent accidental updates.
    # (For the main buildbuddy dashboard only).
    if args.name == "buildbuddy.json":
        for panel in dashboard["panels"]:
            panel["collapsed"] = True

    # Note: ensure_ascii=False keeps strings like "µs" as-is.
    json.dump(dashboard, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")

def sha256(text):
    h = hashlib.sha256()
    h.update(text.encode('utf-8'))
    return h.hexdigest()


def with_data_source_uids(obj, uid):
    if isinstance(obj, list):
        return [with_data_source_uids(item, uid) for item in obj]
    elif isinstance(obj, dict):
        datasource = obj.get("datasource")
        if datasource and isinstance(datasource, dict) and datasource.get("type") == "prometheus":
            datasource["uid"] = uid
            obj = dict(obj)
            obj["datasource"] = datasource
        return {k: with_data_source_uids(v, uid) for (k, v) in obj.items()}
    else:
        return obj


def get_file_tag(dash):
    for tag in dash.get("tags", []):
        if tag.startswith("file:"):
            return tag[len("file:") :]
    return None


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
    main(ARGS.parse_args())
