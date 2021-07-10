#!/usr/bin/env python3

import argparse
import base64
import gzip
import json
import re
import os
import sys


def get_chunk_data(chunk_dir):
    chunks = []
    chunk_names = os.listdir(chunk_dir)
    for chunk_name in chunk_names:
        chunk_path = os.path.join(chunk_dir, chunk_name)
        stat = os.stat(chunk_path)
        modify_time = stat.st_mtime
        with gzip.open(chunk_path, "rb") as chunk_file:
            content = chunk_file.read()
        chunks.append(
            {
                "content": base64.b64encode(content).decode("utf-8"),
                "modify_time": modify_time,
            }
        )
    min_modify_time = min([chunk["modify_time"] for chunk in chunks])
    for chunk in chunks:
        chunk["modify_time"] = chunk["modify_time"] - min_modify_time
    chunks = sorted(chunks, key=lambda chunk: chunk["modify_time"])
    return chunks


def main(invocations_dir, invocation_id, out_file):
    invocation_id = args.invocation_id
    if not invocation_id:
        invocation_id = latest_invocation_id(invocations_dir)
        print(f"Invocation ID: {invocation_id}")
    chunk_dir = f"{invocations_dir}/{invocation_id}/chunks/log"
    chunks = get_chunk_data(chunk_dir)
    with open(out_file, "w") as data_file:
        data_file.write("export default ")
        data_file.write(json.dumps(chunks, indent=2))
        data_file.write(";")
    print(f"Wrote {out_file}")


UUIDV4_PATTERN = r"[\w]{8}-[\w]{4}-[\w]{4}-[\w]{4}-[\w]{12}"


def latest_invocation_id(invocations_dir):
    invocation_ids = [
        id for id in os.listdir(invocations_dir) if re.match(UUIDV4_PATTERN, id)
    ]
    if not invocation_ids:
        raise ValueError(f"No invocations yet in {invocations_dir}")
    entries = [
        {"id": id, "mtime": os.stat(f"{invocations_dir}/{id}").st_mtime}
        for id in invocation_ids
    ]
    return max(entries, key=lambda entry: entry["mtime"])["id"]


if __name__ == "__main__":
    os.chdir(os.path.dirname(sys.path[0]))

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--invocation_id",
        help="Invocation ID to export. Defaults to the last run invocation.",
    )
    parser.add_argument(
        "-d",
        "--invocations_dir",
        default="/tmp/buildbuddy_enterprise",
        help="Invocations directory.",
    )
    parser.add_argument(
        "-o",
        "--out_file",
        default="app/log/fake_fetcher_data.tsx",
        help="Output file path.",
    )
    args = parser.parse_args()

    main(**vars(args))
