#!/usr/bin/env python3
import os
import re
import subprocess
import sys
from typing import List, Tuple

# Resolutions by proto path prefix. First matching prefix wins.
PREFIX_RESOLUTIONS = [
    (r"proto", "//proto"),
    (r"google/protobuf", "@com_google_protobuf//"),
    (r"google", "@go_googleapis//google"),
]

# Resolutions by proto import path (to work around naming quirks).
FILE_RESOLUTIONS = {
    "google/longrunning/operations.proto": "@go_googleapis//google/longrunning:longrunning_proto",
}

# These proto library imports do not require explicit Go deps.
PROTO_LIBRARY_IMPORTS = set(
    [
        "google/protobuf/any.proto",
        "google/protobuf/duration.proto",
        "google/protobuf/empty.proto",
        "google/protobuf/timestamp.proto",
    ]
)


def fatal(msg):
    sys.stderr.write(msg + "\n")
    sys.exit(1)


def sh(cmd: str) -> List[str]:
    """Runs a shell command and returns a list of non-blank output lines."""
    proc = subprocess.run(cmd, capture_output=True, encoding="utf-8", shell=True)
    return [
        line for line in (line.strip() for line in proc.stdout.splitlines()) if line
    ]


def get_label(suffix: str, package: str, path: str) -> str:
    """
    >>> get_label('proto', '//proto', 'local.proto')
    ':local_proto'
    >>> get_label('ts_proto', '@foo//', 'bar/baz.proto')
    '@foo//bar:baz_ts_proto'
    >>> get_label('go_proto', '@foo//bar', 'baz/qux.proto')
    '@foo//bar/baz:qux_go_proto'
    >>> get_label('proto', '@foo', 'baz.proto')
    '@foo//:baz_proto'
    """
    label = package
    if "//" not in label:
        label += "//"
    elif not label.endswith("/"):
        label = label + "/"
    segments = path.split("/")
    label += "/".join(segments[:-1])
    base = segments[-1]
    label += ":" + base.replace(".proto", f"_{suffix}")
    if "//:" not in label:
        label = label.replace("/:", ":")
    if label.startswith("//proto:"):
        label = label.replace("//proto:", ":")
    return label


if __name__ == "__main__":
    os.chdir(sys.path[0])

    if sys.argv[1:] == ["test"]:
        import doctest

        doctest.testmod()
        exit()

    import_paths_by_proto: List[Tuple[str, str]] = []
    for proto in sh("ls *.proto"):
        imported_paths = []
        for imported_path in sh(f'cat {proto} | grep -P "^import ".*.proto""'):
            match = re.search(r'"(.*?\.proto)"', imported_path)
            if not match:
                fatal(f"could not process import: {imported_path}")
            imported_paths.append(match.group(1))
        import_paths_by_proto.append((proto, imported_paths))

    proto_rules = []
    go_rules = []
    ts_rules = []

    for (proto_filename, imported_paths) in import_paths_by_proto:
        proto_name = os.path.basename(proto_filename).replace(".proto", "")
        resolved_proto_deps = []
        resolved_go_deps = []
        for imported_path in imported_paths:
            if imported_path in FILE_RESOLUTIONS:
                label = FILE_RESOLUTIONS[imported_path]
                resolved_proto_deps.append(label)
                resolved_go_deps.append(label.replace("_proto", "_go_proto"))
                continue

            resolved_package = None
            match = None
            for (prefix, package) in PREFIX_RESOLUTIONS:
                if match := re.match(prefix + r"/(.*)", imported_path):
                    resolved_package = package
                    break
            if resolved_package is None:
                fatal(f'could not resolve build dep for "{imported_path}"')
            subpath = match.group(1)

            resolved_proto_deps.append(get_label("proto", resolved_package, subpath))
            if imported_path not in PROTO_LIBRARY_IMPORTS:
                resolved_go_deps.append(
                    get_label("go_proto", resolved_package, subpath)
                )

        proto_rules.append(
            f"""
proto_library(
    name = "{proto_name}_proto",
    srcs = ["{proto_name}.proto"],
    deps = {repr(resolved_proto_deps)},
)
"""
        )

        go_rules.append(
            f"""
go_proto_library(
    name = "{proto_name}_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/{proto_name}",
    proto = "{proto_name}_proto",
    {'compilers = ["@io_bazel_rules_go//proto:go_grpc"],' if 'service' in proto_name else ''}
    deps = {repr(resolved_go_deps)},
)
"""
        )

        ts_rules.append(
            f"""
ts_proto_library(
    name = "{proto_name}_ts_proto",
    deps = [":{proto_name}_proto"],
)
"""
        )

    header = """
# DO NOT EDIT BY HAND -- generate automatically with `python3 proto/buildgen.py`.
# To add new external deps, edit RESOLUTIONS at the top of buildgen.py.

load("@rules_proto//proto:defs.bzl", "proto_library")
load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")
load("@npm//@bazel/labs:index.bzl", ts_proto_library = "protobufjs_ts_library")

package(default_visibility = ["//visibility:public"])

"""

    with open("BUILD", "w") as f:
        f.write(header)
        f.write("".join(proto_rules))
        f.write("".join(go_rules))
        f.write("".join(ts_rules))

    sh("buildifier BUILD")

    print(f"wrote {os.getcwd()}/BUILD")
