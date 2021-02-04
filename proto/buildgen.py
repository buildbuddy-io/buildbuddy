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
    "proto/grp.proto": "//proto:group_proto",
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


def resolve_full_label(suffix: str, path: str) -> str:
    if path in FILE_RESOLUTIONS:
        return FILE_RESOLUTIONS[path].replace("_proto", f"_{suffix}")

    resolved_package = None
    match = None
    for (prefix, package) in PREFIX_RESOLUTIONS:
        if match := re.match(prefix + r"/(.*)", path):
            resolved_package = package
            break
    if resolved_package is None:
        fatal(f'could not resolve build dep for "{path}"')

    path = match.group(1)
    label = resolved_package
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


def simplify_label(label: str) -> str:
    if label.startswith("//proto:"):
        return label[len("//proto") :]
    return label


def resolve(suffix: str, path: str) -> str:
    """
    >>> resolve('proto', 'proto/local.proto')
    ':local_proto'
    >>> resolve('proto', 'proto/grp.proto')
    ':group_proto'
    >>> resolve('ts_proto', 'google/rpc/foo.proto')
    '@go_googleapis//google/rpc:foo_ts_proto'
    >>> resolve('go_proto', 'google/protobuf/any.proto')
    """
    if suffix == "go_proto" and path in PROTO_LIBRARY_IMPORTS:
        return None
    label = resolve_full_label(suffix, path)
    label = simplify_label(label)
    return label


def has_service(file_name: str) -> bool:
    with open(file_name, "r") as f:
        for line in f:
            if line.startswith("service "):
                return True
    return False


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
            resolved_proto_deps.append(resolve("proto", imported_path))
            resolved_go_deps.append(resolve("go_proto", imported_path))

        proto_path = "proto/" + proto_filename

        proto_rules.append(
            f"""
proto_library(
    name = "{resolve("proto", proto_path)[1:]}",
    srcs = ["{proto_name}.proto"],
    deps = {repr(resolved_proto_deps)},
)
"""
        )

        url_suffix = resolve("proto", proto_path)[1:].replace("_proto", "")

        go_rules.append(
            f"""
go_proto_library(
    name = "{resolve("go_proto", proto_path)[1:]}",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/{url_suffix}",
    proto = "{resolve("proto", proto_path)}",
    {'compilers = ["@io_bazel_rules_go//proto:go_grpc"],' if has_service(proto_filename) else ''}
    deps = {repr([dep for dep in resolved_go_deps if dep is not None])},
)
"""
        )

        ts_rules.append(
            f"""
ts_proto_library(
    name = "{resolve("ts_proto", proto_path)[1:]}",
    deps = ["{resolve('proto', proto_path)}"],
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
