load("@rules_python//python:defs.bzl", "py_binary", "py_library")

exports_files(["process_dashboard.py"])

py_binary(
    name = "process_dashboard",
    srcs = ["process_dashboard.py"],
    visibility = ["//:__subpackages__"],
)

py_binary(
    name = "simulate_traffic",
    srcs = ["simulate_traffic.py"],
    visibility = ["//:__subpackages__"],
)

py_library(
    name = "metrics_py_library",
    srcs = [
        "process_dashboard.py",
        "simulate_traffic.py",
    ],
    visibility = ["//:__subpackages__"],
)
