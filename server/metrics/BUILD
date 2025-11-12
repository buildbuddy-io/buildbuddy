load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("@rules_python//python:defs.bzl", "py_binary", "py_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "metrics",
    srcs = ["metrics.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/metrics",
    deps = [
        "@com_github_prometheus_client_golang//prometheus",
        "@com_github_prometheus_client_golang//prometheus/promauto",
    ],
)

genrule(
    name = "generate_mdx",
    srcs = [":metrics.go"],
    outs = ["docs.mdx"],
    cmd_bash = """
    BAZEL_BINDIR=$(BINDIR) python3 $(location :generate_docs.py) \
        --prettier_path=$(location //tools/prettier) \
        --input_path=$(SRCS) \
        --output_path=$@
    """,
    tools = [
        ":generate_docs.py",
        "//tools/prettier",
    ],
)

py_binary(
    name = "generate_docs",
    srcs = ["generate_docs.py"],
    visibility = ["//:__subpackages__"],
)

py_library(
    name = "metrics_py_library",
    srcs = ["generate_docs.py"],
    visibility = ["//:__subpackages__"],
)
