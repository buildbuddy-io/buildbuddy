load("@io_bazel_rules_go//go:def.bzl", "go_library")

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

py_binary(
    name = "generate_docs",
    srcs = ["generate_docs.py"],
    main = "generate_docs.py",
)

genrule(
    name = "generate_mdx",
    srcs = [":metrics.go"],
    outs = ["docs.mdx"],
    cmd_bash = """
    BAZEL_BINDIR=$(BINDIR) $(location :generate_docs) \
        --prettier_path=$(location //tools/prettier) \
        --input_path=$(SRCS) \
        --output_path=$@
    """,
    tools = [
        ":generate_docs",
        "//tools/prettier",
    ],
)
