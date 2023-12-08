load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "generate_docs_lib",
    srcs = ["generate_docs.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/prom/generate_docs",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/backends/prom",
        "@org_golang_x_text//cases",
        "@org_golang_x_text//language",
    ],
)

go_binary(
    name = "generate_docs",
    embed = [":generate_docs_lib"],
    visibility = ["//visibility:public"],
)

genrule(
    name = "generate_mdx",
    outs = ["docs.mdx"],
    cmd_bash = """
    ./$(location :generate_docs) -output_path="$@"
    """,
    tools = [
        ":generate_docs",
    ],
    visibility = [
        "//website:__pkg__",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
