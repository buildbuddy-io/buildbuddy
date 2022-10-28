load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_binary(
    name = "yaml_doc",
    embed = [":yaml_doc_lib"],
)

go_library(
    name = "yaml_doc_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/server/yaml_doc",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/cmd/server:server_lib",
        "//server/util/flagutil/yaml",
    ],
)

genrule(
    name = "generate_yaml",
    outs = ["buildbuddy_enterprise_server_documented_defaults.yaml"],
    cmd = "./$(location :yaml_doc) -yaml_documented_defaults_out_file=\"$@\"",
    tools = [":yaml_doc"],
)

genrule(
    name = "generate_mdx",
    srcs = [":generate_yaml"],
    outs = ["buildbuddy_enterprise_server_documented_defaults.mdx"],
    cmd = """
        echo "\\`\\`\\`yaml" >> "$@" &&
        cat $(location :generate_yaml) >> "$@" &&
        echo "\\`\\`\\`" >> "$@"
    """,
    visibility = [
        "//website:__pkg__",
    ],
)
