load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

# gazelle:default_visibility //enterprise:__subpackages__,@buildbuddy_internal//:__subpackages__
package(default_visibility = [
    "//enterprise:__subpackages__",
    "@buildbuddy_internal//:__subpackages__",
])

go_binary(
    name = "yaml_doc",
    embed = [":yaml_doc_lib"],
    # Only used to generate docs for //website; keep out of //... builds
    # (it is a full server/executor link, ~150MB per configuration).
    tags = ["manual"],
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
    tags = ["manual"],
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
    tags = ["manual"],
    visibility = [
        "//website:__pkg__",
    ],
)
