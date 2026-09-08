load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_binary(
    name = "yaml_doc",
    embed = [":yaml_doc_lib"],
    # Only used to generate docs for //website; keep out of //... builds
    # (it is a full server/executor link, ~150MB per configuration).
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

go_library(
    name = "yaml_doc_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/cmd/buildbuddy/yaml_doc",
    visibility = ["//visibility:private"],
    deps = [
        "//server/cmd/buildbuddy:buildbuddy_lib",
        "//server/util/flagutil/yaml",
    ],
)

genrule(
    name = "generate_yaml",
    outs = ["buildbuddy_server_documented_defaults.yaml"],
    cmd = "./$(location :yaml_doc) -yaml_documented_defaults_out_file=\"$@\"",
    tags = ["manual"],
    tools = [":yaml_doc"],
    visibility = ["//visibility:public"],
)

genrule(
    name = "generate_mdx",
    srcs = [":generate_yaml"],
    outs = ["buildbuddy_server_documented_defaults.mdx"],
    cmd = """
        echo "\\`\\`\\`yaml" >> "$@" &&
        cat $(location :generate_yaml) >> "$@" &&
        echo "\\`\\`\\`" >> "$@"
    """,
    tags = ["manual"],
    visibility = ["//visibility:public"],
)
