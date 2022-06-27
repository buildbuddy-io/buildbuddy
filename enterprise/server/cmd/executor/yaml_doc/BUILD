load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_binary(
    name = "yaml_doc",
    embed = [":yaml_doc_lib"],
    visibility = ["//visibility:public"],
)

go_library(
    name = "yaml_doc_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/executor/yaml_doc",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/cmd/executor:executor_lib",
        "//server/util/flagutil/yaml",
    ],
)

genrule(
    name = "generate",
    outs = ["buildbuddy_executor_documented_defaults.yaml"],
    cmd = "./$(location :yaml_doc) -yaml_documented_defaults_out_file=\"$@\"",
    tools = [":yaml_doc"],
)
