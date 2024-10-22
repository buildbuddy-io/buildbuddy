load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "unmarshal_lib",
    srcs = ["unmarshal.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/unmarshal",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/proto",
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)

go_binary(
    name = "unmarshal",
    embed = [":unmarshal_lib"],
    visibility = ["//visibility:public"],
)
