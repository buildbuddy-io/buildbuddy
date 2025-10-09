load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "printlog",
    srcs = ["printlog.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/printlog",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/printlog/compact",
        "//proto:remote_execution_log_go_proto",
        "//server/util/proto",
        "@org_golang_google_protobuf//encoding/protodelim",
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)
