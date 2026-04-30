load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "reflinker_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/reflinker",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/remote_execution/filecache",
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/digest",
        "//server/util/claims",
        "//server/util/log",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "reflinker",
    embed = [":reflink_lib"],
    pure = "on",
    static = "on",
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "reflink_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/reflink",
    deps = [
        "//enterprise/server/remote_execution/filecache",
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/digest",
        "//server/util/claims",
        "//server/util/flag",
        "//server/util/log",
        "@org_golang_x_sync//errgroup",
    ],
)
