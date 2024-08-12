load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "exec_snapshot_lib",
    srcs = ["exec_snapshot.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/exec_snapshot",
    visibility = ["//visibility:private"],
    deps = [
        "//server/util/log",
        "//server/util/status",
        "@com_github_creack_pty//:pty",
        "@com_github_logrusorgru_aurora//:aurora",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "exec_snapshot",
    embed = [":exec_snapshot_lib"],
    visibility = ["//visibility:public"],
)
