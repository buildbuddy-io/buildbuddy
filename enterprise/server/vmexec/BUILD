load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "vmexec",
    srcs = ["vmexec.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/vmexec",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//proto:vmexec_go_proto",
        "//server/util/log",
        "//server/util/status",
        "@com_github_vishvananda_netlink//:netlink",
        "@org_golang_google_grpc//status",
        "@org_golang_x_sys//unix",
    ],
)
