load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "vmexec",
    srcs = ["vmexec.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/vmexec",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:vmexec_go_proto",
        "//server/util/status",
    ],
)
