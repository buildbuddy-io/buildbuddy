load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "driver",
    srcs = ["driver.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/driver",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:raft_go_proto",
        "//server/util/log",
        "//server/util/status",
        "@com_github_golang_protobuf//proto:go_default_library",
    ],
)

go_test(
    name = "driver_test",
    srcs = ["driver_test.go"],
    deps = [
        ":driver",
        "//proto:raft_go_proto",
        "//server/util/log",
        "//server/util/status",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_stretchr_testify//require",
    ],
)
