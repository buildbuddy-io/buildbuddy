load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "sandbox",
    srcs = ["sandbox.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/sandbox",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/status",
        "@com_github_golang_protobuf//proto:go_default_library",
    ],
)

go_test(
    name = "sandbox_test",
    srcs = ["sandbox_test.go"],
    deps = [
        ":sandbox",
        "//enterprise/server/remote_execution/container",
        "//proto:remote_execution_go_proto",
        "//server/config",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//assert",
    ],
)
