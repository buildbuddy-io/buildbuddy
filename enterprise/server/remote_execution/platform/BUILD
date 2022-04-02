load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "platform",
    srcs = ["platform.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/executor/config",
        "//proto:remote_execution_go_proto",
        "//server/config",
        "//server/environment",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "platform_test",
    srcs = ["platform_test.go"],
    deps = [
        ":platform",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
