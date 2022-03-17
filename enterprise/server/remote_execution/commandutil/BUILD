load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "commandutil",
    srcs = ["commandutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/util/status",
    ],
)

go_test(
    name = "commandutil_test",
    srcs = ["commandutil_test.go"],
    deps = [
        ":commandutil",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/testutil/testfs",
        "//server/util/status",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
