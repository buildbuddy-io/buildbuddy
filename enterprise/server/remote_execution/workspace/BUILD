load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "workspace",
    srcs = ["workspace.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/workspace",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/dirtools",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/disk",
        "//server/util/log",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_google_uuid//:uuid",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "workspace_test",
    srcs = ["workspace_test.go"],
    deps = [
        ":workspace",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
