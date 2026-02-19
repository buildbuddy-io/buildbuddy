load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "chunking",
    srcs = ["chunking.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/chunking",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "@com_github_buildbuddy_io_fastcdc2020//fastcdc",
        "@org_golang_google_grpc//status",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "chunking_test",
    srcs = ["chunking_test.go"],
    deps = [
        ":chunking",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
