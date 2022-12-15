load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testolapdb",
    srcs = ["testolapdb.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testolapdb",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:stored_invocation_go_proto",
        "//server/tables",
        "@com_github_stretchr_testify//require",
        "@io_gorm_gorm//:gorm",
    ],
)
