load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testolapdb",
    srcs = ["testolapdb.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testolapdb",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:internal_execution_go_proto",
        "//server/interfaces",
        "//server/tables",
        "@io_gorm_gorm//:gorm",
    ],
)
