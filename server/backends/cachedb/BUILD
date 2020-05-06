load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["cachedb.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/cachedb",
    visibility = ["//visibility:public"],
    deps = [
        "//server/tables:go_default_library",
        "//server/util/db:go_default_library",
        "@com_github_jinzhu_gorm//:go_default_library",
    ],
)
