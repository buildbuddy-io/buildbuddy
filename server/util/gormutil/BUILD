load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "gormutil",
    srcs = ["gormutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/gormutil",
    visibility = ["//visibility:public"],
    deps = ["@io_gorm_gorm//:gorm"],
)
