load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["tables.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/tables",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:invocation_go_proto",
        "//proto:user_go_proto",
        "//server/util/random:go_default_library",
        "@com_github_jinzhu_gorm//:go_default_library",
    ],
)
