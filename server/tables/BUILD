load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["tables.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/tables",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:group_go_proto",
        "//proto:user_id_go_proto",
        "//server/util/random:go_default_library",
        "@io_gorm_gorm//:go_default_library",
    ],
)
