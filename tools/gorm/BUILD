load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "print_auto_migration_lib",
    srcs = ["print_auto_migration.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/print_auto_migration",
    visibility = ["//visibility:private"],
    deps = [
        "//:bundle",
        "//server/util/db",
        "//server/util/fileresolver",
        "//server/util/log",
        "@io_gorm_gorm//:gorm",
    ],
)
