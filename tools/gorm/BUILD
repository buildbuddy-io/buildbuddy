load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

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

go_binary(
    name = "print_auto_migration",
    embed = [":gorm_lib"],
    visibility = ["//visibility:public"],
)

go_library(
    name = "gorm_lib",
    srcs = ["print_auto_migration.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/gorm",
    visibility = ["//visibility:private"],
    deps = [
        "//:bundle",
        "//server/util/db",
        "//server/util/fileresolver",
        "//server/util/log",
        "@io_gorm_gorm//:gorm",
    ],
)
