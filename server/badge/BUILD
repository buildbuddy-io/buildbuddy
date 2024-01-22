load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "badge",
    srcs = ["badge.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/badge",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:badge_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/util/db",
        "//server/util/status",
        "@io_gorm_gorm//clause",
    ],
)
