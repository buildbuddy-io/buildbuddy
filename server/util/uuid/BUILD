load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "uuid",
    srcs = ["uuid.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/uuid",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "//server/util/status",
        "@com_github_google_uuid//:uuid",
    ],
)
