load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "hostid",
    srcs = ["hostid.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/hostid",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/disk",
        "@com_github_google_uuid//:uuid",
    ],
)
