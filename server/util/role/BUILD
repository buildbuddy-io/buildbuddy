load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "role",
    srcs = ["role.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/role",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:api_key_go_proto",
        "//proto:group_go_proto",
        "//server/util/status",
    ],
)
