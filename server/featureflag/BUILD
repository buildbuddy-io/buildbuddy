load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "featureflag",
    srcs = [
        "featureflag_server.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/featureflag",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:featureflag_go_proto",
        "//server/environment",
        "//server/featureflag/featureflag_cache",
        "//server/interfaces",
        "//server/tables",
        "//server/util/authutil",
        "//server/util/db",
        "//server/util/perms",
        "//server/util/random",
        "//server/util/status",
    ],
)
