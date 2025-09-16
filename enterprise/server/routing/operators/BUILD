load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "operators",
    srcs = ["operators.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/routing/operators",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/batch_operator",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/util/status",
        "//server/util/usageutil",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)
