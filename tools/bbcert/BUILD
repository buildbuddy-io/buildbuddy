load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "bbcert_lib",
    srcs = ["bbcert.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbcert",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:certgenerator_go_proto",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/log",
    ],
)

go_binary(
    name = "bbcert",
    embed = [":bbcert_lib"],
    visibility = ["//visibility:public"],
)
