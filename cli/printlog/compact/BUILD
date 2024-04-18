load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "compact",
    srcs = ["compact.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/printlog/compact",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/log",
        "//proto:spawn_go_proto",
        "@com_github_klauspost_compress//zstd",
        "@org_golang_google_protobuf//encoding/protodelim",
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)
