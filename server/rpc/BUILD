load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "filters",
    srcs = ["filters.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/rpc/filters",
    deps = [
        "//server:environment",
        "@org_golang_google_grpc//:go_default_library",
    ],
)
