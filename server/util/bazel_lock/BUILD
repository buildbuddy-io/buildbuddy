load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "bazel_lock",
    srcs = ["bazel_lock.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/bazel_lock",
    deps = ["@org_golang_x_sys//unix"],
)
