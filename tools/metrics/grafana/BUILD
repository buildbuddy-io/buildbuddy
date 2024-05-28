load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["**/*"]))

go_library(
    name = "grafana_lib",
    srcs = ["grafana.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana",
    deps = ["@org_golang_x_sync//errgroup"],
)

go_binary(
    name = "grafana",
    embed = [":grafana_lib"],
)
