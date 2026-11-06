load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "preview_lib",
    srcs = ["preview.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/preview",
    deps = [
        "@io_k8s_api//batch/v1:batch",
        "@io_k8s_api//core/v1:core",
        "@io_k8s_apimachinery//pkg/apis/meta/v1:meta",
        "@io_k8s_apimachinery//pkg/types",
        "@io_k8s_apimachinery//pkg/util/intstr",
    ],
)

go_binary(
    name = "preview",
    embed = [":preview_lib"],
)
