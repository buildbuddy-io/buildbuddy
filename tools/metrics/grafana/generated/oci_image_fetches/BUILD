load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "oci_image_fetches_lib",
    srcs = ["oci_image_fetches.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/oci_image_fetches",
    deps = [
        "//tools/metrics/grafana/generated/dash",
        "@com_github_grafana_grafana_foundation_sdk_go//common",
        "@com_github_grafana_grafana_foundation_sdk_go//dashboard",
        "@com_github_grafana_grafana_foundation_sdk_go//timeseries",
    ],
)

go_binary(
    name = "oci_image_fetches",
    embed = [":oci_image_fetches_lib"],
)

genrule(
    name = "oci_image_fetches_json",
    outs = ["oci-image-fetches.json"],
    cmd = "$(location :oci_image_fetches) > $@",
    tools = [":oci_image_fetches"],
)
