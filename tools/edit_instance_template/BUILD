load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "edit_instance_template_lib",
    srcs = ["edit_instance_template.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/edit_instance_template",
    visibility = ["//visibility:private"],
    deps = [
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "@com_google_cloud_go_compute//apiv1",
        "@com_google_cloud_go_compute//apiv1/computepb",
        "@org_golang_google_api//iterator",
    ],
)

go_binary(
    name = "edit_instance_template",
    embed = [":edit_instance_template_lib"],
    visibility = ["//visibility:public"],
)
