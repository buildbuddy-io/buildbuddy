load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "edit_instance_template_lib",
    srcs = ["edit_instance_template.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/edit_instance_template",
    visibility = ["//visibility:private"],
    deps = [
        "@com_google_cloud_go_compute//apiv1",
        "@org_golang_google_api//iterator",
        "@org_golang_google_genproto//googleapis/cloud/compute/v1:compute",
        "@org_golang_google_protobuf//proto",
    ],
)

go_binary(
    name = "edit_instance_template",
    embed = [":edit_instance_template_lib"],
    visibility = ["//visibility:public"],
)
