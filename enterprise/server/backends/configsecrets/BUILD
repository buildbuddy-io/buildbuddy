load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "configsecrets",
    srcs = ["configsecrets.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/util/flagutil/yaml",
        "//server/util/status",
        "@com_google_cloud_go_secretmanager//apiv1",
        "@com_google_cloud_go_secretmanager//apiv1/secretmanagerpb",
        "@org_golang_google_api//option",
        "@org_golang_x_oauth2//google",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
