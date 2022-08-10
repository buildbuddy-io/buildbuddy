load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "flagz",
    srcs = ["flagz.go"],
    data = [
        "//enterprise/server/flagz/web:app_bundle",
        "//enterprise/server/flagz/web:index.html",
        "//enterprise/server/flagz/web:style",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/flagz",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:flagz_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/flagutil/yaml",
        "//server/util/status",
        "@in_gopkg_yaml_v3//:yaml_v3",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)
