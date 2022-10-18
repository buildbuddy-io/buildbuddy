load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "kms",
    srcs = ["kms.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/kms",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/status",
        "@com_github_google_tink_go//core/registry",
        "@com_github_google_tink_go//integration/gcpkms",
        "@org_golang_google_api//option",
    ],
)
