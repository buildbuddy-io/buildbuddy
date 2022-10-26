load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = [
    "//enterprise:__subpackages__",
    "//tools:__subpackages__",
])

go_library(
    name = "kms",
    srcs = ["kms.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/kms",
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
