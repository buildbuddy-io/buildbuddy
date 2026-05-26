load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "stripe",
    srcs = ["stripe.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/stripe",
    visibility = ["//enterprise:__subpackages__"],
    deps = [
        "//server/interfaces",
        "//server/real_environment",
        "//server/tables",
        "//server/util/flag",
        "//server/util/status",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
