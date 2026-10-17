load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "stripe",
    srcs = ["stripe.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/stripe",
    visibility = ["//enterprise:__subpackages__"],
    deps = [
        "//enterprise/server/billing/metronome",
        "//server/http/httpclient",
        "//server/util/flag",
        "//server/util/status",
    ],
)
